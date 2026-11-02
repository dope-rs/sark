use o3::depot::queue::group::{self, vacant};

use crate::conn::{
    self,
    io::write::{Stage, budget, plan},
    output,
};

/// The unchanged entries and exact budget owner admitted by one aggregate check.
pub(in crate::conn::io::write) struct Batch<'a, T, const N: usize> {
    change: budget::Change<'a>,
    entries: [T; N],
}

impl<'a, T> Batch<'a, T, 1> {
    pub(in crate::conn::io::write) fn commit_one<'d>(
        self,
        slot: vacant::Entry<'a, plan::Record<'d>, 2>,
    ) where
        T: Stage<'d>,
    {
        let [entry] = self.entries;
        self.change.commit();
        slot.insert(entry.into_write());
    }
}

impl<'a, T, const N: usize> Batch<'a, T, N> {
    pub(in crate::conn::io::write) fn new<'d>(
        budget: &'a mut budget::WriteBudget,
        entries: [T; N],
    ) -> Result<Self, (conn::Error, [T; N])>
    where
        T: Stage<'d>,
    {
        let mut regular = output::WriteCharge::default();
        let mut critical = output::WriteCharge::default();
        for entry in &entries {
            let class = entry.class();
            let charge = match class {
                budget::WriteClass::Regular => &mut regular,
                budget::WriteClass::Critical => &mut critical,
            };
            *charge = match charge.try_add(entry.charge()) {
                Ok(next) => next,
                Err(error) => {
                    let error = match (class, error) {
                        (budget::WriteClass::Regular, output::Overflow::Wire { left, right }) => {
                            conn::Error::BatchRegularWire { left, right }
                        }
                        (
                            budget::WriteClass::Regular,
                            output::Overflow::Resident { left, right },
                        ) => conn::Error::BatchRegularResident { left, right },
                        (budget::WriteClass::Critical, output::Overflow::Wire { left, right }) => {
                            conn::Error::BatchCriticalWire { left, right }
                        }
                        (
                            budget::WriteClass::Critical,
                            output::Overflow::Resident { left, right },
                        ) => conn::Error::BatchCriticalResident { left, right },
                    };
                    return Err((error, entries));
                }
            };
        }
        let change = match budget.try_reserve(regular, critical) {
            Ok(change) => change,
            Err(error) => return Err((error, entries)),
        };
        Ok(Self { change, entries })
    }

    pub(in crate::conn::io::write) fn into_entries(self) -> [T; N] {
        self.entries
    }

    pub(in crate::conn::io::write) fn commit<'d, C>(
        self,
        slots: group::Batch<'a, plan::Record<'d>, 2, N>,
        commit: C,
        ready: impl FnOnce(C),
    ) -> Result<(), (conn::Error, (C, [T; N]))>
    where
        T: Stage<'d>,
    {
        let items = self.entries.map(|entry| {
            vacant::Item::new(
                entry.class().lane(),
                entry.target().stream_id().get(),
                entry,
            )
        });
        slots
            .publish(
                items,
                (commit, self.change),
                |(commit, change)| {
                    ready(commit);
                    change.commit();
                },
                T::into_write,
            )
            .map_err(|(error, ((commit, _), items))| {
                (
                    conn::Error::Egress(error),
                    (commit, items.map(|item| item.into_parts().2)),
                )
            })
    }
}
