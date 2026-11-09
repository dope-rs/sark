use dope_quic::stream::release;
use o3::{
    depot::queue::{group, group::vacant},
    mem,
};

use crate::{
    conn,
    conn::{
        io::write::{Entry, Queues, Stage as _, budget, plan},
        output,
    },
    stream,
};

pub(in crate::conn) struct Permit<'a, 'd, const N: usize> {
    slots: group::Batch<'a, plan::Record<'d>, 2, N>,
    change: budget::Change<'a>,
    entries: [(stream::Target, Entry<'d>); N],
}

impl<'a, 'd, const N: usize> Permit<'a, 'd, N> {
    pub(in crate::conn) fn new<S, B>(
        queues: &'a mut Queues<'d, S, B>,
        entries: [(stream::Target, Entry<'d>); N],
    ) -> Result<Self, mem::Rejected<[(stream::Target, Entry<'d>); N], conn::Error>> {
        let lane = budget::WriteClass::Regular.lane();
        let queued = queues.writes.lane_len(lane);
        let added = entries
            .iter()
            .filter(|(_, entry)| entry.class() == budget::WriteClass::Regular)
            .count();
        if queued
            .checked_add(added)
            .is_none_or(|count| count > queues.regular_write_limit)
        {
            return Err(mem::Rejected::new(
                entries,
                conn::Error::QueueRegular {
                    queued,
                    additional: added,
                    limit: queues.regular_write_limit,
                },
            ));
        }
        let mut regular = output::WriteCharge::default();
        let mut critical = output::WriteCharge::default();
        for (_, entry) in &entries {
            let class = entry.class();
            let charge = match class {
                budget::WriteClass::Regular => &mut regular,
                budget::WriteClass::Critical => &mut critical,
            };
            *charge = match class.try_add(*charge, entry.charge()) {
                Ok(next) => next,
                Err(error) => return Err(mem::Rejected::new(entries, error)),
            };
        }
        let change = match queues.write_budget.try_reserve(regular, critical) {
            Ok(change) => change,
            Err(error) => return Err(mem::Rejected::new(entries, error)),
        };
        let slots = match group::Batch::new(&mut queues.writes) {
            Ok(slots) => slots,
            Err(error) => {
                return Err(mem::Rejected::new(entries, conn::Error::Egress(error)));
            }
        };
        Ok(Self {
            slots,
            change,
            entries,
        })
    }

    pub(in crate::conn) fn commit<M: release::Mode, C>(
        self,
        commit: C,
        ready: impl FnOnce(C),
    ) -> Result<
        M::Projection,
        (
            mem::Rejected<[(stream::Target, Entry<'d>); N], conn::Error>,
            C,
        ),
    > {
        let items = self.entries.map(|(target, entry)| {
            vacant::Item::new(
                entry.class().lane(),
                target.stream_id().get(),
                (target, entry),
            )
        });
        let mut retained = M::Projection::default();
        self.slots
            .publish(
                items,
                (commit, self.change),
                |(commit, change)| {
                    ready(commit);
                    change.commit();
                },
                |(target, entry)| {
                    let class = entry.class();
                    let write = entry.into_write(target);
                    if class == budget::WriteClass::Regular {
                        // The admitted aggregate bounds all retained components.
                        retained = M::merge(retained, M::project(|| write.retained()));
                    }
                    write
                },
            )
            .map_err(|(error, ((commit, _), items))| {
                (
                    mem::Rejected::new(
                        items.map(|item| item.into_parts().2),
                        conn::Error::Egress(error),
                    ),
                    commit,
                )
            })?;
        Ok(retained)
    }
}
