use o3::{depot::queue::group, mem};

use crate::{
    conn,
    conn::io::write::{Queues, Stage, budget, plan},
};

pub(in crate::conn) struct Permit<'a, 'd, T, const N: usize>
where
    T: Stage<'d>,
{
    slots: group::Batch<'a, plan::Record<'d>, 2, N>,
    budget: budget::Batch<'a, T, N>,
}

impl<'a, 'd, T: Stage<'d>, const N: usize> Permit<'a, 'd, T, N> {
    pub(in crate::conn) fn new<S, B>(
        queues: &'a mut Queues<'d, S, B>,
        entries: [T; N],
    ) -> Result<Self, mem::Rejected<[T; N], conn::Error>> {
        let lane = budget::WriteClass::Regular.lane();
        let queued = queues.writes.lane_len(lane);
        let added = entries
            .iter()
            .filter(|entry| entry.class() == budget::WriteClass::Regular)
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
        let budget = match budget::Batch::new(&mut queues.write_budget, entries) {
            Ok(budget) => budget,
            Err((error, entries)) => return Err(mem::Rejected::new(entries, error)),
        };
        let slots = match group::Batch::new(&mut queues.writes) {
            Ok(slots) => slots,
            Err(error) => {
                return Err(mem::Rejected::new(
                    budget.into_entries(),
                    conn::Error::Egress(error),
                ));
            }
        };
        Ok(Self { slots, budget })
    }

    pub(in crate::conn) fn commit<C>(
        self,
        commit: C,
        ready: impl FnOnce(C),
    ) -> Result<(), (mem::Rejected<[T; N], conn::Error>, C)> {
        self.budget
            .commit(self.slots, commit, ready)
            .map_err(|(error, (commit, entries))| (mem::Rejected::new(entries, error), commit))
    }
}
