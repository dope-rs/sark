use o3::{
    depot::queue::{group, group::vacant},
    mem,
};

use crate::{
    conn,
    conn::io::write::{Queues, Stage, budget, plan},
};

pub(in crate::conn) struct Single<'a, 'd, T> {
    slot: vacant::Entry<'a, plan::Record<'d>, 2>,
    budget: budget::Batch<'a, T, 1>,
}

impl<'a, 'd, T: Stage<'d>> Single<'a, 'd, T> {
    pub(in crate::conn) fn new<S, B>(
        queues: &'a mut Queues<'d, S, B>,
        entry: T,
    ) -> Result<Self, mem::Rejected<[T; 1], conn::Error>> {
        let class = entry.class();
        let lane = class.lane();
        let owner = entry.target().stream_id().get();
        if class == budget::WriteClass::Regular {
            let queued = queues.writes.lane_len(lane);
            if queued >= queues.regular_write_limit {
                return Err(mem::Rejected::new(
                    [entry],
                    conn::Error::QueueRegular {
                        queued,
                        additional: 1,
                        limit: queues.regular_write_limit,
                    },
                ));
            }
        }
        let budget = match budget::Batch::new(&mut queues.write_budget, [entry]) {
            Ok(budget) => budget,
            Err((error, entries)) => return Err(mem::Rejected::new(entries, error)),
        };
        let slot =
            match group::Batch::new(&mut queues.writes).and_then(|slots| slots.bind(lane, owner)) {
                Ok(slot) => slot,
                Err(error) => {
                    return Err(mem::Rejected::new(
                        budget.into_entries(),
                        conn::Error::Egress(error),
                    ));
                }
            };
        Ok(Self { slot, budget })
    }

    pub(in crate::conn) fn commit(self) {
        self.budget.commit_one(self.slot);
    }
}
