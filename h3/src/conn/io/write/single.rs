use dope_quic::stream::release;
use o3::{
    depot::queue::{group, group::vacant},
    mem,
};

use crate::{
    conn,
    conn::{
        io::write::{Queues, Stage, budget, plan},
        output,
    },
    stream,
};

pub(in crate::conn) struct Single<'a, 'd, T> {
    slot: vacant::Entry<'a, plan::Record<'d>, 2>,
    change: budget::Change<'a>,
    target: stream::Target,
    entry: T,
}

impl<'a, 'd, T: Stage<'d>> Single<'a, 'd, T> {
    pub(in crate::conn) fn new<S, B>(
        queues: &'a mut Queues<'d, S, B>,
        target: stream::Target,
        entry: T,
    ) -> Result<Self, mem::Rejected<[T; 1], conn::Error>> {
        let class = entry.class();
        let lane = class.lane();
        let owner = target.stream_id().get();
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
        let zero = output::WriteCharge::default();
        let (regular, critical) = match class {
            budget::WriteClass::Regular => (entry.charge(), zero),
            budget::WriteClass::Critical => (zero, entry.charge()),
        };
        let change = match queues.write_budget.try_reserve(regular, critical) {
            Ok(change) => change,
            Err(error) => return Err(mem::Rejected::new([entry], error)),
        };
        let slot =
            match group::Batch::new(&mut queues.writes).and_then(|slots| slots.bind(lane, owner)) {
                Ok(slot) => slot,
                Err(error) => {
                    return Err(mem::Rejected::new([entry], conn::Error::Egress(error)));
                }
            };
        Ok(Self {
            slot,
            change,
            target,
            entry,
        })
    }

    pub(in crate::conn) fn commit<M: release::Mode>(self) -> M::Projection {
        self.change.commit();
        let write = self.entry.into_write(self.target);
        let retained = M::project(|| write.retained());
        self.slot.insert(write);
        retained
    }
}
