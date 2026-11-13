use std::{marker, ops};

use dope_quic::{stream::send, wire::stream::Id};
use o3::{
    buffer::{hold, resident},
    depot::queue::{group, ring},
    mem,
};
use sark_core::manifold::stream::Boundary;
use sark_http::field::metadata::section::rules;

use crate::{
    conn,
    conn::{event, io::ingress::wait, output},
    qpack::decoder::state::State,
    stream,
};

pub(crate) mod budget;
mod credit;
mod entry;
pub(in crate::conn) mod feedback;
mod permit;
pub(in crate::conn) mod plan;
mod single;

pub(in crate::conn) use credit::Credit;
pub(in crate::conn) use entry::{Entry, Stage};
pub(in crate::conn) use permit::Permit;
pub(in crate::conn) use single::Single;

type Events<'d, S, B> = ring::Fifo<event::Event<'d, S, B, event::Token>>;

impl<'d> Stage<'d> for output::WritePrefix {
    fn class(&self) -> budget::WriteClass {
        budget::WriteClass::Critical
    }

    fn charge(&self) -> output::WriteCharge {
        self.charge()
    }

    fn into_write(self, target: stream::Target) -> plan::Record<'d> {
        plan::Record::single(target, self, Boundary::More)
    }
}

impl<'d> Stage<'d> for ring::Front<'_, ring::Fifo<(output::WritePrefix, u64)>> {
    fn class(&self) -> budget::WriteClass {
        budget::WriteClass::Critical
    }

    fn charge(&self) -> output::WriteCharge {
        self.get().0.charge()
    }

    fn into_write(self, target: stream::Target) -> plan::Record<'d> {
        plan::Record::single(target, self.remove().0, Boundary::More)
    }
}

impl<'d, P: rules::Policy, A: rules::Push> conn::Connection<'d, P, A> {
    pub(crate) fn publish_encoder(&mut self) -> Result<(), conn::Error> {
        let Some(front) = self
            .qpack
            .pending_encoder
            .as_mut()
            .and_then(ring::Fifo::front_entry)
        else {
            return Ok(());
        };
        let target = self
            .session
            .qpack_encoder
            .ok_or(conn::Error::EncoderMissing)?;
        Single::new(&mut self.queues, target, front)
            .map_err(mem::Rejected::discard)?
            .commit::<hold::evidence::Off>();
        Ok(())
    }
}

pub(crate) struct Queues<'d, S, B> {
    pub(in crate::conn) waiters: wait::Table,
    pub(in crate::conn) receive_credits: ring::Fifo<(Credit, usize)>,
    pub(in crate::conn) events: Events<'d, S, B>,
    // Exact queued cancellation count; token-based discard preserves these events.
    pub(in crate::conn) cancellations: usize,
    pub(in crate::conn) writes: group::Ledger<plan::Record<'d>, 2>,
    pub(in crate::conn) regular_write_limit: usize,
    pub(in crate::conn) write_budget: budget::WriteBudget,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Gate {
    Open,
    Held,
}

#[must_use = "a taken write must be committed or restored"]
pub(crate) struct TakenWrite<'q, 'd> {
    write: plan::Record<'d>,
    source: Source<'q, 'd>,
    class: budget::WriteClass,
    budget: &'q mut budget::WriteBudget,
}

impl<'q, 'd> TakenWrite<'q, 'd> {
    pub(crate) fn with<R>(
        self,
        run: impl for<'id> FnOnce(Flight<'id, 'd>, Prepared<'id, 'q, 'd>) -> R,
    ) -> R {
        let Self {
            write,
            source,
            class,
            budget,
        } = self;
        let charge = write.charge();
        run(
            Flight::new(write),
            Prepared {
                source,
                budget,
                class,
                charge,
                brand: marker::PhantomData,
            },
        )
    }
}

enum Source<'q, 'd> {
    Queued {
        vacancy: group::Vacancy<'q, plan::Record<'d>, 2>,
    },
    DecoderFeedback {
        wire: &'q mut State,
    },
}

impl<'q, 'd> Source<'q, 'd> {
    fn restore(self, write: plan::Record<'d>) {
        match self {
            Self::Queued { vacancy, .. } => {
                vacancy.restore(write);
            }
            Self::DecoderFeedback { .. } => {}
        }
    }
}

#[must_use = "a prepared write must be committed or restored"]
pub(crate) struct Prepared<'id, 'q, 'd> {
    source: Source<'q, 'd>,
    budget: &'q mut budget::WriteBudget,
    class: budget::WriteClass,
    charge: output::WriteCharge,
    brand: marker::PhantomData<fn(&'id mut ()) -> &'id mut ()>,
}

impl<'id, 'q, 'd> Prepared<'id, 'q, 'd> {
    pub(crate) fn commit(self) -> Completion {
        self.budget.release(self.class, self.charge);
        match self.source {
            Source::Queued { .. } => Completion::Queued,
            Source::DecoderFeedback { wire } => {
                wire.clear();
                Completion::Feedback
            }
        }
    }

    pub(crate) fn restore(self, write: Flight<'id, 'd>) {
        self.source.restore(write.into_record());
    }

    pub(in crate::conn) fn finish(self, write: Flight<'id, 'd>) -> (output::Write<'d>, Completion) {
        let done = self.commit();
        (write.into_record().into_output(), done)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Completion {
    Queued,
    Feedback,
}

impl Completion {
    pub(crate) const fn is_feedback(self) -> bool {
        matches!(self, Self::Feedback)
    }
}

pub(in crate::conn) const fn regular_limit(capacity: usize, maximum: usize) -> usize {
    capacity
        - match capacity / 16 {
            0 => 1,
            reserve if reserve < maximum => reserve,
            _ => maximum,
        }
}

impl<'d, S, B> Queues<'d, S, B> {
    /// Classifies a returned error after its exclusive admission guard has ended.
    /// Free slots and the two disjoint lanes then partition the fixed capacity.
    pub(crate) fn is_pressure(&self, error: &conn::Error) -> bool {
        match error {
            conn::Error::Egress(group::Error::Entries(shortage)) => {
                let capacity = self.writes.available()
                    + self.writes.lane_len(budget::WriteClass::Regular.lane())
                    + self.writes.lane_len(budget::WriteClass::Critical.lane());
                shortage.requested.get() <= capacity
            }
            _ => error.is_pressure(),
        }
    }

    pub(in crate::conn) fn try_event(
        &mut self,
    ) -> Result<ring::Vacant<'_, Events<'d, S, B>>, conn::Error> {
        let len = self.events.len();
        let capacity = self.events.capacity();
        match self.events.vacant() {
            Some(vacancy) => Ok(vacancy),
            None => Err(conn::Error::EventCapacity { len, capacity }),
        }
    }

    pub(in crate::conn) fn try_credit(
        &mut self,
    ) -> Result<ring::Vacant<'_, ring::Fifo<(Credit, usize)>>, conn::Error> {
        let len = self.receive_credits.len();
        let capacity = self.receive_credits.capacity();
        match self.receive_credits.vacant() {
            Some(vacancy) => Ok(vacancy),
            None => Err(conn::Error::CreditCapacity { len, capacity }),
        }
    }

    pub(in crate::conn) fn enqueue(
        &mut self,
        target: stream::Target,
        entry: Entry<'d>,
    ) -> Result<(), conn::Error> {
        match Single::new(self, target, entry) {
            Ok(permit) => {
                permit.commit::<hold::evidence::Off>();
                Ok(())
            }
            Err(rejected) => Err(rejected.discard()),
        }
    }

    pub(crate) fn has_write(&self, feedback: &State) -> bool {
        !feedback.is_empty() || !self.writes.is_empty()
    }

    pub(crate) fn take<'q>(
        &'q mut self,
        wire: &'q mut State,
        target: Option<stream::Target>,
    ) -> Option<TakenWrite<'q, 'd>> {
        if let Some(target) = target
            && !wire.is_empty()
        {
            let bytes = wire.bytes();
            return Some(TakenWrite {
                write: plan::Record::single(
                    target,
                    output::WritePrefix::Inline(bytes),
                    Boundary::More,
                ),
                source: Source::DecoderFeedback { wire },
                class: budget::WriteClass::Critical,
                budget: &mut self.write_budget,
            });
        }
        let order = [budget::WriteClass::Critical, budget::WriteClass::Regular]
            .map(|class| (class.lane(), class));
        let (class, taken) = self.writes.take_first(order)?;
        let (write, vacancy) = taken;
        Some(TakenWrite {
            write,
            source: Source::Queued { vacancy },
            class,
            budget: &mut self.write_budget,
        })
    }

    pub(crate) fn discard(
        &mut self,
        stream: Id,
    ) -> Result<hold::Charge, (conn::Error, hold::Charge)> {
        use group::drain::Drain;

        let class = budget::WriteClass::Regular;
        let mut retired = hold::Charge::default();
        let result = (|| {
            let Some(mut drain) = Drain::new(&mut self.writes, stream.get(), class.lane()) else {
                return Ok(());
            };
            while let Some(item) = drain.next_entry() {
                let write = item.get();
                let charge = write.charge();
                let retained = write.retained();
                let next = retired.try_add(retained).map_err(conn::Error::Retention)?;
                item.remove();
                self.write_budget.release(class, charge);
                retired = next;
            }
            Ok(())
        })();
        result.map(|()| retired).map_err(|error| (error, retired))
    }

    pub(crate) fn close(mut self, mut release: impl FnMut(Id, hold::Charge)) {
        let order = [budget::WriteClass::Critical, budget::WriteClass::Regular]
            .map(|class| (class.lane(), ()));
        while let Some((id, charge)) = self
            .writes
            .take_first(order)
            .map(|((), (write, _))| (write.stream_id(), write.retained()))
        {
            release(id, charge);
        }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Completion>() == 1);
    assert!(core::mem::size_of::<Source<'static, 'static>>() <= 4 * core::mem::size_of::<usize>());
    assert!(
        core::mem::size_of::<Prepared<'static, 'static, 'static>>()
            <= 8 * core::mem::size_of::<usize>()
    );
};

/// The original write, invariant in one admission operation's brand.
#[repr(transparent)]
pub(crate) struct Flight<'id, 'd> {
    write: plan::Record<'d>,
    brand: marker::PhantomData<fn(&'id mut ()) -> &'id mut ()>,
}

impl<'id, 'd> Flight<'id, 'd> {
    const fn new(write: plan::Record<'d>) -> Self {
        Self {
            write,
            brand: marker::PhantomData,
        }
    }

    pub(crate) const fn get(&self) -> &output::Write<'d> {
        self.write.get()
    }

    fn into_record(self) -> plan::Record<'d> {
        self.write
    }

    pub(crate) fn retained(&self) -> hold::Charge {
        self.write.retained()
    }
}

impl<'d> ops::Deref for Flight<'_, 'd> {
    type Target = output::Write<'d>;
    fn deref(&self) -> &Self::Target {
        self.write.get()
    }
}

// SAFETY: the original batch stays owned and unchanged until admission consumes it.
unsafe impl<'d> send::raw::Batch<resident::Shared<'d>> for Flight<'_, 'd> {
    fn demand(&self) -> Result<send::Demand, send::Overflow> {
        self.write.demand()
    }

    fn into_parts(self) -> send::Parts<resident::Shared<'d>> {
        self.write.into_parts()
    }
}

const _: () = assert!(size_of::<Flight<'static, 'static>>() == size_of::<output::Write<'static>>());
