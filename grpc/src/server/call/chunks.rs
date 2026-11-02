use std::num::NonZero;

use o3::{
    alloc,
    buffer::segments,
    depot::{capacity, queue::pool},
};
use sark_core::{life::session, manifold::stream::Boundary};

use crate::{
    details, message,
    server::call::{credits, input},
};

#[derive(Default)]
pub(in crate::server) struct State {
    boundary: Boundary,
    charge: credits::Charge,
}

impl State {
    pub(in crate::server) fn end(&mut self) {
        self.boundary = Boundary::End;
    }
}

pub(in crate::server) struct Pool<C: session::Storage, P> {
    lanes: pool::Lanes<message::DataChunk<C, P>>,
    capacity: capacity::Count,
    ledger: credits::Ledger,
}

/// One admitted ingress lane retaining its node pool and credit ledger.
pub(in crate::server) struct Queue<'a, C: session::Storage, P> {
    lane: pool::Lane<'a, message::DataChunk<C, P>>,
    ledger: &'a mut credits::Ledger,
}

impl<C: session::Storage, P> Pool<C, P> {
    pub(in crate::server) fn try_new(
        cap: capacity::Count,
        calls: capacity::Count,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            lanes: pool::Lanes::try_new(pool::Plan::from_counts(cap, calls))?,
            capacity: cap,
            ledger: credits::Ledger::default(),
        })
    }

    pub(in crate::server) fn try_bind(
        &mut self,
        index: usize,
    ) -> Result<Queue<'_, C, P>, pool::Missing> {
        Ok(Queue {
            lane: self.lanes.try_bind_mut(index)?,
            ledger: &mut self.ledger,
        })
    }

    pub(in crate::server) fn is_pending(&self, index: usize, state: &State) -> bool {
        self.lanes
            .get(index)
            .is_some_and(|lane| !lane.is_empty() || matches!(state.boundary, Boundary::End))
    }

    pub(in crate::server) fn release_live(
        &mut self,
        stream: &mut input::Input<C>,
        bytes: usize,
    ) -> Result<(), details::Credit> {
        self.ledger.shrink(&mut stream.data.charge, bytes)?.commit();
        Ok(())
    }

    pub(in crate::server) fn is_empty(&self) -> bool {
        self.lanes.available() == self.capacity.get() && self.ledger.used() == 0
    }
}

impl<C: session::Storage, P> Queue<'_, C, P> {
    pub(in crate::server) fn is_pending(&self, state: &State) -> bool {
        !self.lane.is_empty() || matches!(state.boundary, Boundary::End)
    }

    pub(in crate::server) fn try_stage(
        mut self,
        stream: &mut input::Input<C>,
        chunk: message::DataChunk<C, P>,
        boundary: Boundary,
        local: usize,
        total: usize,
    ) -> Result<(), details::Chunks> {
        if matches!(stream.data.boundary, Boundary::End) {
            return Err(details::Chunks::Closed);
        }
        if chunk.is_empty() {
            stream.data.boundary = boundary;
            return Ok(());
        }
        let update = self
            .ledger
            .try_grow(&mut stream.data.charge, chunk.len(), local, total)
            .map_err(details::Chunks::Grow)?;
        let vacant = self
            .lane
            .vacant()
            .ok_or(details::Chunks::Full(capacity::Shortage {
                requested: NonZero::<usize>::MIN,
                available: 0,
            }))?;
        vacant.append(chunk);
        stream.data.boundary = boundary;
        update.commit();
        Ok(())
    }

    pub(in crate::server) fn try_deframe(
        &mut self,
        stream: &mut input::Input<C>,
        segments: &segments::Pool<C>,
    ) -> Result<input::Turn<C>, crate::Status> {
        if matches!(stream.request, input::Request::Finished) {
            return Ok(input::Turn::Complete);
        }
        let next = match self.lane.front_mut() {
            Some(mut front) => {
                let chunk = front.get_mut();
                let next = stream.deframer.next(chunk, segments);
                if chunk.is_empty() {
                    front.pop();
                }
                next
            }
            None => Ok(None),
        };
        Ok(match next? {
            Some(frame) => input::Turn::Frame(frame.into_plain()?),
            None => match (self.lane.is_empty(), stream.data.boundary) {
                (false, _) => input::Turn::Pending,
                (true, Boundary::More) => input::Turn::Idle,
                (true, Boundary::End) if stream.deframer.is_idle() => input::Turn::Complete,
                (true, Boundary::End) => input::Turn::Incomplete,
            },
        })
    }

    pub(in crate::server) fn try_release(
        mut self,
        stream: &mut input::Input<C>,
    ) -> Result<(), details::Chunks> {
        let bytes = stream.data.charge.amount();
        let release = self
            .ledger
            .shrink(&mut stream.data.charge, bytes)
            .map_err(details::Chunks::Shrink)?;
        self.lane.clear();
        release.commit();
        Ok(())
    }
}
