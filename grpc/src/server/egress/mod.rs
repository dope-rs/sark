use std::ops::ControlFlow;

use o3::{
    alloc,
    buffer::storage,
    depot::{capacity, queue::pool},
};
use sark_core::manifold::{self, schedule, server, stream};

use crate::{
    message,
    protocol::{response, trailer},
    server::call::credits,
};

pub(in crate::server) mod commit;
pub(in crate::server) mod draft;
pub(in crate::server) mod driver;
use crate::server::runtime::run::Fault;
mod pending;

pub(super) enum Error<E> {
    Write(E),
    Fault(Fault),
}

/// Capacity returned by one committed output transition.
#[derive(Default)]
pub(super) struct Release {
    pub(super) prepared: bool,
    pub(super) output: bool,
}

/// An immutable payload whose length and complete framing were admitted together.
#[repr(transparent)]
struct Payload(storage::Shared);

impl Payload {
    fn shared(&self) -> &storage::Shared {
        &self.0
    }

    fn header(&self) -> [u8; message::HEADER_LEN] {
        message::Frame::header(message::Compression::Plain, self.0.len() as u32)
    }

    fn wire_len(&self) -> usize {
        self.0.len() + message::HEADER_LEN
    }
}

pub(in crate::server) struct Store<K: manifold::raw::Key> {
    lanes: pool::Lanes<Payload, pending::State<K>>,
    credits: credits::Ledger,
    wire_limit: usize,
    message_limit: capacity::Count,
    active: usize,
}

impl<K: manifold::raw::Key> Store<K> {
    pub(in crate::server) fn try_new(
        calls: capacity::Count,
        messages: capacity::Count,
        bytes: usize,
        limit: capacity::Count,
    ) -> Result<Self, alloc::Error> {
        let plan = pool::Plan::from_counts(messages, calls);
        Ok(Self {
            lanes: pool::Lanes::<_, pending::State<K>>::try_new(plan)?,
            credits: credits::Ledger::default(),
            wire_limit: bytes,
            message_limit: limit,
            active: 0,
        })
    }

    fn try_pending(&self, key: K) -> Result<bool, Fault> {
        let lane = self.lanes.try_bind(key.slot()).map_err(Fault::Lane)?;
        match lane.state().pending.as_ref() {
            Some(pending) if pending.key == key => Ok(true),
            Some(_) => Err(Fault::Protocol),
            None if lane.is_empty() => Ok(false),
            None => Err(Fault::Accounting),
        }
    }

    pub(super) fn remove<S: server::Connection<Key = K, Output = storage::Shared>>(
        &mut self,
        key: K,
        conn: &mut S,
    ) -> Result<Release, Fault> {
        self.replace(key, conn, None).map(|(released, _)| released)
    }

    pub(super) fn fail<S: server::Connection<Key = K, Output = storage::Shared>>(
        &mut self,
        key: K,
        conn: &mut S,
        status: crate::Status,
    ) -> Result<(Release, bool), Fault> {
        let trailer = pending::Trailer::new(status, crate::metadata::Metadata::new())
            .map_err(Fault::Response)?;
        self.replace(key, conn, Some(trailer))
    }

    fn replace<S: server::Connection<Key = K, Output = storage::Shared>>(
        &mut self,
        key: K,
        conn: &mut S,
        trailer: Option<pending::Trailer>,
    ) -> Result<(Release, bool), Fault> {
        let Self {
            lanes,
            credits,
            active,
            ..
        } = self;
        let mut lane = lanes.try_bind_mut(key.slot()).map_err(Fault::Lane)?;
        let empty = lane.is_empty();
        let state = lane.state_mut();
        let bytes = state.charge.amount();
        let pending = match state.pending.as_ref() {
            Some(pending) if pending.key == key => Some(pending),
            Some(_) => return Err(Fault::Protocol),
            None if empty && bytes == 0 => None,
            None => return Err(Fault::Accounting),
        };
        let output = trailer.and_then(|trailer| pending::Pending::failed(key, pending, trailer));
        let retained = output.is_some();
        let next = match (pending.is_some(), retained) {
            (true, false) => active.checked_sub(1).ok_or(Fault::Accounting)?,
            (false, true) => active.checked_add(1).ok_or(Fault::Accounting)?,
            (true, true) | (false, false) => *active,
        };
        let released = Release {
            prepared: pending
                .is_some_and(|pending| matches!(pending.phase, pending::Phase::Prepared(_))),
            output: !empty || bytes != 0,
        };
        let release = credits
            .shrink(&mut state.charge, bytes)
            .map_err(Fault::Credit)?;
        if released.prepared {
            stream::Write::discard(&mut conn.writer(key));
        }
        release.commit();
        lane.clear();
        lane.state_mut().pending = output;
        *active = next;
        Ok((released, retained))
    }

    pub(super) fn is_quiescent(&self) -> bool {
        self.active == 0
            && self.credits.used() == 0
            && self.lanes.available() == self.lanes.capacity()
    }

    pub(super) fn control<W>(
        &mut self,
        key: K,
        writer: impl FnOnce() -> W,
        released: &mut bool,
    ) -> Result<schedule::ControlTurn<W::Block>, Error<W::Error>>
    where
        W: stream::Write<storage::Shared>,
    {
        let mut lane = self
            .lanes
            .try_bind_mut(key.slot())
            .map_err(|error| Error::Fault(Fault::Lane(error)))?;
        let ready = !lane.is_empty();
        let current = lane
            .state_mut()
            .pending
            .as_mut()
            .ok_or(Error::Fault(Fault::Protocol))?;
        if current.key != key {
            return Err(Error::Fault(Fault::Protocol));
        }
        let mut writer = writer();
        let (boundary, control) = match &current.phase {
            pending::Phase::Head(metadata) => {
                let boundary = stream::Boundary::More;
                let control = writer
                    .headers(response::Fields::new(metadata), boundary)
                    .map_err(Error::Write)?;
                (boundary, control)
            }
            pending::Phase::Body { offset: _ } => {
                if ready {
                    return Err(Error::Fault(Fault::Protocol));
                }
                let trailing = current
                    .trailer
                    .as_ref()
                    .ok_or(Error::Fault(Fault::Protocol))?;
                let fields = trailer::Fields::from_lengths(
                    &trailing.status,
                    &trailing.metadata,
                    trailing.lengths,
                );
                let boundary = stream::Boundary::End;
                (
                    boundary,
                    writer.headers(fields, boundary).map_err(Error::Write)?,
                )
            }
            pending::Phase::Prepared(boundary) => {
                match writer.commit().map_err(Error::Write)? {
                    ControlFlow::Continue(()) => *released = true,
                    ControlFlow::Break(block) => {
                        return Ok(schedule::ControlTurn::blocked(block));
                    }
                }
                (*boundary, stream::Control::Written)
            }
        };
        Ok(match control {
            stream::Control::Written => match boundary {
                stream::Boundary::More => {
                    current.phase = pending::Phase::Body { offset: 0 };
                    match current.body(ready) {
                        Some(next) => schedule::ControlTurn::progress(next),
                        None => schedule::ControlTurn::idle(),
                    }
                }
                stream::Boundary::End => schedule::ControlTurn::complete(),
            },
            stream::Control::Prepared(block) => {
                current.phase = pending::Phase::Prepared(boundary);
                schedule::ControlTurn::blocked(block)
            }
            stream::Control::Blocked(block) => schedule::ControlTurn::blocked(block),
        })
    }

    pub(super) fn data<W>(
        &mut self,
        key: K,
        writer: impl FnOnce() -> W,
        released: &mut bool,
    ) -> Result<schedule::DataTurn<W::Block>, Error<W::Error>>
    where
        W: stream::Write<storage::Shared>,
    {
        let Self { lanes, credits, .. } = self;
        let mut lane = lanes
            .try_bind_mut(key.slot())
            .map_err(|error| Error::Fault(Fault::Lane(error)))?;
        let ready = lane.count().get() > 1;
        let mut front = lane.front_mut().ok_or(Error::Fault(Fault::Protocol))?;
        let (payload, state) = front.parts_mut();
        let current = state
            .pending
            .as_mut()
            .ok_or(Error::Fault(Fault::Accounting))?;
        if current.key != key {
            return Err(Error::Fault(Fault::Protocol));
        }
        let offset = match &mut current.phase {
            pending::Phase::Body { offset } => offset,
            pending::Phase::Head(_) | pending::Phase::Prepared(_) => {
                return Err(Error::Fault(Fault::Protocol));
            }
        };
        let header = payload.header();
        let mut writer = writer();
        let written = match writer
            .data(&header, payload.shared(), *offset, stream::Boundary::More)
            .map_err(Error::Write)?
        {
            stream::Data::Written(written) => written.get(),
            stream::Data::Blocked(block) => return Ok(schedule::DataTurn::Blocked(block)),
        };
        let len = payload.wire_len();
        let pos = offset
            .checked_add(written)
            .ok_or(Error::Fault(Fault::Accounting))?;
        if pos > len {
            return Err(Error::Fault(Fault::Protocol));
        }
        let release = credits
            .shrink(&mut state.charge, written)
            .map_err(|error| Error::Fault(Fault::Credit(error)))?;
        *offset = if pos == len { 0 } else { pos };
        release.commit();
        *released = true;
        if pos == len {
            let next = match current.body(ready) {
                Some(next) => schedule::DataTurn::Progress(next),
                None => schedule::DataTurn::Idle,
            };
            front.pop();
            return Ok(next);
        }
        Ok(schedule::DataTurn::Progress(schedule::WorkClass::Data))
    }
}

impl<E> From<Fault> for Error<E> {
    fn from(fault: Fault) -> Self {
        Self::Fault(fault)
    }
}
