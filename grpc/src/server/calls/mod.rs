use o3::{
    alloc,
    buffer::segments,
    depot::{capacity::Count, queue::pool},
};
use sark_core::{
    life::session,
    manifold::{self, schedule, stream::Boundary},
};
use sark_h2::stream::Key;

use crate::{
    Reason, details, message,
    server::{
        call::{self, chunks, input, messages},
        runtime::state::active,
        service::handler::Production,
    },
};

pub(crate) mod capacity;

#[derive(Debug)]
pub(crate) enum Reserve {
    Capacity(capacity::Error),
    Occupied,
}

pub(super) enum Stage {
    Unknown,
    Failed(crate::Status),
}

pub(super) enum Error {
    Call(details::Mismatch),
    Credit(details::Credit),
    Chunks(details::Chunks),
    Lane(pool::Missing),
    State,
}

impl Error {
    pub(super) fn status(self) -> crate::Status {
        match self {
            Self::Call(error) => error.into(),
            Self::Credit(error) => crate::Status::credit(Reason::IngressAccounting, error),
            Self::Chunks(error) => error.into(),
            Self::Lane(error) => error.into(),
            Self::State => crate::Status::new(crate::Code::Internal, Reason::RequestUnknown),
        }
    }
}

pub(super) struct Store<'r, T, C, P, B>
where
    C: session::Storage,
    B: schedule::Block,
{
    pub(super) records: schedule::Runtime<Key, call::State<'r, T, C>, B>,
    pub(super) data: chunks::Pool<C, P>,
    pub(super) messages: pool::Lanes<message::Frame<C>>,
}

impl<'r, T, C, P, B> Store<'r, T, C, P, B>
where
    C: session::Storage,
    B: schedule::Block,
{
    pub(super) fn try_new(
        cap: Count,
        data: Count,
        messages: pool::Plan,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            records: schedule::Runtime::try_bounded(cap, cap)?,
            data: chunks::Pool::try_new(data, cap)?,
            messages: pool::Lanes::try_new(messages)?,
        })
    }

    pub(in crate::server) fn get_mut(&mut self, key: Key) -> Option<&mut call::State<'r, T, C>> {
        self.records.get_mut(key)
    }

    pub(in crate::server) fn capacity(&self) -> usize {
        self.records.capacity()
    }

    pub(in crate::server) fn has_runnable(&self) -> bool {
        self.records.has_runnable()
    }

    pub(in crate::server) fn wake_egress(&mut self) {
        self.records.wake_egress();
    }

    pub(in crate::server) fn key_at(&self, index: usize) -> Option<Key> {
        self.records.key_at(index)
    }

    pub(in crate::server) fn entry(&self, key: Key) -> Option<active::Entry<'r>> {
        Some(self.records.get(key)?.entry)
    }

    pub(in crate::server) fn entry_at(&self, index: usize) -> Option<active::Entry<'r>> {
        Some(self.records.entry_at(index)?.1.entry)
    }

    pub(in crate::server) fn production(&self, key: Key) -> Option<Production> {
        self.records.get(key)?.production()
    }

    pub(in crate::server) fn wake(&mut self, wake: schedule::Wake<Key>) -> usize {
        self.records.wake(wake)
    }

    pub(super) fn stream_mut(&mut self, key: Key) -> Option<&mut input::Input<C>> {
        self.records.get_mut(key)?.active()?.input.as_mut()
    }

    pub(super) fn has_input(&mut self, key: Key) -> bool {
        self.records
            .get_mut(key)
            .and_then(|record| record.active()?.input.as_mut())
            .is_some_and(|stream| {
                self.data
                    .is_pending(manifold::raw::Key::slot(key), &stream.data)
            })
    }

    pub(super) fn handler_mut(&mut self, key: Key) -> Option<&mut T> {
        Some(&mut self.records.get_mut(key)?.active()?.handler)
    }

    pub(super) fn output(&mut self, key: Key) -> Option<(&mut T, Production)> {
        let call = self.records.get_mut(key)?.active()?;
        Some((&mut call.handler, call.output))
    }

    pub(super) fn stage_data(
        &mut self,
        key: Key,
        chunk: message::DataChunk<C, P>,
        boundary: Boundary,
        local: usize,
        total: usize,
    ) -> Result<(), Stage> {
        let Some(stream) = self
            .records
            .get_mut(key)
            .and_then(|record| record.active()?.input.as_mut())
        else {
            return Err(Stage::Unknown);
        };
        self.data
            .try_bind(manifold::raw::Key::slot(key))
            .map_err(|error| Stage::Failed(error.into()))?
            .try_stage(stream, chunk, boundary, local, total)
            .map_err(|error| Stage::Failed(error.into()))?;
        Ok(())
    }

    pub(super) fn deframe_turn(
        &mut self,
        key: Key,
        source: &segments::Pool<C>,
        limit: usize,
    ) -> Result<input::Turn<C>, crate::Status> {
        let Some(stream) = self
            .records
            .get_mut(key)
            .and_then(|record| record.active()?.input.as_mut())
        else {
            return Err(Error::State.status());
        };
        let mut data = self.data.try_bind(manifold::raw::Key::slot(key))?;
        let turn = data.try_deframe(stream, source)?;
        let input::Turn::Frame(frame) = turn else {
            return Ok(turn);
        };
        if stream.head.is_some() {
            messages::try_push(
                self.messages.try_bind_mut(manifold::raw::Key::slot(key))?,
                frame,
                limit,
            )?;
            return Ok(if data.is_pending(&stream.data) {
                input::Turn::Pending
            } else {
                input::Turn::Idle
            });
        }
        Ok(input::Turn::Frame(frame))
    }

    pub(super) fn release_live(&mut self, key: Key, bytes: usize) -> Result<(), Error> {
        let stream = self
            .records
            .get_mut(key)
            .and_then(|record| record.active()?.input.as_mut())
            .ok_or(Error::State)?;
        self.data.release_live(stream, bytes).map_err(Error::Credit)
    }

    pub(super) fn try_remove(&mut self, key: Key) -> Result<(), Error> {
        let mut entry = self.records.entry(key).ok_or(Error::State)?;
        Self::release(entry.get_mut(), &mut self.data, &mut self.messages)?;
        entry.discard();
        Ok(())
    }

    /// Ends the application half while preserving the committed response owner.
    pub(super) fn complete(&mut self, key: Key) -> Result<(), Error> {
        let record = self.records.get_mut(key).ok_or(Error::State)?;
        Self::release(record, &mut self.data, &mut self.messages)?;
        record.complete().map_err(Error::Call)
    }

    /// Keeps the exact call row until its scheduled error response is released.
    pub(super) fn reject(&mut self, key: Key) -> Result<(), Error> {
        let mut entry = self.records.entry(key).ok_or(Error::State)?;
        let record = entry.get_mut();
        Self::release(record, &mut self.data, &mut self.messages)?;
        record.reject();
        entry.activate(schedule::Activation::control(None));
        Ok(())
    }

    pub(super) fn release(
        record: &mut call::State<'r, T, C>,
        data: &mut chunks::Pool<C, P>,
        messages: &mut pool::Lanes<message::Frame<C>>,
    ) -> Result<(), Error> {
        let index = manifold::raw::Key::slot(record.entry.key());
        if let Some(stream) = record.active().and_then(|call| call.input.as_mut()) {
            data.try_bind(index)
                .map_err(Error::Lane)?
                .try_release(stream)
                .map_err(Error::Chunks)?;
            if let Some(mut lane) = messages.get_mut(index) {
                lane.clear();
            }
        }
        Ok(())
    }

    pub(super) fn is_quiescent(&self) -> bool {
        self.records.is_empty()
            && self.data.is_empty()
            && self.messages.available() == self.messages.capacity()
    }

    pub(in crate::server) fn try_reserve(
        &mut self,
        entry: active::Entry<'r>,
    ) -> Result<schedule::Vacant<'_, Key, call::State<'r, T, C>, B>, Reserve> {
        let active = self.records.len();
        let limit = self.records.capacity();
        if active >= limit {
            return Err(Reserve::Capacity(capacity::Error::new(active, limit)));
        }
        self.records.vacant(entry.key()).ok_or(Reserve::Occupied)
    }
}
