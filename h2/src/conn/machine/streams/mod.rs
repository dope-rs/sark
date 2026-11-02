use std::num::NonZero;

use conn::{
    event,
    outputs::{Barrier, modes},
    registry,
};
use o3::buffer::{resident, segments, storage};
use sark_core::{life::session, manifold::stream::Boundary};
use sark_http::{self, field::metadata::section::rules, payload::owned};

use crate::{
    conn,
    conn::machine,
    frame::{layout, numbers},
    stream,
};

mod headers;
mod prepared;
mod retained;
mod writer;

pub use headers::Headers;
use prepared::{Outcome, Prepared, Source};
pub use retained::Retained;
pub use writer::Writer;

pub(crate) enum Flow<'d> {
    Blocked(conn::DataBlock, segments::Nonempty<resident::Shared<'d>>),
    Sent {
        bytes: numbers::Length,
        rest: owned::Segments<'d>,
    },
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Limit(NonZero<usize>);

impl Limit {
    pub const MAX: Self = Self(NonZero::<usize>::MAX);
    pub(crate) const QUANTUM: Self = Self(NonZero::<usize>::MIN.saturating_add(64 * 1024 - 1));

    const fn get(self) -> usize {
        self.0.get()
    }
}

#[repr(transparent)]
pub struct Set<
    'a,
    R: crate::Role,
    F: rules::Policy = rules::RetainRaw,
    C: session::Storage = storage::Shared,
    A: rules::Push = rules::RejectPush,
    E: event::Evidence = event::Retirement,
> {
    pub(super) conn: &'a mut machine::Connection<R, F, C, A, E>,
}

const _: () = {
    use core::mem::{align_of, size_of};

    assert!(size_of::<Set<'static, crate::ClientRole>>() == size_of::<usize>());
    assert!(align_of::<Set<'static, crate::ClientRole>>() == align_of::<usize>());
    assert!(size_of::<Limit>() == size_of::<usize>());
    assert!(size_of::<Option<Limit>>() == size_of::<usize>());
    assert!(size_of::<Flow<'static>>() == 5 * size_of::<usize>());
};

impl<R, F, C, A, E> Set<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub fn retained(&mut self) -> Retained<'_, R, F, C, A, E> {
        Retained {
            conn: &mut *self.conn,
        }
    }

    pub fn headers(&mut self) -> Headers<'_, R, F, C, A, E> {
        Headers {
            conn: &mut *self.conn,
        }
    }

    /// Schedules a reset, returning the original key if it is no longer active.
    pub fn try_cancel(
        &mut self,
        key: stream::Key,
        error: numbers::ErrorCode,
    ) -> Result<(), stream::Key> {
        match self.conn.state.active_streams.begin_reset(key, error) {
            registry::Begin::Started => {
                self.finish_off(key);
                Ok(())
            }
            registry::Begin::Inactive => Err(key),
        }
    }

    pub fn cancel(&mut self, key: stream::Key, error: numbers::ErrorCode) {
        if matches!(
            self.conn.state.active_streams.begin_reset(key, error),
            registry::Begin::Started
        ) {
            self.finish_off(key);
        }
    }

    pub(crate) fn retire(
        &mut self,
        key: stream::Key,
        error: numbers::ErrorCode,
    ) -> Option<event::Retirement> {
        match self.conn.state.active_streams.begin_reset(key, error) {
            registry::Begin::Started => self.finish_on(key),
            registry::Begin::Inactive => None,
        }
    }

    fn finish_on(&mut self, key: stream::Key) -> Option<event::Retirement> {
        let done = self
            .conn
            .state
            .retire_write::<_, event::Retirement>(&mut self.conn.egress, key.id());
        self.flush_reset();
        done
    }

    fn finish_off(&mut self, key: stream::Key) {
        let _ = self
            .conn
            .state
            .retire_write::<_, event::Ignore>(&mut self.conn.egress, key.id());
        self.flush_reset();
    }

    fn flush_reset(&mut self) {
        self.conn.egress.outbound.set_barrier(Barrier::Blocked);
        self.conn.state.flush_resets(&mut self.conn.egress.outbound);
    }

    pub fn try_send(
        &mut self,
        id: stream::Id,
        data: &[u8],
        boundary: Boundary,
    ) -> Result<conn::DataOutcome, conn::Error> {
        let key = self
            .conn
            .status()
            .stream_key(id)
            .ok_or(conn::Error::UnknownStream(id))?;
        self.try_to(key, data, &[], boundary, Limit::MAX)
    }

    pub fn try_parts(
        &mut self,
        id: stream::Id,
        first: &[u8],
        second: &[u8],
        boundary: Boundary,
    ) -> Result<conn::DataOutcome, conn::Error> {
        let key = self
            .conn
            .status()
            .stream_key(id)
            .ok_or(conn::Error::UnknownStream(id))?;
        self.try_to(key, first, second, boundary, Limit::MAX)
    }

    pub fn try_to(
        &mut self,
        key: stream::Key,
        first: &[u8],
        second: &[u8],
        boundary: Boundary,
        max: Limit,
    ) -> Result<conn::DataOutcome, conn::Error> {
        let parts = layout::Parts::new(first, second);
        let ready = match Prepared::<_, modes::Buffered, _>::prepare(
            self.conn, key, parts, boundary, max,
        ) {
            Outcome::Ready(ready) => ready,
            Outcome::Blocked(block, _) => {
                return Ok(conn::DataOutcome::Blocked(block));
            }
            Outcome::Failed(error, _) => return Err(error),
        };
        let sent = ready.send()?;
        if matches!(sent.state, stream::State::Closed) {
            self.conn.state.active_streams.remove_key(key);
        }
        Ok(conn::DataOutcome::sent(sent.length.into_usize()))
    }

    pub fn try_shared(
        &mut self,
        id: stream::Id,
        data: &storage::Shared,
        boundary: Boundary,
    ) -> Result<conn::DataOutcome, conn::Error> {
        let key = self
            .conn
            .status()
            .stream_key(id)
            .ok_or(conn::Error::UnknownStream(id))?;
        let owner = C::from(data.try_clone().map_err(conn::Error::Retain)?);
        match self.retained().try_to(key, owner, boundary, Limit::MAX) {
            Ok((out, ..)) => Ok(out),
            Err((error, ..)) => Err(error),
        }
    }
}
