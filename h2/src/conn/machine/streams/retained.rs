use core::{mem, num::NonZero, ops::Deref};

use o3::{
    buffer,
    buffer::{hold, resident, segments, storage, write::consume::Consume as _},
    mem::rc,
};
use sark_core::{life::session, manifold::stream::Boundary};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{
        self, event, machine,
        machine::streams,
        outputs::{modes, payloads},
    },
    stream,
};

#[repr(transparent)]
pub struct Retained<
    'a,
    R: crate::Role,
    F: rules::Policy = rules::RetainRaw,
    C: session::Storage = storage::Shared,
    A: rules::Push = rules::RejectPush,
    E: event::Evidence = event::Retirement,
> {
    pub(super) conn: &'a mut machine::Connection<R, F, C, A, E>,
}

pub(in crate::conn::machine) trait Body:
    Deref<Target = storage::Shared>
{
    fn try_take(self, complete: bool) -> Result<storage::Shared, rc::Error>;
}

impl Body for &storage::Shared {
    fn try_take(self, _: bool) -> Result<storage::Shared, rc::Error> {
        self.try_clone()
    }
}

impl Body for &mut storage::Shared {
    fn try_take(self, complete: bool) -> Result<storage::Shared, rc::Error> {
        if complete {
            Ok(mem::take(self))
        } else {
            self.try_clone()
        }
    }
}

pub(super) struct Parts<'a, B: Body> {
    pub(super) prefix: &'a [u8],
    pub(super) body: B,
    pub(super) off: usize,
    len: usize,
}

pub(super) struct Front<'d>(pub(super) segments::Nonempty<resident::Shared<'d>>);

impl streams::Source for Front<'_> {
    fn len(&self) -> usize {
        self.0.front().len()
    }

    fn resident(&self, amount: usize) -> usize {
        hold::Charge::from_owner(amount, buffer::Residency::resident(self.0.front())).resident_bytes
    }
}

const _: () = {
    use core::mem::{align_of, size_of};

    assert!(size_of::<Retained<'static, crate::ClientRole>>() == size_of::<usize>());
    assert!(align_of::<Retained<'static, crate::ClientRole>>() == align_of::<usize>());
};

impl<B: Body> Parts<'_, B> {
    pub(super) fn try_take<C: session::Storage>(
        self,
        amount: usize,
    ) -> Result<(payloads::Inline, Option<C>), rc::Error> {
        let head = amount.min(self.prefix.len());
        let used = amount - head;
        let inline = payloads::Inline::prefix(self.prefix, head);
        let body = if used == 0 {
            None
        } else {
            let mut body = self.body.try_take(amount == self.len)?;
            body.consume_max(self.off);
            let part = buffer::raw::Segment::prefix(body, used);
            Some(C::from(part))
        };
        Ok((inline, body))
    }
}

impl<B: Body> streams::Source for Parts<'_, B> {
    fn len(&self) -> usize {
        self.len
    }

    fn resident(&self, amount: usize) -> usize {
        let body = amount.saturating_sub(self.prefix.len());
        hold::Charge::from_owner(body, buffer::Residency::resident(&*self.body)).resident_bytes
    }
}

impl<R, F, C, A, E> Retained<'_, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub(crate) fn try_to(
        &mut self,
        key: stream::Key,
        data: C,
        boundary: Boundary,
        max: streams::Limit,
    ) -> Result<(conn::DataOutcome, Option<C>), (conn::Error, C)> {
        let ready = match streams::Prepared::<_, modes::Retained, _>::prepare(
            self.conn, key, data, boundary, max,
        ) {
            streams::Outcome::Ready(ready) => ready,
            streams::Outcome::Blocked(block, data) => {
                return Ok((conn::DataOutcome::Blocked(block), Some(data)));
            }
            streams::Outcome::Failed(error, data) => return Err((error, data)),
        };
        let (sent, rest) = ready.send()?;
        if matches!(sent.state, stream::State::Closed) {
            self.conn.state.active_streams.remove_key(key, false);
        }
        Ok((conn::DataOutcome::sent(sent.length.into_usize()), rest))
    }

    /// Queues a copied prefix and retained owner range without copying the body.
    pub(in crate::conn::machine) fn try_parts(
        &mut self,
        key: stream::Key,
        prefix: &[u8],
        body: impl Body,
        off: usize,
        boundary: Boundary,
    ) -> Result<conn::DataOutcome, conn::Error> {
        let tail = body.len().checked_sub(off).ok_or(conn::Error::DataRange {
            offset: off,
            prefix: prefix.len(),
            body: body.len(),
        })?;
        let parts = Parts {
            prefix,
            body,
            off,
            len: prefix.len() + tail,
        };
        let limit = if prefix.len() > payloads::INLINE_PREFIX {
            streams::Limit(NonZero::<usize>::MIN.saturating_add(payloads::INLINE_PREFIX - 1))
        } else {
            streams::Limit::QUANTUM
        };
        let ready = match streams::Prepared::<_, modes::Retained, _>::prepare(
            self.conn, key, parts, boundary, limit,
        ) {
            streams::Outcome::Ready(ready) => ready,
            streams::Outcome::Blocked(block, _) => {
                return Ok(conn::DataOutcome::Blocked(block));
            }
            streams::Outcome::Failed(error, _) => return Err(error),
        };
        let sent = ready.send()?;
        if matches!(sent.state, stream::State::Closed) {
            self.conn.state.active_streams.remove_key(key, false);
        }
        Ok(conn::DataOutcome::sent(sent.length.into_usize()))
    }
}

impl<'d, R, F, A, E> Retained<'_, R, F, resident::Shared<'d>, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    A: rules::Push,
    E: event::Evidence,
{
    pub(crate) fn try_chunk(
        &mut self,
        key: stream::Key,
        data: segments::Nonempty<resident::Shared<'d>>,
        boundary: Boundary,
        max: streams::Limit,
    ) -> Result<streams::Flow<'d>, (conn::Error, segments::Nonempty<resident::Shared<'d>>)> {
        let ready = match streams::Prepared::<_, modes::Retained, _>::prepare(
            self.conn,
            key,
            Front(data),
            boundary,
            max,
        ) {
            streams::Outcome::Ready(ready) => ready,
            streams::Outcome::Blocked(block, Front(data)) => {
                return Ok(streams::Flow::Blocked(block, data));
            }
            streams::Outcome::Failed(error, Front(data)) => return Err((error, data)),
        };
        let (sent, rest) = ready.send()?;
        if matches!(sent.state, stream::State::Closed) {
            self.conn.state.active_streams.remove_key(key, false);
        }
        Ok(streams::Flow::Sent {
            bytes: sent.length,
            rest,
        })
    }
}
