use std::{convert::Infallible, num::NonZero, pin, task};

use dope::{
    core::driver::route::Identity,
    manifold::client::{close, conn, port},
    net::link::egress::{Rejected, Rejection, load},
    runtime::client,
};
use dope_fiber::{
    abi, context,
    reply::recv::{self, attach::Attach as _},
    task::time::budget,
};
use o3::buffer::pool;
use sark_client::{Error, Failure, capacity, protocol::response, timeout};

use crate::client::{
    config::report,
    exchange::{
        net,
        request::{Encoded, Kind},
        response::phases,
    },
};

#[must_use = "the response stream must be consumed to observe the response"]
/// A live response registration scoped to its installed HTTP connector.
pub struct Stream<'app, 'e, 'b, 'c, 'd: 'app, B: ?Sized, ID: Identity, E = Infallible> {
    conn: conn::Id<'d, ID>,
    reply: recv::Stream<
        'app,
        'b,
        'c,
        'd,
        net::Outcome<'d, E>,
        net::Outcome<'d, E>,
        net::Outcome<'d, E>,
        Kind,
    >,
    io: &'app port::Port<'e, 'd, pool::Cursor, ID>,
    state: client::Bound<'app, 'd, B, State>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Active(NonZero<u8>),
    Done,
}

impl<'app, 'e, 'b, 'c, 'd: 'app, B: ?Sized, ID: Identity> Stream<'app, 'e, 'b, 'c, 'd, B, ID> {
    pub(in crate::client) fn submit<'turn, 'f, const CLOSE: bool, Tag, E, F>(
        io: &'app port::Port<'e, 'd, pool::Cursor, ID>,
        bound: client::Bound<'app, 'd, B>,
        mut claim: net::Claim<'app, 'turn, 'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>,
        req: Encoded,
        check: budget::Check<'d>,
    ) -> Result<Stream<'app, 'e, 'b, 'c, 'd, B, ID, F::Detail<'f>>, Error<F::Detail<'f>>>
    where
        Tag: net::PoolTag<'d, ID>,
        F: report::Policy<E>,
        E: 'f,
    {
        match Self::try_submit(io, bound, &mut claim, req, check) {
            Ok(stream) => {
                claim.commit();
                Ok(stream)
            }
            Err(error) => {
                if CLOSE || matches!(error, Error::NotConnected) {
                    claim.retire();
                } else {
                    claim.release();
                }
                Err(error)
            }
        }
    }

    fn try_submit<'turn, 'f, Tag, E, F>(
        io: &'app port::Port<'e, 'd, pool::Cursor, ID>,
        bound: client::Bound<'app, 'd, B>,
        claim: &mut net::Claim<'app, 'turn, 'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>,
        req: Encoded,
        check: budget::Check<'d>,
    ) -> Result<Stream<'app, 'e, 'b, 'c, 'd, B, ID, F::Detail<'f>>, Error<F::Detail<'f>>>
    where
        Tag: net::PoolTag<'d, ID>,
        F: report::Policy<E>,
        E: 'f,
    {
        use port::write::Reserve;

        let conn = claim.connection();
        let lane = claim.lane();
        let (wire, kind) = req.into_parts();
        let item = match io.try_reserve(claim.region(), conn, wire) {
            Reserve::Item(item) => item,
            Reserve::Unknown(wire) | Reserve::Inactive(wire) => {
                drop(wire);
                return Err(Error::NotConnected);
            }
            Reserve::Rejected(cause) => {
                let (load, cause) = cause.into_parts();
                let charge = load.charge();
                drop(load.into_inner());
                return Err(Error::Capacity(capacity::Error::RequestEgress(
                    Rejection::Capacity { charge, cause },
                )));
            }
            Reserve::Invalid(cause) => {
                let (load, cause) = Rejected::<load::Value<_>>::from(cause).into_parts();
                drop(load.into_inner());
                return Err(Error::Capacity(capacity::Error::RequestEgress(cause)));
            }
            Reserve::Empty(cause) => {
                let (load, cause) = cause.into_parts();
                drop(load.into_inner());
                return Err(Error::Capacity(capacity::Error::RequestEgress(
                    Rejection::Empty(cause),
                )));
            }
        };
        let item = item.validate().map_err(|wire| {
            drop(wire.into_inner());
            Error::NotConnected
        })?;
        item.try_commit(move |token| {
            let mut reply = recv::Stream::new();
            check
                .check()
                .map_err(|elapsed| Error::Timeout(timeout::Phase::Dispatch.expire(elapsed)))?;
            reply.try_tagged(token, lane, kind).map_err(Error::Reply)?;
            Ok(Stream {
                conn,
                reply,
                io,
                state: bound.bind(State::Active(NonZero::<u8>::MIN)),
            })
        })
        .map_err(|rejected| {
            let (wire, cause) = rejected.into_parts();
            drop(wire.into_inner());
            cause
        })
    }
}

impl<'app, 'e, 'b, 'c, 'd: 'app, B: ?Sized, ID: Identity, E>
    Stream<'app, 'e, 'b, 'c, 'd, B, ID, E>
{
    /// Waits for the next response event.
    ///
    /// `None` is the typed end-of-response marker.
    pub fn next_event(
        &mut self,
    ) -> impl abi::Fiber<'d, Output = Option<Result<response::Event<'d>, Failure<E>>>> + '_ {
        use dope_fiber::abi::poll_fn;
        let stream = self;
        poll_fn(move |cx: pin::Pin<&mut context::Context<'_, 'd>>| {
            phases::Events::poll_next(stream, cx)
        })
    }
}

impl<'d, B: ?Sized, ID: Identity, E> phases::Events<'d> for Stream<'_, '_, '_, '_, 'd, B, ID, E> {
    type Error = E;

    fn attempted(&mut self, attempt: NonZero<u8>) {
        *self.state = State::Active(attempt);
    }

    fn poll_next(
        &mut self,
        cx: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<Option<Result<response::Event<'d>, Failure<E>>>> {
        let State::Active(attempt) = *self.state else {
            return task::Poll::Ready(None);
        };
        let poll = pin::Pin::new(&mut self.reply).poll_next(cx);
        if matches!(poll, task::Poll::Ready(None)) {
            *self.state = State::Done;
        }
        poll.map(|event| event.map(|result| result.map_err(|failure| failure.attempted(attempt))))
    }
}

impl<B: ?Sized, ID: Identity, E> Drop for Stream<'_, '_, '_, '_, '_, B, ID, E> {
    fn drop(&mut self) {
        if matches!(*self.state, State::Active(_)) {
            use close::{Directive, LocalOrigin, Reason};

            self.io.close(
                self.conn,
                Directive::reconnect(Reason::Local(LocalOrigin::OperationCancelled)),
            );
        }
    }
}

const _: () = {
    assert!(size_of::<State>() == size_of::<u8>());
    assert!(size_of::<client::Bound<'static, 'static, (), State>>() == size_of::<State>());
    assert!(align_of::<client::Bound<'static, 'static, (), State>>() == align_of::<State>());
};
