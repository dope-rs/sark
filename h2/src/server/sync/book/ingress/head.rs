use std::ops::ControlFlow;

use o3::buffer::hold::evidence::Off;
use sark_core::{
    decode::{self, exec, request, state},
    manifold::{schedule::typed, stream::Boundary},
};
use sark_http::{
    field::metadata::section::{
        self,
        state::{RetainedHead, Selected},
    },
    payload::response::status::Code,
    request::body::Ref,
};

use crate::{
    conn::{self, event},
    frame::numbers,
    server::{
        self, payload,
        payload::Buffer,
        response,
        state::exchange::{Accepted, Deferred, Plain, Rejected, Routed, Sync, admit},
        sync::{
            self,
            book::{self, Control, Delayed, driver::Driver, ingress::Ingress, phase::Phase},
        },
    },
    stream,
};

type Retained<P> = RetainedHead<<P as section::Policy>::Block, Selected<P>>;

pub(in crate::server::sync) trait Begin<'d, R>:
    request::Selection<R::Prepared>
where
    R: decode::Decode<Prepared: request::Synchronous, Head: request::Head<Start = Self>>,
{
    fn begin(
        self,
        ingress: &mut Ingress<'_, 'd, R>,
        router: &R,
        stream: stream::Key,
        limits: server::Config,
        report: &mut impl FnMut(conn::Error),
    );

    fn resume<B, D: FnMut(conn::Error)>(
        driver: &mut Driver<'_, 'd, R, B, D>,
        stream: stream::Key,
        routed: Delayed<'d, R>,
    ) -> Control<'d, R>;
}

impl<'a, 'd: 'a, R> Ingress<'a, 'd, R>
where
    R: decode::Decode,
    R::Prepared: request::Synchronous,
    <<R::Head as request::Head>::Start as request::Selection<R::Prepared>>::Deferred<
        Routed<'d, R::Prepared, R::Routes>,
    >: Deferred<'d, R::Prepared, R::Routes>,
{
    fn try_route(
        &mut self,
        stream: stream::Key,
        route: Routed<'d, R::Prepared, R::Routes>,
    ) -> Result<(), Routed<'d, R::Prepared, R::Routes>>
    where
        <R::Head as request::Head>::Start: request::Selection<
                R::Prepared,
                Deferred<Routed<'d, R::Prepared, R::Routes>> = Routed<'d, R::Prepared, R::Routes>,
            >,
    {
        let Some(slot) = self.book.runtime.vacant(stream) else {
            return Err(route);
        };
        slot.insert(typed::Next::Control(
            Phase::Routed(route),
            Some(crate::conn::DataBlock::Egress),
        ));
        Ok(())
    }

    fn guard(
        &mut self,
        router: &R,
        stream: stream::Key,
        routed: request::Guarded<<R::Routes as state::Family<R::Prepared>>::State<state::Routed>>,
        report: &mut impl FnMut(conn::Error),
    ) -> Option<request::Body<request::Admitted<exec::Sync, R::Prepared, Buffer<'d>>>>
    where
        R: decode::Guarded,
    {
        let encoder = response::Encoder::new(self.connection, stream, Ref::from_slice(&[]));
        match admit::run::<R, _, Off, event::Ignore, _>(router, routed, encoder) {
            Ok(ControlFlow::Continue(plan)) => Some(Sync::new(plan).take()),
            Ok(ControlFlow::Break((reject, input))) => {
                // SAFETY: this newly rejected head has not charged any DATA.
                if let Err(error) = unsafe { book::raw::discard(input, &mut self.book.credit) } {
                    if let ControlFlow::Break(head) = reject {
                        head.discard(self.connection);
                    }
                    self.reset(stream, error.code());
                    return None;
                }
                match reject {
                    ControlFlow::Break(head) => {
                        let Some(slot) = self.book.runtime.vacant(stream) else {
                            head.discard(self.connection);
                            self.reset(stream, numbers::ErrorCode::EnhanceYourCalm);
                            return None;
                        };
                        slot.insert(typed::Next::Control(
                            Phase::Head(head),
                            Some(crate::conn::DataBlock::Egress),
                        ));
                    }
                    ControlFlow::Continue(receipt) => {
                        if let Some(send) = receipt.into_pending()
                            && let Err(failure) = self
                                .book
                                .runtime
                                .try_insert(stream, typed::Next::Data(send, None))
                        {
                            self.reject(failure);
                        }
                    }
                }
                None
            }
            Err((error, failed, input)) => {
                let error = sync::response(error, report);
                // SAFETY: this failed fresh head has not charged any DATA.
                let error = match unsafe { book::raw::discard(input, &mut self.book.credit) } {
                    Ok(()) => error,
                    Err(error) => error.code(),
                };
                self.reset(failed, error);
                None
            }
        }
    }

    fn status(&mut self, stream: stream::Key, status: Code, limits: server::Config) {
        let phase = Phase::Status(response::Status::new(status));
        self.admit(stream, phase, limits);
    }

    pub(super) fn select(
        &mut self,
        router: &R,
        stream: stream::Key,
        head: Retained<R::Plan>,
        boundary: Boundary,
        limits: server::Config,
    ) -> Option<<R::Head as request::Head>::Start> {
        let head = match router.try_head(head) {
            Ok(head) => head,
            Err(error) => {
                self.status(stream, error.status(), limits);
                return None;
            }
        };
        match request::Head::try_start(head, limits.max_request_body_bytes, boundary) {
            Ok(start) => Some(start),
            Err(error) => {
                let error = error.discard();
                self.reset(stream, payload::Error::Body(error).code());
                None
            }
        }
    }

    fn store(
        &mut self,
        stream: stream::Key,
        body: request::Body<request::Admitted<exec::Sync, R::Prepared, Buffer<'d>>>,
        limits: server::Config,
    ) {
        match body {
            request::Body::Finished(input) => {
                // SAFETY: this freshly admitted head has not charged any DATA.
                match unsafe { book::raw::try_body::<R>(input, &mut self.book.credit) } {
                    Ok(input) => self.admit(stream, Phase::Request(input), limits),
                    Err(error) => self.reset(stream, error),
                }
            }
            request::Body::Open(input) => {
                if let Err(input) = self.try_receive(stream, input) {
                    // SAFETY: failed initial insertion returns this uncharged body.
                    let error = match unsafe {
                        book::raw::discard(Rejected::Body(input.into_body()), &mut self.book.credit)
                    } {
                        Ok(()) => numbers::ErrorCode::EnhanceYourCalm,
                        Err(error) => error.code(),
                    };
                    self.reset(stream, error);
                }
            }
        }
    }

    fn plain<T>(&mut self, stream: stream::Key, request: request::Plain<T>, limits: server::Config)
    where
        T:
            state::Admission<
                    R::Prepared,
                    state::Routed,
                    Accepted<'d>,
                    Output = <<R::Prepared as request::Prepared>::Family as state::Family<
                        R::Prepared,
                    >>::State<Accepted<'d>>,
                >,
    {
        let admitted = Plain::new().admit::<R::Prepared, state::Routed, T>(request.into_state());
        self.store(stream, Sync::new(admitted).take(), limits)
    }

    fn checked(
        &mut self,
        router: &R,
        stream: stream::Key,
        request: request::Guarded<<R::Routes as state::Family<R::Prepared>>::State<state::Routed>>,
        limits: server::Config,
        report: &mut impl FnMut(conn::Error),
    ) where
        R: decode::Guarded,
        <R::Head as request::Head>::Start: request::Selection<
                R::Prepared,
                Deferred<Routed<'d, R::Prepared, R::Routes>> = Routed<'d, R::Prepared, R::Routes>,
            >,
    {
        if self.connection.output().len() >= limits.max_outbound_bytes
            || !self.connection.streams().headers().is_ready()
        {
            if let Err(input) = self.try_route(stream, Routed::new(request)) {
                // SAFETY: failed initial insertion returns this uncharged route.
                let error = match unsafe { book::raw::discard_route(input, &mut self.book.credit) }
                {
                    Ok(()) => numbers::ErrorCode::EnhanceYourCalm,
                    Err(error) => error.code(),
                };
                self.reset(stream, error);
            }
            return;
        }
        let body = match self.guard(router, stream, request, report) {
            Some(body) => body,
            None => return,
        };
        self.store(stream, body, limits);
    }
}

impl<'d, R, U> Begin<'d, R> for request::Plain<U>
where
    R: decode::Decode,
    R::Head: request::Head<Start = Self>,
    R::Prepared: request::Synchronous,
    U: state::Flow<R::Prepared, state::Routed, Family = R::Routes>
        + state::Admission<
            R::Prepared,
            state::Routed,
            Accepted<'d>,
            Output = <<R::Prepared as request::Prepared>::Family as state::Family<R::Prepared>>::State<Accepted<'d>>,
        >,
{
    fn begin(
        self,
        ingress: &mut Ingress<'_, 'd, R>,
        _: &R,
        stream: stream::Key,
        limits: server::Config,
        _: &mut impl FnMut(conn::Error),
    ) {
        ingress.plain(stream, self, limits);
    }

    fn resume<B, D: FnMut(conn::Error)>(
        _: &mut Driver<'_, 'd, R, B, D>,
        _: stream::Key,
        never: Delayed<'d, R>,
    ) -> Control<'d, R> {
        match never {}
    }
}

impl<'d, R> Begin<'d, R>
    for request::Guarded<<R::Routes as state::Family<R::Prepared>>::State<state::Routed>>
where
    R: decode::Guarded,
    R::Head: request::Head<Start = Self>,
    R::Prepared: request::Synchronous,
{
    fn begin(
        self,
        ingress: &mut Ingress<'_, 'd, R>,
        router: &R,
        stream: stream::Key,
        limits: server::Config,
        report: &mut impl FnMut(conn::Error),
    ) {
        ingress.checked(router, stream, self, limits, report);
    }

    fn resume<B, D: FnMut(conn::Error)>(
        driver: &mut Driver<'_, 'd, R, B, D>,
        stream: stream::Key,
        routed: Delayed<'d, R>,
    ) -> Control<'d, R> {
        driver.routed(stream, routed)
    }
}

impl<'d, R, U> Begin<'d, R>
    for request::Start<U, <R::Routes as state::Family<R::Prepared>>::State<state::Routed>>
where
    R: decode::Guarded,
    R::Head: request::Head<Start = Self>,
    R::Prepared: request::Synchronous,
    U: state::Admission<
            R::Prepared,
            state::Routed,
            Accepted<'d>,
            Output = <<R::Prepared as request::Prepared>::Family as state::Family<R::Prepared>>::State<Accepted<'d>>,
        >,
{
    fn begin(
        self,
        ingress: &mut Ingress<'_, 'd, R>,
        router: &R,
        stream: stream::Key,
        limits: server::Config,
        report: &mut impl FnMut(conn::Error),
    ) {
        match self {
            request::Start::Plain(request) => ingress.plain(stream, request, limits),
            request::Start::Guarded(request) => ingress.checked(router, stream, request, limits, report),
        }
    }

    fn resume<B, D: FnMut(conn::Error)>(
        driver: &mut Driver<'_, 'd, R, B, D>,
        stream: stream::Key,
        routed: Delayed<'d, R>,
    ) -> Control<'d, R> {
        driver.routed(stream, routed)
    }
}
