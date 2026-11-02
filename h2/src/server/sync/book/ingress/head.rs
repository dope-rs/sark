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
    conn::event,
    frame::numbers,
    server::{
        self, payload,
        payload::Buffer,
        response,
        state::exchange::{Accepted, Deferred, Plain, Rejected, Routed, Sync, admit},
        sync::book::{self, ingress::Ingress, phase::Phase},
    },
    stream,
};

type Retained<P> = RetainedHead<<P as section::Policy>::Block, Selected<P>>;

pub(in crate::server::sync) trait Begin<S> {
    type Router: decode::Decode;

    fn begin(
        &mut self,
        router: &Self::Router,
        stream: stream::Key,
        head: Retained<<Self::Router as decode::Decode>::Plan>,
        boundary: Boundary,
        limits: server::Config,
    );
}

pub(in crate::server::sync) struct Head<'i, 'a, 'd: 'a, R: decode::Decode> {
    ingress: &'i mut Ingress<'a, 'd, R>,
}

impl<'i, 'a, 'd: 'a, R> Head<'i, 'a, 'd, R>
where
    R: decode::Decode,
    R::Prepared: request::Synchronous,
    <<R::Head as request::Head>::Start as request::Selection<R::Prepared>>::Deferred<
        Routed<'d, R::Prepared, R::Routes>,
    >: Deferred<'d, R::Prepared, R::Routes>,
{
    pub(super) const fn new(ingress: &'i mut Ingress<'a, 'd, R>) -> Self {
        Self { ingress }
    }

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
        let Some(slot) = self.ingress.book.runtime.vacant(stream) else {
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
    ) -> Option<request::Body<request::Admitted<exec::Sync, R::Prepared, Buffer<'d>>>>
    where
        R: decode::Guarded,
    {
        let encoder = response::Encoder::new(self.ingress.connection, stream, Ref::from_slice(&[]));
        match admit::run::<R, _, Off, event::Ignore>(router, routed, encoder) {
            Ok(ControlFlow::Continue(plan)) => Some(Sync::new(plan).take()),
            Ok(ControlFlow::Break((reject, input))) => {
                // SAFETY: this newly rejected head has not charged any DATA.
                if let Err(error) =
                    unsafe { book::raw::discard(input, &mut self.ingress.book.credit) }
                {
                    if let ControlFlow::Break(head) = reject {
                        head.discard(self.ingress.connection);
                    }
                    self.ingress.reset(stream, error.code());
                    return None;
                }
                match reject {
                    ControlFlow::Break(head) => {
                        let Some(slot) = self.ingress.book.runtime.vacant(stream) else {
                            head.discard(self.ingress.connection);
                            self.ingress
                                .reset(stream, numbers::ErrorCode::EnhanceYourCalm);
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
                                .ingress
                                .book
                                .runtime
                                .try_insert(stream, typed::Next::Data(send, None))
                        {
                            self.ingress.reject(failure);
                        }
                    }
                }
                None
            }
            Err((error, failed, input)) => {
                // SAFETY: this failed fresh head has not charged any DATA.
                let error =
                    match unsafe { book::raw::discard(input, &mut self.ingress.book.credit) } {
                        Ok(()) => error,
                        Err(error) => error.code(),
                    };
                self.ingress.reset(failed, error);
                None
            }
        }
    }

    fn status(&mut self, stream: stream::Key, status: Code, limits: server::Config) {
        let phase = Phase::Status(response::Status::new(status));
        self.ingress.admit(stream, phase, limits);
    }

    fn select(
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
                self.ingress
                    .reset(stream, payload::Error::Body(error).code());
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
                match unsafe { book::raw::try_body::<R>(input, &mut self.ingress.book.credit) } {
                    Ok(input) => self.ingress.admit(stream, Phase::Request(input), limits),
                    Err(error) => self.ingress.reset(stream, error),
                }
            }
            request::Body::Open(input) => {
                if let Err(input) = self.ingress.try_receive(stream, input) {
                    // SAFETY: failed initial insertion returns this uncharged body.
                    let error = match unsafe {
                        book::raw::discard(
                            Rejected::Body(input.into_body()),
                            &mut self.ingress.book.credit,
                        )
                    } {
                        Ok(()) => numbers::ErrorCode::EnhanceYourCalm,
                        Err(error) => error.code(),
                    };
                    self.ingress.reset(stream, error);
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
    ) where
        R: decode::Guarded,
        <R::Head as request::Head>::Start: request::Selection<
                R::Prepared,
                Deferred<Routed<'d, R::Prepared, R::Routes>> = Routed<'d, R::Prepared, R::Routes>,
            >,
    {
        if self.ingress.connection.output().len() >= limits.max_outbound_bytes
            || !self.ingress.connection.streams().headers().is_ready()
        {
            if let Err(input) = self.try_route(stream, Routed::new(request)) {
                // SAFETY: failed initial insertion returns this uncharged route.
                let error =
                    match unsafe { book::raw::discard_route(input, &mut self.ingress.book.credit) }
                    {
                        Ok(()) => numbers::ErrorCode::EnhanceYourCalm,
                        Err(error) => error.code(),
                    };
                self.ingress.reset(stream, error);
            }
            return;
        }
        let body = match self.guard(router, stream, request) {
            Some(body) => body,
            None => return,
        };
        self.store(stream, body, limits);
    }
}

impl<'i, 'a, 'd: 'a, R, U> Begin<request::Plain<U>> for Head<'i, 'a, 'd, R>
where
    R: decode::Decode,
    R::Head: request::Head<Start = request::Plain<U>>,
    R::Prepared: request::Synchronous,
    U: state::Flow<R::Prepared, state::Routed, Family = R::Routes>
        + state::Admission<
            R::Prepared,
            state::Routed,
            Accepted<'d>,
            Output = <<R::Prepared as request::Prepared>::Family as state::Family<R::Prepared>>::State<Accepted<'d>>,
        >,
{
    type Router = R;

    fn begin(
        &mut self,
        router: &R,
        stream: stream::Key,
        head: Retained<R::Plan>,
        boundary: Boundary,
        limits: server::Config,
    ) {
        let Some(request) = self.select(router, stream, head, boundary, limits)
        else {
            return;
        };
        self.plain(stream, request, limits);
    }
}

impl<'i, 'a, 'd: 'a, R>
    Begin<request::Guarded<<R::Routes as state::Family<R::Prepared>>::State<state::Routed>>>
    for Head<'i, 'a, 'd, R>
where
    R: decode::Guarded,
    R::Head: request::Head<
            Start = request::Guarded<
                <R::Routes as state::Family<R::Prepared>>::State<state::Routed>,
            >,
        >,
    R::Prepared: request::Synchronous,
{
    type Router = R;

    fn begin(
        &mut self,
        router: &R,
        stream: stream::Key,
        head: Retained<R::Plan>,
        boundary: Boundary,
        limits: server::Config,
    ) {
        let Some(request) = self.select(router, stream, head, boundary, limits) else {
            return;
        };
        self.checked(router, stream, request, limits);
    }
}

impl<'i, 'a, 'd: 'a, R, U>
    Begin<request::Start<U, <R::Routes as state::Family<R::Prepared>>::State<state::Routed>>>
    for Head<'i, 'a, 'd, R>
where
    R: decode::Guarded,
    R::Head: request::Head<
        Start = request::Start<U, <R::Routes as state::Family<R::Prepared>>::State<state::Routed>>,
    >,
    R::Prepared: request::Synchronous,
    U: state::Admission<
            R::Prepared,
            state::Routed,
            Accepted<'d>,
            Output = <<R::Prepared as request::Prepared>::Family as state::Family<R::Prepared>>::State<Accepted<'d>>,
        >,
{
    type Router = R;

    fn begin(
        &mut self,
        router: &R,
        stream: stream::Key,
        head: Retained<R::Plan>,
        boundary: Boundary,
        limits: server::Config,
    ) {
        let Some(request) = self.select(router, stream, head, boundary, limits)
        else {
            return;
        };
        match request {
            request::Start::Plain(request) => self.plain(stream, request, limits),
            request::Start::Guarded(request) => self.checked(router, stream, request, limits),
        }
    }
}
