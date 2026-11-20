//! Synchronous HTTP/1 route execution over borrowed protocol storage.
use core::{convert::Infallible, mem, ops::ControlFlow};

use sark_core::{
    body,
    body::{admit::Retention, credit},
    decode::Call,
    manifold, route,
};
use sark_http::{
    owner::handoff,
    payload::{
        compress,
        response::{Body as _, dispatch},
    },
};

use crate::{
    route::{Pending, PolicyOf},
    session, switch,
    wire::{Failure, body::View, output, parsing::framing},
};

/// Borrowed resources required to produce one synchronous HTTP/1 response.
/// The aggregate owns no allocation and keeps every output borrow live for the
/// complete route execution.
pub struct Egress<'turn, C: compress::Compression = compress::Gzip> {
    output: output::Egress<'turn>,
    gzip: &'turn mut C,
}

impl<'turn, C: compress::Compression> Egress<'turn, C> {
    pub fn new(output: output::Egress<'turn>, gzip: &'turn mut C) -> Self {
        Self { output, gzip }
    }

    fn invoke<'d, R, S>(
        self,
        request: framing::Parsed<&[u8], R>,
        body: &mut body::Body<View<'_, 'd>>,
        state: &S,
    ) -> Result<output::Complete<'d>, Failure>
    where
        R: route::Spec<Kind = dispatch::Sync> + manifold::Route<S> + PolicyOf,
    {
        <<R::Request as route::Schema>::Body as body::Project>::try_project(
            body,
            (request, state, self),
            |scope| -> Result<output::Complete<'d>, Failure> {
                let ((request, state, egress), view) = scope.into_parts();
                let Egress { output, gzip } = egress;
                let call = Call::new(request, view);
                let reply = call.try_invoke::<R, S>(state).map_err(|error| {
                    Failure::Route(<R::Request as route::Project>::reject(error))
                })?;
                output.try_route(reply, gzip, R::POLICY)
            },
        )
        .map_err(|failure| Failure::Projection(failure.discard()))?
    }
}

impl<'d, R: route::Spec> Pending<'d, R> {
    pub fn respond<S, E, D>(
        self,
        done: session::Complete,
        permit: session::Permit,
        state: &S,
        egress: Egress<'_, impl compress::Compression>,
        mut proof: E,
    ) -> session::Outcome<'d, D>
    where
        E: credit::Evidence,
        R: manifold::Route<S> + route::Spec<Kind = dispatch::Sync> + PolicyOf,
    {
        let Self {
            head,
            response,
            content_length,
            body: mut owner,
        } = self;
        let Egress { output, gzip } = egress;
        let output = output.with_context(response);
        let mut body = match Self::admit(&mut owner, done.body_len, content_length, &mut proof) {
            Ok(body) => body,
            Err(error) => return error.into(),
        };
        let ready = match <<R::Request as route::Schema>::Body as body::Project>::try_project(
            &mut body,
            (head.borrow(), state, output, gzip),
            |scope| {
                let ((request, state, output, gzip), view) = scope.into_parts();
                let input = view.body();
                let call = Call::new(request, view);
                let reply = call.try_invoke::<R, S>(state).map_err(|error| {
                    Failure::Route(<R::Request as route::Project>::reject(error))
                })?;
                output.try_stage(reply, handoff::Slot::new(input), gzip, R::POLICY)
            },
        ) {
            Ok(Ok(ready)) => ready,
            Ok(Err(error)) => return session::Outcome::Failed(error),
            Err(failure) => {
                return session::Outcome::Failed(Failure::Projection(failure.discard()));
            }
        };
        let ready = match ready {
            ControlFlow::Continue(ready) => {
                drop(owner);
                ready
            }
            ControlFlow::Break(declared) => {
                let body = owner;
                let actual = body.len();
                if actual != declared {
                    return session::Outcome::Failed(Failure::Admission(
                        body::Error::LengthMismatch { actual, declared },
                    ));
                }
                output::Complete::Linked { body }
            }
        };
        ready.into_consume(
            permit,
            session::Consumption::Buffered(session::Buffered::new(done.consumed)),
            response.connection(),
        )
    }
}

pub struct Endpoint<
    'a,
    'd,
    Runtime,
    Stored,
    Transitions = switch::Disabled,
    Ready = Infallible,
    Heads = Infallible,
    C: compress::Compression = compress::Gzip,
> {
    egress: Egress<'a, C>,
    connection: &'a mut session::Conn<'d, Runtime, Stored, Transitions, Ready, Heads>,
}

impl<'a, 'd, Runtime, Stored, Transitions, Ready, Heads, C: compress::Compression>
    Endpoint<'a, 'd, Runtime, Stored, Transitions, Ready, Heads, C>
where
    Transitions: switch::Engine,
{
    pub fn new(
        egress: Egress<'a, C>,
        conn: &'a mut session::Conn<'d, Runtime, Stored, Transitions, Ready, Heads>,
    ) -> Self {
        Self {
            egress,
            connection: conn,
        }
    }

    pub fn dispatch<R, S, E: credit::Evidence>(
        self,
        permit: session::Permit,
        found: framing::Head<'_, '_, R>,
        state: &S,
        proof: E,
    ) -> session::RouteOutcome<'d, Pending<'d, R>, Transitions::Raw>
    where
        R: route::Spec<Kind = dispatch::Sync> + manifold::Route<S> + PolicyOf,
    {
        match <R::Request as route::Schema>::BODY_RETENTION {
            Retention::Retained => self.buffered(permit, found, state, proof),
            Retention::Discarded => self.discard(permit, found, state, proof),
        }
    }

    fn buffered<R, S, E: credit::Evidence>(
        mut self,
        permit: session::Permit,
        found: framing::Head<'_, '_, R>,
        state: &S,
        mut proof: E,
    ) -> session::RouteOutcome<'d, Pending<'d, R>, Transitions::Raw>
    where
        R: route::Spec<Kind = dispatch::Sync> + manifold::Route<S> + PolicyOf,
    {
        let frame = match framing::Frame::drive(found, &mut self.connection.ingress, &mut proof) {
            Ok(framing::Progress::Complete(frame)) => frame,
            Ok(framing::Progress::Pending(wait)) => {
                return Pending::start(wait, self.connection.ingress.payload.take(), permit, proof);
            }
            Err(reason) => {
                return reason.into_consume().into();
            }
        };
        let total = frame.total();
        self.egress.output = self.egress.output.with_context(frame.response);
        let (request, mut body, _, close, _) = match frame.into_parts() {
            Ok(parts) => parts,
            Err(error) => return error.into_consume().into(),
        };
        let ready = match self.egress.invoke::<R, S>(request, &mut body, state) {
            Ok(ready) => ready,
            Err(error) => return session::Outcome::Failed(error).into(),
        };
        ready
            .into_consume(
                permit,
                session::Consumption::Buffered(session::Buffered::new(total)),
                close,
            )
            .into()
    }

    fn discard<R, S, E: credit::Evidence>(
        mut self,
        permit: session::Permit,
        found: framing::Head<'_, '_, R>,
        state: &S,
        mut proof: E,
    ) -> session::RouteOutcome<'d, Pending<'d, R>, Transitions::Raw>
    where
        R: route::Spec<Kind = dispatch::Sync> + manifold::Route<S> + PolicyOf,
    {
        let frame = match framing::Discard::drive(found, &mut self.connection.ingress, &mut proof) {
            Ok(framing::Discarded::Complete(frame)) => frame,
            Ok(framing::Discarded::Pending(wait)) => {
                return Pending::start(wait, self.connection.ingress.payload.take(), permit, proof);
            }
            Err(reason) => {
                return reason.into_consume().into();
            }
        };
        let (request, total, want, used, close, response) = match frame.into_parts() {
            Ok(parts) => parts,
            Err(error) => return error.into_consume().into(),
        };
        self.egress.output = self.egress.output.with_context(response);
        let body = View::Slice(&[]);
        let mut body = match body.finish::<R>(total, want) {
            Ok(body) => body,
            Err(error) => {
                return session::Outcome::Failed(Failure::reject(error)).into();
            }
        };
        let ready = match self.egress.invoke::<R, S>(request, &mut body, state) {
            Ok(ready) => ready,
            Err(error) => return session::Outcome::Failed(error).into(),
        };
        ready.into_consume(permit, used, close).into()
    }
}

const _: () = assert!(mem::size_of::<Egress<'static>>() == 6 * mem::size_of::<usize>());
