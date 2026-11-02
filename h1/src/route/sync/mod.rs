//! Synchronous HTTP/1 route execution over borrowed protocol storage.
use core::{convert::Infallible, mem};

use sark_core::{
    body,
    body::{admit::Retention, credit},
    decode::Call,
    manifold, route,
};
use sark_http::{
    h1::head::request::Coding,
    owner::handoff,
    payload::{compress, response::dispatch},
};

use crate::{
    route::PolicyOf,
    session, switch,
    wire::{Failure, body::View, cache, output, parsing::framing},
};

mod pending;

pub use pending::Pending;

/// Borrowed resources required to produce one synchronous HTTP/1 response.
/// The aggregate owns no allocation and keeps every output borrow live for the
/// complete route execution.
pub struct Egress<'turn, 'cache, C: compress::Compression = compress::Gzip> {
    output: output::Egress<'turn>,
    cache: cache::Cache<'cache>,
    gzip: &'turn mut C,
}

impl<'turn, 'cache, C: compress::Compression> Egress<'turn, 'cache, C> {
    pub fn new(
        output: output::Egress<'turn>,
        cache: cache::Cache<'cache>,
        gzip: &'turn mut C,
    ) -> Self {
        Self {
            output,
            cache,
            gzip,
        }
    }

    fn invoke<'d, R, S>(
        self,
        request: framing::Parsed<&[u8], R>,
        body: &mut body::Body<View<'_, 'd>>,
        state: &S,
        coding: Coding,
    ) -> Result<output::Outcome<'d>, Failure>
    where
        R: route::Spec<Kind = dispatch::Sync> + manifold::Route<S> + PolicyOf,
    {
        <<R::Request as route::Schema>::Body as body::Project>::try_project(
            body,
            (request, state, self, coding),
            |scope| -> Result<output::Outcome<'d>, Failure> {
                let ((request, state, egress, coding), view) = scope.into_parts();
                let Egress {
                    output,
                    cache,
                    gzip,
                } = egress;
                let call = Call::new(request, view);
                let reply = call.try_invoke::<R, S>(state).map_err(|error| {
                    Failure::Route(<R::Request as route::Project>::reject(error))
                })?;
                Ok(output.route(
                    reply,
                    handoff::Slot::ready(None),
                    cache,
                    gzip,
                    coding,
                    R::POLICY,
                ))
            },
        )
        .map_err(|failure| Failure::Projection(failure.discard()))?
    }
}

pub struct Endpoint<
    'a,
    'd,
    'cache,
    Runtime,
    Stored,
    Transitions = switch::Disabled,
    Ready = Infallible,
    Heads = Infallible,
    C: compress::Compression = compress::Gzip,
> {
    egress: Egress<'a, 'cache, C>,
    connection: &'a mut session::Conn<'d, Runtime, Stored, Transitions, Ready, Heads>,
}

impl<'a, 'd, 'cache, Runtime, Stored, Transitions, Ready, Heads, C: compress::Compression>
    Endpoint<'a, 'd, 'cache, Runtime, Stored, Transitions, Ready, Heads, C>
where
    Transitions: switch::Engine,
{
    pub fn new(
        egress: Egress<'a, 'cache, C>,
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
        found: framing::Head<'_, '_, R, Coding>,
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

    fn pending<R: route::Spec, E: credit::Evidence>(
        &mut self,
        permit: session::Permit,
        frame: framing::Pending<R, Coding>,
        proof: E,
    ) -> session::RouteOutcome<'d, Pending<'d, R>, Transitions::Raw> {
        let state = frame.state;
        let expect = frame.expectation;
        let mut wait = Pending::from_frame(frame);
        if let Err(error) = wait.push_body(self.connection.ingress.payload.take(), proof) {
            return session::Outcome::Failed(Failure::Assembly(error)).into();
        }
        permit.commit();
        session::RouteOutcome::AwaitBody {
            state,
            expectation: expect,
            pending: wait,
        }
    }

    fn buffered<R, S, E: credit::Evidence>(
        mut self,
        permit: session::Permit,
        found: framing::Head<'_, '_, R, Coding>,
        state: &S,
        mut proof: E,
    ) -> session::RouteOutcome<'d, Pending<'d, R>, Transitions::Raw>
    where
        R: route::Spec<Kind = dispatch::Sync> + manifold::Route<S> + PolicyOf,
    {
        let frame =
            match framing::Frame::<R, Coding>::drive(found, &mut self.connection.ingress, || {
                credit::Evidence::head(&mut proof)
            }) {
                Ok(framing::Progress::Complete(frame)) => frame,
                Ok(framing::Progress::Pending(wait)) => {
                    return self.pending(permit, wait, proof);
                }
                Err(reason) => {
                    return reason.into_consume().into();
                }
            };
        let total = frame.total();
        let close = frame.connection;
        self.egress.output = self.egress.output.with_context(frame.response);
        if let Some(ready) = self
            .egress
            .output
            .cached(&mut self.egress.cache, R::POLICY.cache)
        {
            return ready
                .into_consume(
                    permit,
                    session::Consumption::Buffered(session::Buffered::new(total)),
                    close,
                )
                .into();
        }
        let (request, mut body, _, close, coding, _) = match frame.into_parts() {
            Ok(parts) => parts,
            Err(error) => return error.into_consume().into(),
        };
        let ready = match self
            .egress
            .invoke::<R, S>(request, &mut body, state, coding)
        {
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
        found: framing::Head<'_, '_, R, Coding>,
        state: &S,
        mut proof: E,
    ) -> session::RouteOutcome<'d, Pending<'d, R>, Transitions::Raw>
    where
        R: route::Spec<Kind = dispatch::Sync> + manifold::Route<S> + PolicyOf,
    {
        let frame = match framing::Discard::<R>::drive(found, self.connection, || {
            credit::Evidence::head(&mut proof)
        }) {
            Ok(framing::Discarded::Complete(frame)) => frame,
            Ok(framing::Discarded::Pending(wait)) => {
                return self.pending(permit, wait, proof);
            }
            Err(reason) => {
                return reason.into_consume().into();
            }
        };
        let (request, total, want, used, close, coding, response) = match frame.into_parts() {
            Ok(parts) => parts,
            Err(error) => return error.into_consume().into(),
        };
        self.egress.output = self.egress.output.with_context(response);
        if let Some(ready) = self
            .egress
            .output
            .cached(&mut self.egress.cache, R::POLICY.cache)
        {
            return ready.into_consume(permit, used, close).into();
        }
        let body = View::from_slice(&[]);
        let mut body = match body.finish::<R>(total, want) {
            Ok(body) => body,
            Err(error) => {
                return session::Outcome::Failed(Failure::reject(error)).into();
            }
        };
        let ready = match self
            .egress
            .invoke::<R, S>(request, &mut body, state, coding)
        {
            Ok(ready) => ready,
            Err(error) => return session::Outcome::Failed(error).into(),
        };
        ready.into_consume(permit, used, close).into()
    }
}

const _: () = assert!(mem::size_of::<Egress<'static, 'static>>() == 6 * mem::size_of::<usize>());
