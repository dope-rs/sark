//! Streaming route execution over borrowed HTTP/1 protocol storage.
use std::convert::Infallible;

use o3::buffer::{segments::assembly, storage};
use sark_core::{
    body,
    body::{admit::Length, credit},
    decode::Call,
    manifold, route,
};
use sark_http::{
    h1::codec::canned,
    payload::{
        owned,
        response::{Method, dispatch},
    },
};

use crate::{
    route::pending,
    session, switch,
    wire::{Failure, body::View, head, output, parsing::framing, response},
};

/// One terminal continuation turn for a streaming route.
pub struct Execution<'turn, 'env, S, Runtime, Tasks> {
    egress: output::Egress<'turn>,
    state: &'env S,
    runtime: &'turn mut Runtime,
    tasks: Tasks,
}

impl<'turn, 'env, S, Runtime, Tasks> Execution<'turn, 'env, S, Runtime, Tasks> {
    pub fn new(
        egress: output::Egress<'turn>,
        state: &'env S,
        rt: &'turn mut Runtime,
        tasks: Tasks,
    ) -> Self {
        Self {
            egress,
            state,
            runtime: rt,
            tasks,
        }
    }

    fn finish<'req, 'd: 'req, R, Admit, D>(
        self,
        request: framing::Parsed<&'req [u8], R>,
        body: &mut body::Body<View<'req, 'd>>,
        response: response::Context,
        used: usize,
        close: head::Connection,
        permit: session::Permit,
    ) -> session::Outcome<'d, D>
    where
        R: route::Spec<Kind = dispatch::Polled> + manifold::Route<S> + crate::route::PolicyOf,
        Tasks: FnOnce() -> Option<Admit>,
        Admit: FnOnce(R::Stream<'d>, &mut Runtime, response::Context),
    {
        let Self {
            egress,
            state,
            runtime,
            tasks,
        } = self;
        let admit = match (response.method(), tasks) {
            (Method::Head, _) => Some(output::Admit::Head),
            (Method::Other | Method::Connect, tasks) => tasks().map(output::Admit::Body),
        };
        let Some(admit) = admit else {
            return session::Outcome::Close(canned::SERVICE_UNAVAILABLE);
        };
        let streamed = match <<R::Request as route::Schema>::Body as body::Project>::try_project(
            body,
            (request, state, egress.with_context(response), admit),
            |scope| -> Result<_, Failure> {
                let ((request, state, egress, admit), view) = scope.into_parts();
                let call = Call::new(request, view);
                let reply = call.try_invoke::<R, S>(state).map_err(|error| {
                    Failure::Route(<R::Request as route::Project>::reject(error))
                })?;
                egress.stream(reply, R::POLICY, admit)
            },
        ) {
            Ok(Ok(streamed)) => streamed,
            Ok(Err(error)) => return session::Outcome::Failed(error),
            Err(failure) => {
                return session::Outcome::Failed(Failure::Projection(failure.discard()));
            }
        };
        let (wrote, stream) = streamed;
        let Some((admit, stream)) = stream else {
            return output::Outcome::Send {
                written: wrote,
                connection: close,
            }
            .into_consume(
                permit,
                session::Consumption::Buffered(session::Buffered::new(used)),
                close,
            );
        };
        permit.commit();
        admit(stream, runtime, response);
        session::Outcome::Streamed {
            consumed: used,
            written: wrote,
            close,
        }
    }
}

/// Owned continuation for a streamed request body.
pub struct Pending<'d, R: route::Spec> {
    head: framing::Parsed<storage::Shared, R>,
    connection: head::Connection,
    response: response::Context,
    content_length: Length,
    body: pending::Body<'d>,
}

impl<'d, R: route::Spec> Pending<'d, R> {
    pub const fn response(&self) -> response::Context {
        self.response
    }

    fn new(
        head: framing::Parsed<storage::Shared, R>,
        close: head::Connection,
        response: response::Context,
        want: Length,
    ) -> Self {
        Self {
            head,
            connection: close,
            response,
            content_length: want,
            body: pending::Body::new(),
        }
    }

    /// Framing admission bounded this immutable head by HEAD_CAPACITY.
    pub fn head_len(&self) -> u32 {
        self.head.owner().as_ref().len() as u32
    }

    pub fn push_body<E: credit::Evidence>(
        &mut self,
        parts: owned::Segments<'d>,
        proof: E,
    ) -> Result<(), assembly::Error> {
        self.body.push(parts, proof)
    }

    pub fn cancel<E: credit::Evidence>(self, proof: E) {
        self.body.discard(proof);
    }

    pub fn finish<S, Runtime, Source, Admit, E, D>(
        self,
        done: session::Complete,
        permit: session::Permit,
        exec: Execution<'_, '_, S, Runtime, Source>,
        mut proof: E,
    ) -> session::Outcome<'d, D>
    where
        R: route::Spec<Kind = dispatch::Polled> + manifold::Route<S> + crate::route::PolicyOf,
        Source: FnOnce() -> Option<Admit>,
        Admit: FnOnce(R::Stream<'d>, &mut Runtime, response::Context),
        E: credit::Evidence,
    {
        let Self {
            head,
            connection: close,
            response,
            content_length: want,
            body,
        } = self;
        let session::Complete {
            body_len: total,
            consumed: used,
        } = done;
        if !body.matches(<R::Request as route::Schema>::BODY_RETENTION, total) {
            body.discard(proof);
            return session::Outcome::Close(canned::BAD_REQUEST);
        }
        let mut owner = body;
        let mut body = match owner.try_admit::<R, E>(total, want, &mut proof) {
            Ok(body) => body,
            Err(error) => return session::Outcome::Failed(Failure::reject(error)),
        };
        exec.finish::<R, Admit, D>(head.borrow(), &mut body, response, used, close, permit)
    }
}

pub struct Endpoint<
    'a,
    'd,
    Runtime,
    Continuations,
    Transitions = switch::Disabled,
    Ready = Infallible,
    Heads = Infallible,
> {
    egress: output::Egress<'a>,
    connection: &'a mut session::Conn<'d, Runtime, Continuations, Transitions, Ready, Heads>,
}

impl<'a, 'd, Runtime, Continuations, Transitions, Ready, Heads>
    Endpoint<'a, 'd, Runtime, Continuations, Transitions, Ready, Heads>
where
    Transitions: switch::Engine,
{
    pub fn new(
        egress: output::Egress<'a>,
        conn: &'a mut session::Conn<'d, Runtime, Continuations, Transitions, Ready, Heads>,
    ) -> Self {
        Self {
            egress,
            connection: conn,
        }
    }

    pub fn dispatch<R, S, Source, Admit, E>(
        self,
        permit: session::Permit,
        found: framing::Head<'_, '_, R>,
        state: &S,
        source: Source,
        mut proof: E,
    ) -> session::RouteOutcome<'d, Pending<'d, R>, Transitions::Raw>
    where
        R: route::Spec<Kind = dispatch::Polled> + manifold::Route<S> + crate::route::PolicyOf,
        Source: FnOnce() -> Option<Admit>,
        Admit: FnOnce(R::Stream<'d>, &mut Runtime, response::Context),
        E: credit::Evidence,
    {
        let frame = match framing::Frame::drive(found, &mut self.connection.ingress, &mut proof) {
            Ok(framing::Progress::Complete(frame)) => frame,
            Ok(framing::Progress::Pending(wait)) => {
                let mut route = Pending::new(
                    wait.head,
                    wait.connection,
                    wait.response,
                    wait.declared_body_len,
                );
                if let Err(error) = route.push_body(self.connection.ingress.payload.take(), proof) {
                    return session::Outcome::Failed(Failure::Assembly(error)).into();
                }
                permit.commit();
                return session::RouteOutcome::AwaitBody {
                    state: wait.state,
                    expectation: wait.expectation,
                    pending: route,
                };
            }
            Err(reason) => {
                return reason.into_consume().into();
            }
        };
        let (request, mut body, total, close, framing::Plain, response) = match frame.into_parts() {
            Ok(parts) => parts,
            Err(error) => return error.into_consume().into(),
        };
        Execution::new(self.egress, state, &mut self.connection.async_state, source)
            .finish::<R, Admit, Transitions::Raw>(
                request, &mut body, response, total, close, permit,
            )
            .into()
    }
}
