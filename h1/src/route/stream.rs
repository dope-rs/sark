//! Streaming route execution over borrowed HTTP/1 protocol storage.
use std::convert::Infallible;

use sark_core::{body, body::credit, decode::Call, manifold, route};
use sark_http::{
    h1::codec::canned,
    payload::response::{Method, dispatch},
};

use crate::{
    route::Pending,
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
        let Some((admit, stream)) = streamed else {
            return output::Complete::Inline.into_consume(
                permit,
                session::Consumption::Buffered(session::Buffered::new(used)),
                close,
            );
        };
        permit.commit();
        admit(stream, runtime, response);
        session::Outcome::Streamed {
            consumed: used,
            close,
        }
    }
}

impl<'d, R: route::Spec> Pending<'d, R> {
    pub fn stream<S, Runtime, Source, Admit, E, D>(
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
            response,
            content_length,
            body: mut owner,
        } = self;
        let mut body = match Self::admit(&mut owner, done.body_len, content_length, &mut proof) {
            Ok(body) => body,
            Err(error) => return error.into(),
        };
        exec.finish::<R, Admit, D>(
            head.borrow(),
            &mut body,
            response,
            done.consumed,
            response.connection(),
            permit,
        )
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
                return Pending::start(wait, self.connection.ingress.payload.take(), permit, proof);
            }
            Err(reason) => {
                return reason.into_consume().into();
            }
        };
        let (request, mut body, total, close, response) = match frame.into_parts() {
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
