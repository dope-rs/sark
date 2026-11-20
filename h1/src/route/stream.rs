//! Streaming route execution over borrowed HTTP/1 protocol storage.
use o3::buffer::write::Slice;

use sark_core::{body, body::credit, decode::Call, manifold, route};
use sark_http::{
    h1::codec::canned,
    payload::response::{Method, dispatch},
};

use crate::{
    route::Pending,
    session, switch,
    wire::{Failure, body::View, output, parsing::framing, response},
};

fn finish<'req, 'd: 'req, R, S, Runtime, Source, Admit, D>(
    request: framing::Parsed<&'req [u8], R>,
    body: &mut body::Body<View<'req, 'd>>,
    state: &S,
    runtime: &mut Runtime,
    tasks: Source,
    write: Slice<'_, &mut usize>,
    date: &[u8; 29],
    response: response::Context,
    used: usize,
    permit: session::Permit,
) -> session::Outcome<'d, D>
where
    R: route::Spec<Kind = dispatch::Polled> + manifold::Route<S> + crate::route::PolicyOf,
    Source: FnOnce() -> Option<Admit>,
    Admit: FnOnce(R::Stream<'d>, &mut Runtime, response::Context),
{
    let admit = match (response.method(), tasks) {
        (Method::Head, _) => Some(output::Admit::Head),
        (Method::Other | Method::Connect, tasks) => tasks().map(output::Admit::Body),
    };
    let Some(admit) = admit else {
        return session::Outcome::Close(canned::SERVICE_UNAVAILABLE);
    };
    let streamed = match <<R::Request as route::Schema>::Body as body::Project>::try_project(
        body,
        (
            request,
            state,
            output::Egress::new(write, date, response),
            admit,
        ),
        |scope| -> Result<_, Failure> {
            let ((request, state, egress, admit), view) = scope.into_parts();
            let call = Call::new(request, view);
            let reply = call
                .try_invoke::<R, S>(state)
                .map_err(|error| Failure::Route(<R::Request as route::Project>::reject(error)))?;
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
            response.connection(),
        );
    };
    permit.commit();
    admit(stream, runtime, response);
    session::Outcome::Streamed {
        consumed: used,
        close: response.connection(),
    }
}

impl<'d, R: route::Spec> Pending<'d, R> {
    pub fn stream<S, Runtime, Source, Admit, E, D>(
        self,
        done: session::Complete,
        permit: session::Permit,
        state: &S,
        write: Slice<'_, &mut usize>,
        date: &[u8; 29],
        runtime: &mut Runtime,
        tasks: Source,
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
        finish::<R, _, _, _, _, D>(
            head.borrow(),
            &mut body,
            state,
            runtime,
            tasks,
            write,
            date,
            response,
            done.consumed,
            permit,
        )
    }
}

impl<'d, Runtime, Stored, Transitions: switch::Engine, Ready, Heads>
    session::Conn<'d, Runtime, Stored, Transitions, Ready, Heads>
{
    pub fn dispatch_stream<R, S, Source, Admit, E>(
        &mut self,
        permit: session::Permit,
        found: framing::Head<'_, '_, R>,
        state: &S,
        write: Slice<'_, &mut usize>,
        date: &[u8; 29],
        source: Source,
        mut proof: E,
    ) -> session::RouteOutcome<'d, Pending<'d, R>, Transitions::Raw>
    where
        R: route::Spec<Kind = dispatch::Polled> + manifold::Route<S> + crate::route::PolicyOf,
        Source: FnOnce() -> Option<Admit>,
        Admit: FnOnce(R::Stream<'d>, &mut Runtime, response::Context),
        E: credit::Evidence,
    {
        let frame = match framing::Frame::drive(found, &mut self.ingress, &mut proof) {
            Ok(framing::Progress::Complete(frame)) => frame,
            Ok(framing::Progress::Pending(wait)) => {
                return Pending::start(wait, self.ingress.payload.take(), permit, proof);
            }
            Err(reason) => {
                return reason.into_consume().into();
            }
        };
        let (request, mut body, total, _, response) = match frame.into_parts() {
            Ok(parts) => parts,
            Err(error) => return error.into_consume().into(),
        };
        finish::<R, _, _, _, _, Transitions::Raw>(
            request,
            &mut body,
            state,
            &mut self.async_state,
            source,
            write,
            date,
            response,
            total,
            permit,
        )
        .into()
    }
}
