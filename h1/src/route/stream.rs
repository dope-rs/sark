//! Streaming route execution over borrowed HTTP/1 protocol storage.
use o3::buffer::write::Slice;

use sark_core::{body, body::credit, manifold, route, route::Project as _};
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
) -> session::Outcome<'d, D>
where
    R: route::Spec<Kind = dispatch::Polled> + manifold::Route<S> + crate::route::PolicyOf,
    Source: FnOnce() -> Option<Admit>,
    Admit: FnOnce(R::Stream<'d>, &mut Runtime, response::Context),
{
    let admit = match response.method() {
        Method::Head => None,
        Method::Other | Method::Connect => match tasks() {
            Some(admit) => Some(admit),
            None => return session::Outcome::Close(canned::SERVICE_UNAVAILABLE),
        },
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
            let request =
                R::Request::try_view(&request, view).map_err(Failure::route::<R::Request>)?;
            egress.stream(R::invoke(request, state), R::POLICY, admit)
        },
    ) {
        Ok(Ok(streamed)) => streamed,
        Ok(Err(error)) => return session::Outcome::Failed(error),
        Err(failure) => {
            return session::Outcome::Failed(Failure::Projection(failure.discard()));
        }
    };
    let Some((admit, stream)) = streamed else {
        return output::Complete::Inline
            .into_consume(session::Consumption::Buffered(used), response.connection());
    };
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
        )
    }
}

impl<'d, Runtime, Stored, Transitions: switch::Engine, Ready, Heads>
    session::Conn<'d, Runtime, Stored, Transitions, Ready, Heads>
{
    pub fn dispatch_stream<R, S, Source, Admit, E>(
        &mut self,
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
        let (request, mut body, total, response) =
            match found.request(&mut self.ingress, &mut proof) {
                Ok(framing::Progress::Complete(frame)) => frame,
                Ok(framing::Progress::Pending(wait)) => {
                    let (state, expectation, pending) = wait.retain(&mut self.ingress, proof);
                    return session::RouteOutcome::AwaitBody {
                        state,
                        expectation,
                        pending,
                    };
                }
                Err(reason) => {
                    return session::Outcome::from(reason.into_rejection()).into();
                }
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
        )
        .into()
    }
}
