use o3::buffer::write::Slice;

use duplex::machine;
use sark_core::{body, body::credit, duplex, route, route::Project as _};
use sark_http::{
    h1::{
        framing::RequestBody,
        head::{line::Request, request::Fields},
        switch::Kind,
    },
    payload::fixed,
    request::span::Span,
};
pub use sark_ws::server::Upgrade as WebSocket;
use sark_ws::{server as ws, upgrade};
use ws::app::Sink as _;

use crate::{
    route::Pending,
    session, switch,
    wire::{Failure, body::View, head, output, parsing::framing, response},
};

/// WebSocket admission attached to the existing route-specific field scan.
pub type Upgrade = upgrade::request::Admission;

impl head::Admission for Upgrade {
    type Admitted = ([u8; 28], Option<Span>, Option<Span>);
    const MAX_BYTES: usize = 16 * 1024;

    fn field(&mut self, name: &[u8], value: &[u8], span: Span) -> Result<(), Failure> {
        Self::field(self, name, value, span).map_err(Failure::Upgrade)
    }

    fn finish(
        self,
        request: &Request<'_>,
        fields: &Fields,
        body: RequestBody,
    ) -> Result<Self::Admitted, Failure> {
        Ok((
            self.finish(
                request.method(),
                request.target(),
                request.version() as u8,
                body,
                fields.routing.connection(),
                b"",
            )
            .map_err(Failure::Upgrade)?,
            self.origin(),
            self.cookie(),
        ))
    }
}

impl<'d, R: route::Spec> Pending<'d, R> {
    pub fn connect<'env, S, D, Wrap, E>(
        self,
        done: session::Complete,
        state: &'env S,
        wrap: Wrap,
        mut proof: E,
    ) -> session::Outcome<'d, machine::Machine<'d, D>>
    where
        R: duplex::Route<'env, S>,
        R::Session: duplex::Session<'d>,
        D: duplex::Session<'d>,
        Wrap: FnOnce(R::Session) -> D,
        E: credit::Evidence,
        'd: 'env,
    {
        let Self {
            head,
            response,
            content_length,
            body: mut owner,
        } = self;
        let body = match Self::admit(&mut owner, done.body_len, content_length, &mut proof) {
            Ok(body) => body,
            Err(error) => return error.into(),
        };
        construct::<R, S, D, Wrap>(head.borrow(), body, state, response, wrap, done.consumed)
    }
}

impl<'d, Runtime, Stored, D: duplex::Session<'d>, Ready, Heads>
    session::Conn<'d, Runtime, Stored, switch::Enabled<machine::Machine<'d, D>>, Ready, Heads>
{
    pub fn dispatch_connect<'env, R, S, Wrap, E>(
        &mut self,
        found: framing::Head<'_, '_, R>,
        state: &'env S,
        wrap: Wrap,
        mut proof: E,
    ) -> session::RouteOutcome<'d, Pending<'d, R>, machine::Machine<'d, D>>
    where
        R: duplex::Route<'env, S>,
        R::Session: duplex::Session<'d>,
        D: duplex::Session<'d>,
        Wrap: FnOnce(R::Session) -> D,
        E: credit::Evidence,
        'd: 'env,
    {
        if matches!(found.response().connection(), head::Connection::Close) {
            return session::Outcome::Failed(Failure::BadRequest).into();
        }
        let (request, body, total, response) = match found.request(&mut self.ingress, &mut proof) {
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
        construct::<R, S, D, Wrap>(request, body, state, response, wrap, total).into()
    }

    pub fn upgrade_websocket<'env, R, S, Wrap, E>(
        &mut self,
        found: framing::Head<'_, '_, R, Upgrade>,
        state: &'env S,
        write: Slice<'_, &mut usize>,
        wrap: Wrap,
        mut proof: E,
    ) -> session::Outcome<'d, machine::Machine<'d, D>>
    where
        R: duplex::Route<'env, S>,
        R::Session: WebSocket + duplex::Session<'d>,
        D: duplex::Session<'d>,
        Wrap: FnOnce(R::Session) -> D,
        E: credit::Evidence,
        'd: 'env,
    {
        let source = found.source();
        let input = source.input().as_slice();
        let target = source.request().target();
        let (found, (accept, origin, cookie)) = found.into_admission();
        let (request, body, used, _) = match found.request(&mut self.ingress, &mut proof) {
            Ok(framing::Progress::Complete(frame)) => frame,
            Ok(framing::Progress::Pending(_)) => {
                return session::Outcome::Failed(Failure::BadRequest);
            }
            Err(reason) => return reason.into_rejection().into(),
        };
        let authority = request.owner().authority();
        let raw = match open::<R, S>(request, body, state) {
            Ok(raw) => raw,
            Err(error) => return session::Outcome::Failed(error),
        };
        match ws::Response::upgrade_into(write, &accept) {
            Ok(_) => {}
            Err(error) => {
                raw.handler().failed(ws::Error::Capacity(error));
                return session::Outcome::Failed(Failure::Output(
                    response::Error::ValueCapacity(error).into(),
                ));
            }
        };
        let (Some(authority), Ok(target)) = (authority, std::str::from_utf8(target)) else {
            return session::Outcome::Failed(Failure::BadRequest);
        };
        let Ok(authority) = std::str::from_utf8(authority) else {
            return session::Outcome::Failed(Failure::BadRequest);
        };
        let field = |span: Span| span.get(input).unwrap_or_else(|| unreachable!("opening"));
        if let Err(error) = raw.handler().opening(ws::Opening {
            authority,
            target,
            origin: origin.map(field),
            cookie: cookie.map(field),
        }) {
            raw.handler().failed(error);
            return session::Outcome::Close(fixed::Body::new(ws::Response::FORBIDDEN));
        }
        session::Outcome::Switch {
            next: switch::Transition::new(Kind::Upgrade, machine::Machine::new(wrap(raw))),
            consumed: used,
            response: output::Switch::Inline,
        }
    }
}

fn construct<'head, 'body, 'env, 'd, R, S, D, Wrap>(
    request: framing::Parsed<&'head [u8], R>,
    body: body::Body<View<'body, 'd>>,
    state: &'env S,
    response: response::Context,
    wrap: Wrap,
    used: usize,
) -> session::Outcome<'d, machine::Machine<'d, D>>
where
    R: duplex::Route<'env, S>,
    R::Session: duplex::Session<'d>,
    D: duplex::Session<'d>,
    Wrap: FnOnce(R::Session) -> D,
    'd: 'env,
{
    let raw = match open::<R, S>(request, body, state) {
        Ok(open) => machine::Machine::new(wrap(open)),
        Err(error) => return session::Outcome::Failed(error),
    };
    session::Outcome::Switch {
        next: switch::Transition::new(Kind::Connect, raw),
        consumed: used,
        response: output::Switch::Static(fixed::Body::new(response.version().connect())),
    }
}

fn open<'head, 'body, 'env, 'd, R, S>(
    request: framing::Parsed<&'head [u8], R>,
    mut body: body::Body<View<'body, 'd>>,
    state: &'env S,
) -> Result<R::Session, Failure>
where
    R: duplex::Route<'env, S>,
    R::Session: duplex::Session<'d>,
    'd: 'env,
{
    match <<R::Request as route::Schema>::Body as body::Project>::try_project(
        &mut body,
        (request, state),
        |scope| -> Result<_, Failure> {
            let ((request, state), view) = scope.into_parts();
            let request =
                R::Request::try_view(&request, view).map_err(Failure::route::<R::Request>)?;
            Ok(R::open(request, state))
        },
    ) {
        Ok(result) => result,
        Err(rejected) => Err(Failure::Projection(rejected.discard())),
    }
}
