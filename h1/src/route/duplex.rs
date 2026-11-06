use std::convert::Infallible;

use duplex::machine;
use o3::buffer::{segments::assembly, storage};
use sark_core::{
    body,
    body::{admit::Length, credit},
    decode::Call,
    duplex, route,
};
use sark_http::{
    h1::{
        codec::canned,
        framing::RequestBody,
        head::{line::Request, request::Fields},
        switch::Kind,
    },
    payload::{fixed, owned},
    request::span::Span,
};
pub use sark_ws::server::Upgrade as WebSocket;
use sark_ws::{server as ws, upgrade};
use ws::app::Sink as _;

use crate::{
    route::pending,
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

/// Request owners retained while a CONNECT body is arriving.
pub struct Pending<'d, R: route::Spec> {
    head: framing::Parsed<storage::Shared, R>,
    response: response::Context,
    content_length: Length,
    body: pending::Body<'d>,
}

impl<'d, R: route::Spec> Pending<'d, R> {
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

    pub fn finish<'env, S, D, Wrap, E>(
        self,
        done: session::Complete,
        permit: session::Permit,
        finish: (&'env S, Wrap),
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
        let (state, wrap) = finish;
        let Self {
            head,
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
        let body = match owner.try_admit::<R, E>(total, want, &mut proof) {
            Ok(body) => body,
            Err(error) => return session::Outcome::Failed(Failure::reject(error)),
        };
        construct::<R, S, D, Wrap>(head.borrow(), body, state, response, wrap, used, permit)
    }
}

/// CONNECT admission endpoint.
pub struct Endpoint<
    'a,
    'env,
    'd,
    S,
    Runtime,
    Stored: 'a,
    D: duplex::Session<'d>,
    Ready = Infallible,
    Heads = Infallible,
> where
    'd: 'a,
    Runtime: 'a,
    D: 'a,
{
    state: &'env S,
    connection: &'a mut session::Conn<
        'd,
        Runtime,
        Stored,
        switch::Enabled<machine::Machine<'d, D>>,
        Ready,
        Heads,
    >,
}

impl<'a, 'env, 'd, S, Runtime, Stored: 'a, D: duplex::Session<'d>, Ready, Heads>
    Endpoint<'a, 'env, 'd, S, Runtime, Stored, D, Ready, Heads>
where
    'd: 'a,
    Runtime: 'a,
    D: 'a,
{
    pub fn new(
        state: &'env S,
        conn: &'a mut session::Conn<
            'd,
            Runtime,
            Stored,
            switch::Enabled<machine::Machine<'d, D>>,
            Ready,
            Heads,
        >,
    ) -> Self {
        Self {
            state,
            connection: conn,
        }
    }

    pub fn dispatch<R, Wrap, E>(
        self,
        permit: session::Permit,
        found: framing::Head<'_, '_, R>,
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
        if matches!(found.connection(), head::Connection::Close) {
            return session::Outcome::Failed(Failure::BadRequest).into();
        }
        let frame = match framing::Frame::<R>::drive(found, &mut self.connection.ingress, || {
            credit::Evidence::head(&mut proof)
        }) {
            Ok(framing::Progress::Complete(frame)) => frame,
            Ok(framing::Progress::Pending(wait)) => {
                let mut route = Pending {
                    head: wait.head,
                    response: wait.response,
                    content_length: wait.declared_body_len,
                    body: pending::Body::new(),
                };
                if let Err(error) = route.push_body(self.connection.ingress.payload.take(), proof) {
                    return session::Outcome::Failed(Failure::Assembly(error)).into();
                }
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
        let (request, body, total, _, framing::Plain, response) = match frame.into_parts() {
            Ok(parts) => parts,
            Err(error) => return error.into_consume().into(),
        };
        construct::<R, S, D, Wrap>(request, body, self.state, response, wrap, total, permit).into()
    }

    pub fn upgrade<R, Wrap, E>(
        self,
        permit: session::Permit,
        found: framing::Head<'_, '_, R, framing::Plain, Upgrade>,
        write: &mut [u8],
        wrap: Wrap,
        mut proof: E,
    ) -> session::RouteOutcome<'d, Infallible, machine::Machine<'d, D>>
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
        let frame = match framing::Frame::<R>::drive(found, &mut self.connection.ingress, || {
            credit::Evidence::head(&mut proof)
        }) {
            Ok(framing::Progress::Complete(frame)) => frame,
            Ok(framing::Progress::Pending(_)) => {
                return session::Outcome::Failed(Failure::BadRequest).into();
            }
            Err(reason) => return reason.into_consume().into(),
        };
        let (request, body, used, _, framing::Plain, _) = match frame.into_parts() {
            Ok(parts) => parts,
            Err(error) => return error.into_consume().into(),
        };
        let authority = request.owner().authority();
        let raw = match open::<R, S>(request, body, self.state) {
            Ok(raw) => raw,
            Err(error) => return session::Outcome::Failed(error).into(),
        };
        let written = match ws::Response::upgrade_into(write, &accept) {
            Ok(written) => written,
            Err(error) => {
                raw.handler().failed(ws::Error::Capacity(error));
                return session::Outcome::Failed(Failure::Output(
                    response::Error::ValueCapacity(error).into(),
                ))
                .into();
            }
        };
        let (Some(authority), Ok(target)) = (authority, std::str::from_utf8(target)) else {
            return session::Outcome::Failed(Failure::BadRequest).into();
        };
        let Ok(authority) = std::str::from_utf8(authority) else {
            return session::Outcome::Failed(Failure::BadRequest).into();
        };
        let field = |span: Span| span.get(input).unwrap_or_else(|| unreachable!("opening"));
        if let Err(error) = raw.handler().opening(ws::Opening {
            authority,
            target,
            origin: origin.map(field),
            cookie: cookie.map(field),
        }) {
            raw.handler().failed(error);
            return session::Outcome::Close(fixed::Body::new(ws::Response::FORBIDDEN)).into();
        }
        permit
            .switch(
                switch::Transition::new(Kind::Upgrade, machine::Machine::new(wrap(raw))),
                session::Buffered::new(used),
                output::Switch::Inline { written },
            )
            .into()
    }
}

fn construct<'head, 'body, 'env, 'd, R, S, D, Wrap>(
    request: framing::Parsed<&'head [u8], R>,
    body: body::Body<View<'body, 'd>>,
    state: &'env S,
    response: response::Context,
    wrap: Wrap,
    used: usize,
    permit: session::Permit,
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
    permit.switch(
        switch::Transition::new(Kind::Connect, raw),
        session::Buffered::new(used),
        output::Switch::Static(fixed::Body::new(response.version().connect())),
    )
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
            let call = Call::new(request, view);
            call.try_open::<R, S>(state)
                .map_err(|error| Failure::Route(<R::Request as route::Project>::reject(error)))
        },
    ) {
        Ok(result) => result,
        Err(rejected) => Err(Failure::Projection(rejected.discard())),
    }
}
