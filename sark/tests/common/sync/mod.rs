use std::{cell::Cell, ops::ControlFlow};

use dope::core::driver::settings;
use o3::{
    buffer::{resident, storage},
    mem::rc,
};
use resident::bound;
use sark::{Guard, Head};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    ClientRole,
    conn::{self, DataOutcome, config, machine},
    server::{
        self,
        sync::{self},
    },
};
use sark_http::{
    field::{self, Value},
    method::Method,
    payload::{
        fixed,
        response::{dispatch, headers::Entry, statics, status::Code},
    },
};

pub const LIMITS: server::Config = server::Config {
    max_request_body_bytes: 1 << 20,
    max_request_body_chunks: 4 << 10,
    max_connection_body_bytes: 4 << 20,
    max_connection_body_chunks: 16 << 10,
    max_outbound_bytes: 1 << 20,
};

#[derive(Debug)]
pub enum Build {
    Plan(server::ConfigError),
    Endpoint(config::Error),
    Protocol(conn::Error),
    Field(field::Error),
    Storage(storage::AllocError),
    Send {
        actual: DataOutcome,
        expected: usize,
    },
}

pub fn try_build<R>(router: R, limits: server::Config) -> Result<sync::Endpoint<R>, Build> {
    let receive = settings::Receive::fixed::<2, { 64 << 10 }>();
    let plan = server::Plan::new(limits, receive).map_err(Build::Plan)?;
    sync::Endpoint::try_new(router, plan).map_err(Build::Endpoint)
}

pub struct State {
    pub request_body: Cell<usize>,
    pub request_head: Cell<usize>,
    pub dispatches: Cell<usize>,
    pub large_body: storage::Shared,
    pub invalid_path: Cell<bool>,
    pub header_error: Cell<Option<field::Error>>,
}

#[sark::request]
pub struct ItemRequest {
    #[path]
    pub id: storage::Shared,
}

#[sark::request]
pub struct NamedPath {
    /// Opposite to route position; generated captures must bind by name.
    #[path]
    pub team_id: storage::Shared,
    #[path]
    pub user_id: storage::Shared,
}

pub const NAMED_PATH_FRAGMENTS: [&[u8]; 4] = [
    b"POST /users/be",
    b"ta/teams/alpha HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chu",
    b"nked\r\n\r\n1\r\nx\r\n",
    b"0\r\n\r\n",
];

#[sark::handler(pub)]
pub(crate) fn item(request: ItemRequest, state: &State) -> EchoResponse<'req> {
    state.request_head.set(request.id.as_ptr().addr());
    EchoResponse {
        status: Code::OK,
        body: request.id,
    }
}

#[sark::handler(pub)]
pub(crate) fn named_path(request: NamedPath, state: &State) -> EchoResponse<'req> {
    state
        .invalid_path
        .set(state.invalid_path.get() || request.team_id != b"alpha" || request.user_id != b"beta");
    state.request_head.set(request.user_id.as_ptr().addr());
    EchoResponse {
        status: Code::OK,
        body: request.user_id,
    }
}

#[sark::request]
pub struct EchoRequest {
    #[raw_body]
    pub payload: storage::Shared,
}

#[sark::response(raw)]
pub struct EchoResponse<'req> {
    pub status: Code,
    pub body: &'req [u8],
}

#[sark::handler(pub)]
pub(crate) fn echo(request: EchoRequest, state: &State) -> EchoResponse<'req> {
    state.request_body.set(request.payload.as_ptr().addr());
    EchoResponse {
        status: Code::OK,
        body: request.payload,
    }
}

#[sark::response(raw)]
pub struct Headers<'req, 'd> {
    pub status: Code,
    pub body: fixed::Body<'d>,
    #[header("x-large")]
    pub value: Value<&'req [u8]>,
}

#[sark::handler(pub)]
pub(crate) fn large_head(
    request: EchoRequest,
    state: &State,
) -> Result<Headers<'req, 'd>, Fixed<'d>> {
    state.dispatches.set(state.dispatches.get() + 1);
    Ok(Headers {
        status: Code::OK,
        body: fixed::Body::new(b""),
        value: value(request.payload, &state.header_error)?,
    })
}

#[sark::response(raw)]
pub struct Large {
    pub status: Code,
    pub body: storage::Shared,
}

#[sark::handler(pub)]
pub(crate) fn large(
    state: &State,
) -> dispatch::Either<Large, statics::Direct<'d, [Entry<'req>; 0]>> {
    state.dispatches.set(state.dispatches.get() + 1);
    match state.large_body.try_clone() {
        Ok(body) => dispatch::Either::Left(Large {
            status: Code::OK,
            body,
        }),
        Err(rc::Error::Exhausted) => dispatch::Either::Right(statics::Direct::new(
            Code::SERVICE_UNAVAILABLE,
            [],
            fixed::Body::new(b""),
        )),
    }
}

sark::define_router! {
    pub Router: State => {
        POST "/echo" => echo,
        POST "/headers" => large_head,
        GET "/large" => large,
        GET "/items/:id" => request(path) item,
        GET "/users/:user_id/teams/:team_id" => request(path) named_path,
        POST "/users/:user_id/teams/:team_id" => request(path) named_path,
    }
}

#[sark::response(raw)]
pub struct Fixed<'d> {
    pub status: Code,
    pub body: fixed::Body<'d>,
}

#[sark::handler(pub)]
pub(crate) fn static_only() -> Fixed<'d> {
    Fixed {
        status: Code::OK,
        body: fixed::Body::new(b"static-only"),
    }
}

sark::define_router! {
    pub StaticRouter => {
        GET "/static" => static_only,
    }
}

pub struct DeniedState {
    pub guards: Cell<usize>,
    pub dispatches: Cell<usize>,
    pub invalid_guard: Cell<bool>,
    pub header_error: Cell<Option<field::Error>>,
}

pub struct Deny;

impl Guard<DeniedState> for Deny {
    type Egress<'d> = bound::AtMost<0>;

    type Break<'req, 'd>
        = Fixed<'d>
    where
        'd: 'req;

    fn check<'req, 'd>(
        head: Head<'req>,
        state: &'req DeniedState,
    ) -> ControlFlow<Self::Break<'req, 'd>>
    where
        'd: 'req,
    {
        state.invalid_guard.set(
            state.invalid_guard.get()
                || head.method() != &Method::POST
                || head.path() != Some(&b"/guarded"[..]),
        );
        state.guards.set(state.guards.get() + 1);
        ControlFlow::Break(Fixed {
            status: Code::UNAUTHORIZED,
            body: fixed::Body::new(b"denied"),
        })
    }
}

#[sark::response(raw)]
pub struct Occupied<'req, 'd> {
    pub status: Code,
    pub body: fixed::Body<'d>,
    #[header("x-occupy")]
    pub value: Value<&'req [u8]>,
}

#[sark::handler(pub)]
pub(crate) fn occupy(
    request: EchoRequest,
    state: &DeniedState,
) -> Result<Occupied<'req, 'd>, Fixed<'d>> {
    state.dispatches.set(state.dispatches.get() + 1);
    Ok(Occupied {
        status: Code::OK,
        body: fixed::Body::new(b""),
        value: value(request.payload, &state.header_error)?,
    })
}

fn value<'req, 'd>(
    bytes: &'req [u8],
    failure: &Cell<Option<field::Error>>,
) -> Result<Value<&'req [u8]>, Fixed<'d>> {
    Value::new(bytes).map_err(|error| {
        if failure.get().is_none() {
            failure.set(Some(error));
        }
        Fixed {
            status: Code::BAD_REQUEST,
            body: fixed::Body::new(b"invalid header"),
        }
    })
}

#[sark::handler(pub)]
pub(crate) fn guarded(state: &DeniedState) -> Fixed<'d> {
    state.dispatches.set(state.dispatches.get() + 1);
    Fixed {
        status: Code::OK,
        body: fixed::Body::new(b"unreachable"),
    }
}

sark::define_router! {
    pub GuardRouter: DeniedState => {
        POST "/occupy" => occupy,
        scope "" guard (Deny,) => [
            POST "/guarded" => guarded,
        ],
    }
}

pub struct Client<'a> {
    connection: &'a mut machine::Connection<ClientRole>,
}

impl<'a> Client<'a> {
    /// Admits fixture bytes once; literal callers evaluate this at compile time.
    pub const fn field<'f>(
        name: &'f [u8],
        value: &'f [u8],
    ) -> Result<field::Entry<'f>, field::Error> {
        let value = match field::Value::try_bytes(value) {
            Ok(value) => value,
            Err(error) => return Err(error),
        };
        let pseudo = match name {
            b":method" => field::Pseudo::Method,
            b":scheme" => field::Pseudo::Scheme,
            b":authority" => field::Pseudo::Authority,
            b":path" => field::Pseudo::Path,
            b":protocol" => field::Pseudo::Protocol,
            b":status" => field::Pseudo::Status,
            _ => {
                let name = match field::Lower::try_bytes(name) {
                    Ok(name) => name,
                    Err(error) => return Err(error),
                };
                return Ok(field::Entry::new(name, value));
            }
        };
        Ok(field::Entry::pseudo(pseudo, value))
    }

    pub fn new(conn: &'a mut machine::Connection<ClientRole>) -> Self {
        Self { connection: conn }
    }

    pub fn try_get(self, path: &[u8]) -> Result<storage::Shared, Build> {
        self.connection
            .streams()
            .headers()
            .request(
                [
                    field::literal!(Self::field(b":method", b"GET")),
                    field::literal!(Self::field(b":scheme", b"http")),
                    Self::field(b":path", path).map_err(Build::Field)?,
                    field::literal!(Self::field(b":authority", b"localhost")),
                ],
                Boundary::End,
            )
            .map_err(Build::Protocol)?;
        self.try_take()
    }

    pub fn try_post(self, path: &[u8], body: &[u8]) -> Result<storage::Shared, Build> {
        let stream = self
            .connection
            .streams()
            .headers()
            .request(
                [
                    field::literal!(Self::field(b":method", b"POST")),
                    field::literal!(Self::field(b":scheme", b"http")),
                    Self::field(b":path", path).map_err(Build::Field)?,
                    field::literal!(Self::field(b":authority", b"localhost")),
                    Self::field(b"content-length", body.len().to_string().as_bytes())
                        .map_err(Build::Field)?,
                ],
                Boundary::More,
            )
            .map_err(Build::Protocol)?;
        let actual = self
            .connection
            .streams()
            .try_send(stream, body, Boundary::End)
            .map_err(Build::Protocol)?;
        if !matches!(actual, DataOutcome::Sent(sent) if sent.get() == body.len()) {
            return Err(Build::Send {
                actual,
                expected: body.len(),
            });
        }
        self.try_take()
    }

    pub fn try_take(self) -> Result<storage::Shared, Build> {
        let wire =
            storage::Shared::try_copy(self.connection.output().first()).map_err(Build::Storage)?;
        self.connection.transfer().drain(usize::MAX);
        Ok(wire)
    }
}

pub mod wire;

pub mod pending;
