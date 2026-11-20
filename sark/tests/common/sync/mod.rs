use std::{cell::Cell, ops::ControlFlow, pin::Pin};

use dope::core::driver::settings;
use o3::{
    base::cell::region,
    buffer::{raw::Slices, resident, storage},
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
    field::{self, Pseudo, Value},
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

pub const OUTBOUND_LIMIT: usize = 1;

pub fn backpressure_plan(body_bytes: usize) -> server::Plan {
    let limits = server::Config {
        max_request_body_bytes: body_bytes,
        max_connection_body_bytes: body_bytes,
        max_outbound_bytes: OUTBOUND_LIMIT,
        ..LIMITS
    };
    server::Plan::new(limits, settings::Receive::fixed::<2, { 16 << 10 }>())
        .expect("test limits fit the receive plan")
}

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

pub fn with_domain(run: impl for<'d> FnOnce(&'d mut resident::Domain, region::Identity<'d>)) {
    let mut domain = resident::Domain::new();
    region::Token::scope_pinned(Pin::new(&mut domain), |token, domain| {
        run(domain.get_mut(), token.proof());
    });
}

pub fn connection<'e, 'd: 'e, R: sync::Dispatch<'d>>(
    endpoint: &'e sync::Endpoint<R>,
    domain: &'d mut resident::Domain,
    proof: region::Identity<'d>,
    capacity: usize,
) -> sync::Conn<'e, 'd, R> {
    let budget = domain.try_budget(capacity, proof).expect("resident budget");
    let mut server = sync::Conn::try_new(endpoint).expect("sync connection");
    server
        .ingress()
        .try_bind(budget)
        .expect("connection retention");
    server
}

pub struct State {
    pub request_body: Cell<usize>,
    pub request_head: Cell<usize>,
    pub dispatches: Cell<usize>,
    pub large_body: storage::Shared,
    pub invalid_path: Cell<bool>,
    pub header_error: Cell<Option<field::Error>>,
}

impl State {
    pub fn new(large_body: storage::Shared) -> Self {
        Self {
            request_body: Cell::new(0),
            request_head: Cell::new(0),
            invalid_path: Cell::new(false),
            header_error: Cell::new(None),
            dispatches: Cell::new(0),
            large_body,
        }
    }
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

#[derive(Default)]
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

pub fn client() -> machine::Connection<ClientRole> {
    machine::Connection::try_new().expect("client fixture")
}

pub fn try_peer(
    settings: config::Settings,
    recv_window_target: u32,
) -> Result<machine::Connection<ClientRole>, config::Error> {
    machine::Connection::with_config(config::Config {
        local_settings: settings,
        recv_window_target,
        ..Default::default()
    })
}

impl<'a> Client<'a> {
    const SCHEME: field::Entry<'static> =
        field::Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"http")));
    const AUTHORITY: field::Entry<'static> = field::Entry::pseudo(
        Pseudo::Authority,
        field::literal!(Value::try_bytes(b"localhost")),
    );

    pub const fn get_head(path: Value<&[u8]>) -> [field::Entry<'_>; 4] {
        [
            field::Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"GET"))),
            Self::SCHEME,
            field::Entry::pseudo(Pseudo::Path, path),
            Self::AUTHORITY,
        ]
    }

    pub const fn post_head<'f>(
        path: Value<&'f [u8]>,
        length: Value<&'f [u8]>,
    ) -> [field::Entry<'f>; 5] {
        [
            field::Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"POST"))),
            Self::SCHEME,
            field::Entry::pseudo(Pseudo::Path, path),
            Self::AUTHORITY,
            field::Entry::new(
                field::literal!(field::Lower::try_bytes(b"content-length")),
                length,
            ),
        ]
    }

    pub fn new(conn: &'a mut machine::Connection<ClientRole>) -> Self {
        Self { connection: conn }
    }

    pub fn try_get(self, path: &[u8]) -> Result<storage::Shared, Build> {
        self.connection
            .streams()
            .headers()
            .request(
                Self::get_head(Value::try_bytes(path).map_err(Build::Field)?),
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
                Self::post_head(
                    Value::try_bytes(path).map_err(Build::Field)?,
                    Value::try_bytes(body.len().to_string().as_bytes()).map_err(Build::Field)?,
                ),
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
        let (first, second) = self.connection.output().slices();
        let Some(slices) = Slices::try_new([first, second])
            .map_err(storage::AllocError::Capacity)
            .map_err(Build::Storage)?
        else {
            return Ok(storage::Shared::new());
        };
        let wire = slices
            .try_owned()
            .map_err(storage::AllocError::Exhausted)
            .map_err(Build::Storage)?
            .into_shared();
        self.connection.transfer().drain(wire.len());
        Ok(wire)
    }
}

pub mod wire;

pub mod pending;
