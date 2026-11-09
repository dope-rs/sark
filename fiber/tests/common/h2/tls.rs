use std::{cell::Cell, io, io::Write, iter, sync::Arc};

use dope::{
    core::{
        driver::{route::KeyTag, route::table::conn::Capacity, settings},
        io::socket::{self, Listener},
    },
    manifold::network::listener::config,
    net::tcp,
};
use dope_test::{alloc::Tracker, fibers::Yield};
use dope_tls::{config::raw::Identity, rustls};
use o3::{buffer, depot::{capacity, heap}};
use sark_grpc::{
    Code as GrpcCode, Status, encode,
    protocol::request,
    server::service::{
        self,
        handler::{self, modes, output},
        reply, timers,
    },
};
use sark_h2::{
    ClientRole, conn::machine::Connection,
    server::{runtime::Plan, task::timers::{Controller, Policy}},
};
use sark_http::{
    field::{self, Entry, Pseudo, Value},
    payload::{fixed, response::status::Code},
};

use crate::common::client::tls::SEED;

pub const RESPONSE: &[u8] = b"shared async route";
pub const UNARY: &[u8] = b"/benchmark.BenchmarkService/GetSum";
pub const STREAM: &[u8] = b"/benchmark.BenchmarkService/StreamSum";
pub const HEADERS: [Entry<'static>; 4] = [
    Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"GET"))),
    Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"https"))),
    Entry::pseudo(Pseudo::Path, field::literal!(Value::try_bytes(b"/shared"))),
    Entry::pseudo(
        Pseudo::Authority,
        field::literal!(Value::try_bytes(b"localhost")),
    ),
];

pub struct State(pub Cell<usize>, pub bool);

pub struct Deadlines(pub sark_core::deadline::config::Config, pub bool);

impl<'d> Policy<'d> for Deadlines {
    const ENABLED: bool = true;
    type State = Controller<'d>;

    fn state(&self, capacity: capacity::Count) -> Result<Self::State, heap::Error> {
        if self.1 {
            Tracker::fail_after(0, || self.0.state(capacity))
        } else {
            self.0.state(capacity)
        }
    }

    fn controller<'a>(&self, state: &'a mut Self::State) -> Option<&'a mut Controller<'d>> {
        self.0.controller(state)
    }
}

#[sark_fiber::response(raw)]
pub struct Reply<'d> {
    status: Code,
    body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
async fn shared(state: &State) -> Reply<'d> {
    Yield::new().await;
    state.0.set(state.0.get() + 1);
    Reply {
        status: Code::OK,
        body: fixed::Body::new(RESPONSE),
    }
}

#[sark_fiber::duplex(pub)]
fn half_close(
    _: crate::common::route::body::tunnels::Connect,
    state: &State,
) -> super::apps::DuplexEcho {
    super::apps::DuplexEcho(if state.1 { 1 } else { 3 })
}

sark_fiber::define_route! {
    pub Shared: State => {
        GET "/shared" => async(capacity = 1, body = discarded) shared,
        CONNECT "*" => half_close,
    }
}

pub struct Rpc;

impl handler::Handler for Rpc {
    type Call = <output::Streaming<Self> as handler::Handler>::Call;
    type Timer = timers::Off;

    fn start<W: reply::Write>(
        &mut self,
        _: &mut (),
        _: &mut service::Stream,
        head: &request::Head,
        reply: &mut reply::Reply<'_, W>,
    ) -> handler::Start<Self::Call> {
        if head.path() != UNARY && head.path() != STREAM {
            reply.finish_with(Status::from_code(GrpcCode::Unimplemented));
        }
        handler::Start::buffered(Default::default())
    }

    fn request<S: buffer::raw::Segment, W: reply::Write>(
        &mut self,
        _: &mut (),
        call: &mut Self::Call,
        request: service::Request<'_, S>,
        response: &mut service::Response<'_, W>,
    ) -> handler::RequestState {
        assert_eq!(
            request.head.metadata().values(b"x-request").next(),
            Some(b"request-only".as_slice())
        );
        assert_eq!(
            request.head.metadata().values(b"x-request-tail").next(),
            None
        );
        assert_eq!(request.trailers.values(b"x-request").next(), None);
        assert_eq!(
            request.trailers.values(b"x-request-tail").next(),
            Some(b"tail-only".as_slice())
        );
        if request.head.path() == UNARY {
            modes::Unary::new(Self, encode::Prost::new()).request(
                &mut (),
                &mut (),
                request,
                response,
            )
        } else {
            output::Streaming::new(Self, encode::Prost::new()).request(
                &mut (),
                call,
                request,
                response,
            )
        }
    }

    fn output<W: reply::Write>(
        &mut self,
        _: &mut (),
        call: &mut Self::Call,
        output: &mut timers::Output<'_, '_, '_, Self::Timer>,
        reply: &mut reply::Reply<'_, W>,
    ) -> handler::OutputState {
        output::Streaming::new(Self, encode::Prost::new()).output(&mut (), call, output, reply)
    }
}

impl modes::UnaryFn for Rpc {
    type Response = ();
    type Codec = encode::Prost<(), ()>;

    fn unary<'a, S: buffer::raw::Segment + 'a>(
        &mut self,
        _: &mut (),
        _: modes::Request<()>,
    ) -> modes::Response<()> {
        modes::Response::new(())
    }
}

impl output::StreamFn for Rpc {
    type Response = ();
    type Stream = iter::Take<iter::Repeat<()>>;
    type Codec = encode::Prost<(), ()>;

    fn stream<'a, S: buffer::raw::Segment + 'a>(
        &mut self,
        _: &mut (),
        request: modes::Request<()>,
    ) -> output::Response<Self::Stream> {
        let count = match request.head.metadata().values(b"x-count").next() {
            Some(b"3") => 3,
            Some(b"0") => 0,
            _ => panic!("unexpected stream count"),
        };
        output::Response::new(iter::repeat(()).take(count))
    }
}

pub fn plan(listener: Listener) -> Result<Plan<KeyTag<2>>, sark_h2::server::ConfigError> {
    Ok(Plan::new(
        config::Prepared {
            bind: listener.local_addr(),
            connections: Capacity::fixed::<1>(),
            per_ip: None,
            direct_flights: Default::default(),
            backlog: socket::Backlog::fixed::<1>(),
            stream: tcp::StreamConfig::default(),
            transport: Default::default(),
            egress: Default::default(),
        },
        settings::Receive::fixed::<64, { 32 << 10 }>(),
        super::server::CONFIG,
    )?
    .with_bound(listener))
}

pub fn server_config(
    fragment: Option<usize>,
) -> Result<Arc<rustls::ServerConfig>, dope_tls::Error> {
    let mut config = Identity::from_seed(&SEED)?.server()?;
    config.alpn_protocols.push(b"h2".to_vec());
    config.max_fragment_size = fragment;
    Ok(Arc::new(config))
}

pub fn client_config() -> Result<Arc<rustls::ClientConfig>, dope_tls::Error> {
    let key = Identity::try_public_key_from_seed(&SEED)?;
    let mut config = Identity::client(key)?;
    config.alpn_protocols.push(b"h2".to_vec());
    Ok(Arc::new(config))
}

pub fn flush(client: &mut Connection<ClientRole>, wire: &mut impl Write) -> io::Result<()> {
    loop {
        let output = client.output().first();
        if output.is_empty() {
            return wire.flush();
        }
        wire.write_all(output)?;
        let length = output.len();
        client.transfer().drain(length);
    }
}
