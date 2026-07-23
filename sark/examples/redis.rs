use std::io;
use std::net::SocketAddr;

use cartel_redis::{DEFAULT_BACKOFF, Ops, Redis};
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use http::StatusCode;
use sark::{HttpServer, Throughput, app, driver, listener, tcp};

type Env = Bundle<Tcp, Identity, Throughput>;

mod support;

const MAX_CONNECTIONS: usize = 1024;
const HTTP_LISTENER_ID: u8 = 0;
const DATE_UPDATER_ID: u8 = 1;
const REDIS_CONNECTOR_ID: u8 = 2;
const VISIT_KEYS: [&[u8]; 1] = [b"sark:visits"];
#[derive(Clone, Copy)]
struct AppState<'d> {
    redis: Redis<'d>,
}

#[sark_gen::json(encode)]
struct VisitBody {
    ok: bool,
    visits: i64,
    error: String,
}

#[sark_gen::response(json)]
#[header("content-type", "application/json")]
struct VisitResponse {
    status: StatusCode,
    body: VisitBody,
}

fn response(status: StatusCode, body: VisitBody) -> VisitResponse {
    VisitResponse { status, body }
}

#[sark_gen::handler]
async fn visit(state: &AppState<'_>) -> VisitResponse {
    match state.redis.incr(b"sark:visits").await {
        Ok(visits) => response(
            StatusCode::OK,
            VisitBody {
                ok: true,
                visits,
                error: String::new(),
            },
        ),
        Err(error) => response(
            StatusCode::BAD_GATEWAY,
            VisitBody {
                ok: false,
                visits: 0,
                error: error.to_string(),
            },
        ),
    }
}

#[sark_gen::handler]
async fn reset(state: &AppState<'_>) -> VisitResponse {
    match state.redis.del(&VISIT_KEYS).await {
        Ok(_) => response(
            StatusCode::OK,
            VisitBody {
                ok: true,
                visits: 0,
                error: String::new(),
            },
        ),
        Err(error) => response(
            StatusCode::BAD_GATEWAY,
            VisitBody {
                ok: false,
                visits: 0,
                error: error.to_string(),
            },
        ),
    }
}

sark_gen::define_route! {
    HttpWithRedisApp: AppState<'_> => {
        GET "/" => async(capacity = MAX_CONNECTIONS) visit,
        POST "/reset" => async(capacity = MAX_CONNECTIONS) reset,
    }
}

fn main() -> io::Result<()> {
    let bind: SocketAddr = std::env::var("BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse()
        .expect("invalid BIND");
    let redis_addr: SocketAddr = std::env::var("REDIS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6379".to_string())
        .parse()
        .expect("invalid REDIS_ADDR");
    let server = HttpServer::<HTTP_LISTENER_ID, DATE_UPDATER_ID, Throughput>::new(
        listener::Config::<Tcp> {
            bind,
            max_connections: MAX_CONNECTIONS,
            backlog: 1024,
            stream: tcp::stream::Config {
                no_delay: Some(true),
                ..Default::default()
            },
            transport: tcp::listener::Config {
                reuse_port: true,
                ..Default::default()
            },
            egress: Default::default(),
        },
        std::time::Duration::from_secs(10),
    );

    eprintln!("sark redis: listening on http://{bind}, upstream redis {redis_addr}");
    let redis_factory = support::redis_config().map_err(io::Error::other)?.factory();

    server.run_with_storage(
        vec![0u16],
        |_| driver::Config::for_tcp_profile::<Throughput>(MAX_CONNECTIONS),
        move |_, _| redis_factory,
        move |server, session| {
            let backoff = session
                .seed()
                .derive(dope::hash::domain::BACKOFF ^ REDIS_CONNECTOR_ID as u64)
                .state();
            let (redis, connector) = cartel_redis::attach::<REDIS_CONNECTOR_ID, Env>(
                session,
                Static::<Tcp>::new(vec![redis_addr], DEFAULT_BACKOFF, backoff),
            )?;
            let state = AppState { redis };
            let timer = sark::Timer::with_capacity(MAX_CONNECTIONS.saturating_mul(2));
            let app = HttpWithRedisApp::new(
                &state,
                &timer,
                app::Config {
                    timer_capacity: MAX_CONNECTIONS.saturating_mul(2),
                    task_capacity: MAX_CONNECTIONS,
                },
            );
            server.serve_with_resource(session, app, connector, None)
        },
    )
}
