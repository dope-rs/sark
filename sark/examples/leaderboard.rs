use std::io;
use std::net::SocketAddr;

use cartel_redis::{Connect, DEFAULT_BACKOFF, Ops, Redis};
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use http::StatusCode;
use o3::buffer::{Bytes, Retained};
use sark::{HttpServer, Throughput, app, driver, listener, tcp};

const BOARD_KEY: &[u8] = b"sark:leaderboard";
const MAX_CONNECTIONS: usize = 1024;
const HTTP_LISTENER_ID: u8 = 0;
const DATE_UPDATER_ID: u8 = 1;
const REDIS_CONNECTOR_ID: u8 = 2;

mod support;

type Env = Bundle<Tcp, Identity, Throughput>;

#[derive(Clone, Copy)]
struct AppState<'d> {
    redis: Redis<'d>,
}

#[sark_gen::json(ordered)]
struct PostScoreBody {
    user: Bytes<Retained>,
    value: Bytes<Retained>,
}

#[sark_gen::json(encode)]
struct PostScoreResult {
    ok: bool,
    added: u64,
    total: u64,
    error: String,
}

#[sark_gen::json(encode)]
struct Score {
    user: o3::buffer::Shared,
    score: f64,
}

#[sark_gen::json(encode)]
struct TopResult {
    ok: bool,
    #[field(seq, nested)]
    scores: Vec<Score>,
    error: String,
}

#[sark_gen::json(encode)]
struct RankResult {
    ok: bool,
    found: bool,
    user: o3::buffer::Shared,
    rank: u64,
    error: String,
}

#[sark_gen::response(json)]
#[header("content-type", "application/json")]
struct PostScoreResponse {
    status: StatusCode,
    body: PostScoreResult,
}

#[sark_gen::response(json)]
#[header("content-type", "application/json")]
struct TopResponse {
    status: StatusCode,
    body: TopResult,
}

#[sark_gen::response(json)]
#[header("content-type", "application/json")]
struct RankResponse {
    status: StatusCode,
    body: RankResult,
}

#[sark_gen::request(ordered)]
#[json_body(PostScoreBody)]
struct PostScoreRequest {}

#[sark_gen::request(ordered)]
struct GetTopRequest {
    #[query("n", default = "10")]
    n: usize,
}

#[sark_gen::request(ordered)]
struct GetRankRequest {
    #[path("user", default = "")]
    user: Bytes<Retained>,
}

#[sark_gen::handler]
async fn post_score(request: PostScoreRequest, state: &AppState<'_>) -> PostScoreResponse {
    let user = request.body.user.into_shared();
    let value = std::str::from_utf8(request.body.value.as_slice())
        .ok()
        .and_then(|value| value.parse::<f64>().ok());
    let Some(value) = value.filter(|value| value.is_finite() && !user.is_empty()) else {
        return PostScoreResponse {
            status: StatusCode::BAD_REQUEST,
            body: PostScoreResult {
                ok: false,
                added: 0,
                total: 0,
                error: String::from("user must be non-empty and value must be finite"),
            },
        };
    };
    let redis = state.redis;
    match redis.zadd(BOARD_KEY, value, user).await {
        Ok(added) => match redis.zcard(BOARD_KEY).await {
            Ok(total) => PostScoreResponse {
                status: StatusCode::CREATED,
                body: PostScoreResult {
                    ok: true,
                    added,
                    total,
                    error: String::new(),
                },
            },
            Err(error) => PostScoreResponse {
                status: StatusCode::BAD_GATEWAY,
                body: PostScoreResult {
                    ok: false,
                    added,
                    total: 0,
                    error: error.to_string(),
                },
            },
        },
        Err(error) => PostScoreResponse {
            status: StatusCode::BAD_GATEWAY,
            body: PostScoreResult {
                ok: false,
                added: 0,
                total: 0,
                error: error.to_string(),
            },
        },
    }
}

#[sark_gen::handler]
async fn get_top(request: GetTopRequest, state: &AppState<'_>) -> TopResponse {
    if !(1..=100).contains(&request.n) {
        return TopResponse {
            status: StatusCode::BAD_REQUEST,
            body: TopResult {
                ok: false,
                scores: Vec::new(),
                error: String::from("n must be between 1 and 100"),
            },
        };
    }
    match state
        .redis
        .zrev_range_with_scores(BOARD_KEY, 0, request.n as i64 - 1)
        .await
    {
        Ok(rows) => {
            let scores = rows
                .into_iter()
                .map(|(member, score)| Score {
                    user: member,
                    score,
                })
                .collect();
            TopResponse {
                status: StatusCode::OK,
                body: TopResult {
                    ok: true,
                    scores,
                    error: String::new(),
                },
            }
        }
        Err(error) => TopResponse {
            status: StatusCode::BAD_GATEWAY,
            body: TopResult {
                ok: false,
                scores: Vec::new(),
                error: error.to_string(),
            },
        },
    }
}

#[sark_gen::handler]
async fn get_rank(request: GetRankRequest, state: &AppState<'_>) -> RankResponse {
    let user = o3::buffer::Shared::copy_from_slice(request.user.as_slice());
    if user.is_empty() {
        return RankResponse {
            status: StatusCode::BAD_REQUEST,
            body: RankResult {
                ok: false,
                found: false,
                user: o3::buffer::Shared::new(),
                rank: 0,
                error: String::from("user must be non-empty"),
            },
        };
    }
    match state.redis.zrev_rank(BOARD_KEY, user.clone()).await {
        Ok(Some(rank)) => RankResponse {
            status: StatusCode::OK,
            body: RankResult {
                ok: true,
                found: true,
                user,
                rank,
                error: String::new(),
            },
        },
        Ok(None) => RankResponse {
            status: StatusCode::NOT_FOUND,
            body: RankResult {
                ok: true,
                found: false,
                user,
                rank: 0,
                error: String::from("not found"),
            },
        },
        Err(error) => RankResponse {
            status: StatusCode::BAD_GATEWAY,
            body: RankResult {
                ok: false,
                found: false,
                user,
                rank: 0,
                error: error.to_string(),
            },
        },
    }
}

sark_gen::define_route! {
    LeaderboardApp: AppState<'_> => {
        POST "/score" => async(capacity = MAX_CONNECTIONS) post_score,
        GET "/top" => async(capacity = MAX_CONNECTIONS) get_top,
        GET "/rank/:user" => async(capacity = MAX_CONNECTIONS) get_rank,
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

    eprintln!("sark leaderboard: listening on http://{bind}, upstream redis {redis_addr}");
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
            let store = session.storage() as *const cartel_redis::Store<'_>;
            // The store remains owned by the executor for the lifetime branded
            // into both the Redis handle and connector.
            let redis = unsafe { (&*store).redis() };
            let connector = {
                let mut driver = session.driver_access();
                redis.connect::<REDIS_CONNECTOR_ID, _, Env>(
                    Connect {
                        topology: Static::<Tcp>::new(vec![redis_addr], DEFAULT_BACKOFF, backoff),
                    },
                    &mut driver,
                )?
            };
            let state = AppState { redis };
            let app = LeaderboardApp::new(
                state,
                app::Config {
                    timer_capacity: MAX_CONNECTIONS.saturating_mul(2),
                    task_capacity: MAX_CONNECTIONS,
                },
            );
            server.serve_with_resource(session, app, connector, None)
        },
    )
}
