use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use cartel_gen::{pg_instance, query_group};
use cartel_pg::{Client, Config, PgOps, PgPool, PgTable, PickPolicy, Port, port};
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;
use http::StatusCode;
use o3::buffer::{Bytes, Retained};
use sark::{HttpServer, Throughput, app, driver, listener, tcp};

type Env = Bundle<Tcp, Identity, Throughput>;

const MAX_CONNECTIONS: usize = 1024;
const HTTP_LISTENER_ID: u8 = 0;
const DATE_UPDATER_ID: u8 = 1;
const PG_CONNECTOR_ID: u8 = 2;
const PG_CONNECTIONS: usize = 4;
const PG_PENDING_REQUESTS_PER_CONNECTION: usize = 16;
const PG_REQUEST_ENTRIES: usize = PG_CONNECTIONS * PG_PENDING_REQUESTS_PER_CONNECTION;
const PG_REQUEST_FRAME_CAPACITY: usize = 4 * 1024;
const PG_RESPONSE_BUFFER_CAPACITY: usize = 256 * 1024 * 1024;
const PG_RESPONSE_ROW_CAPACITY: usize = 65_536;
const PG_INFLIGHT: usize = PG_REQUEST_ENTRIES;
const PG_WAITERS: usize = PG_REQUEST_ENTRIES;
const PG_NOTIFICATIONS: usize = 1024;
const PG_RECONNECT_BACKOFF: Duration = Duration::from_millis(500);

#[derive(PgTable, Debug)]
struct User {
    #[pk]
    id: i64,
    name: String,
}

#[query_group]
impl User {
    fn by_id(id: i64) -> User {
        User::filter(|user| user.id == id).one()
    }

    fn above(min_id: i64) -> Vec<User> {
        User::filter(|user| user.id > min_id).all()
    }

    fn rename(id: i64, new_name: String) {
        User::filter(|user| user.id == id).update(|user| user.name = new_name)
    }
}

pg_instance! { Db: User }

#[derive(Clone, Copy)]
struct AppState<'d> {
    pg: Client<'d, Db>,
}

#[sark_gen::json(ordered)]
struct RenameBody {
    name: Bytes<Retained>,
}

#[sark_gen::json(encode)]
struct UserView {
    id: u64,
    name: String,
}

#[sark_gen::json(encode)]
struct PgBody {
    ok: bool,
    #[field(seq, nested)]
    users: Vec<UserView>,
    error: String,
}

#[sark_gen::response(json)]
#[header("content-type", "application/json")]
struct PgResponse {
    status: StatusCode,
    body: PgBody,
}

fn response(status: StatusCode, body: PgBody) -> PgResponse {
    PgResponse { status, body }
}

fn pg_error(error: cartel_pg::Error) -> PgResponse {
    let status = if matches!(&error, cartel_pg::Error::NotFound) {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::BAD_GATEWAY
    };
    response(
        status,
        PgBody {
            ok: false,
            users: Vec::new(),
            error: error.to_string(),
        },
    )
}

#[sark_gen::request(ordered)]
struct UserByIdRequest {
    #[path("id")]
    id: usize,
}

#[sark_gen::request(ordered)]
#[json_body(RenameBody)]
struct TxRequest {
    #[path("id")]
    id: usize,
}

#[sark_gen::handler]
async fn get_user(request: UserByIdRequest, state: &AppState<'_>) -> PgResponse {
    let Ok(id) = i64::try_from(request.id) else {
        return response(
            StatusCode::BAD_REQUEST,
            PgBody {
                ok: false,
                users: Vec::new(),
                error: String::from("id must be positive"),
            },
        );
    };
    if id <= 0 {
        return response(
            StatusCode::BAD_REQUEST,
            PgBody {
                ok: false,
                users: Vec::new(),
                error: String::from("id must be positive"),
            },
        );
    }
    match User::by_id(&state.pg, id).await {
        Ok(user) => response(
            StatusCode::OK,
            PgBody {
                ok: true,
                users: vec![UserView {
                    id: user.id as u64,
                    name: user.name,
                }],
                error: String::new(),
            },
        ),
        Err(error) => pg_error(error),
    }
}

#[sark_gen::handler]
async fn rename_in_tx(request: TxRequest, state: &AppState<'_>) -> PgResponse {
    let name = match std::str::from_utf8(request.body.name.as_slice()) {
        Ok(name) if !name.is_empty() && name.len() <= 128 => name.to_owned(),
        _ => {
            return response(
                StatusCode::BAD_REQUEST,
                PgBody {
                    ok: false,
                    users: Vec::new(),
                    error: String::from("name must be valid UTF-8 and 1..=128 bytes"),
                },
            );
        }
    };
    let Ok(id) = i64::try_from(request.id) else {
        return response(
            StatusCode::BAD_REQUEST,
            PgBody {
                ok: false,
                users: Vec::new(),
                error: String::from("id must be positive"),
            },
        );
    };
    if id <= 0 {
        return response(
            StatusCode::BAD_REQUEST,
            PgBody {
                ok: false,
                users: Vec::new(),
                error: String::from("id must be positive"),
            },
        );
    }
    match state.pg.begin().await {
        Ok(transaction) => match User::rename(&transaction, id, name).await {
            Ok(()) => match transaction.commit().await {
                Ok(()) => response(
                    StatusCode::OK,
                    PgBody {
                        ok: true,
                        users: Vec::new(),
                        error: String::new(),
                    },
                ),
                Err(error) => pg_error(error),
            },
            Err(error) => {
                let _ = transaction.rollback().await;
                pg_error(error)
            }
        },
        Err(error) => pg_error(error),
    }
}

#[sark_gen::handler]
async fn list_users(state: &AppState<'_>) -> PgResponse {
    match User::above(&state.pg, 0).await {
        Ok(users) => response(
            StatusCode::OK,
            PgBody {
                ok: true,
                users: users
                    .into_iter()
                    .map(|user| UserView {
                        id: user.id as u64,
                        name: user.name,
                    })
                    .collect(),
                error: String::new(),
            },
        ),
        Err(error) => pg_error(error),
    }
}

sark_gen::define_route! {
    PgApp: AppState<'_> => {
        GET "/users/:id" => async(capacity = MAX_CONNECTIONS) get_user,
        POST "/tx/:id" => async(capacity = MAX_CONNECTIONS) rename_in_tx,
        GET "/users" => async(capacity = MAX_CONNECTIONS) list_users,
    }
}

fn main() -> io::Result<()> {
    let bind: SocketAddr = std::env::var("BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse()
        .expect("invalid BIND");
    let pg_addr: SocketAddr = std::env::var("PG_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:5432".to_string())
        .parse()
        .expect("invalid PG_ADDR");
    let pg_user = std::env::var("PG_USER").unwrap_or_else(|_| "postgres".into());
    let pg_password = std::env::var("PG_PASSWORD").unwrap_or_else(|_| "postgres".into());
    let pg_database = std::env::var("PG_DATABASE").unwrap_or_else(|_| "postgres".into());
    let policy = match std::env::var("PG_PICK_POLICY")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "li" | "least_inflight" | "least-inflight" => PickPolicy::LeastInflight,
        _ => PickPolicy::RoundRobin,
    };
    let pg_config = Config::new(pg_user, pg_password, pg_database);
    let port_config = port::Config::new(port::Capacities {
        connections: PG_CONNECTIONS,
        request_entries: PG_REQUEST_ENTRIES,
        request_bytes: PG_REQUEST_FRAME_CAPACITY,
        response_entries: PG_RESPONSE_ROW_CAPACITY,
        response_bytes: PG_RESPONSE_BUFFER_CAPACITY,
        inflight: PG_INFLIGHT,
        waiters: PG_WAITERS,
        notifications: PG_NOTIFICATIONS,
    })
    .map_err(io::Error::other)?;
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
        Duration::from_secs(10),
    );

    eprintln!("sark pg: listening on http://{bind}, upstream pg {pg_addr}, pick={policy:?}");

    server.run_with_storage(
        vec![0u16],
        |_| driver::Config::for_tcp_profile::<Throughput>(MAX_CONNECTIONS),
        move |_, _| Port::<Db>::factory(pg_config.clone(), port_config),
        move |server, session| {
            let backoff = session
                .seed()
                .derive(dope::hash::domain::BACKOFF ^ PG_CONNECTOR_ID as u64)
                .state();
            let (client, connector) = cartel_pg::attach::<PG_CONNECTOR_ID, Env, Db>(
                session,
                Static::<Tcp>::new(vec![pg_addr], PG_RECONNECT_BACKOFF, backoff),
            )?;
            client.set_pick_policy(policy);
            let state = AppState { pg: client };
            let timer = sark::Timer::with_capacity(MAX_CONNECTIONS.saturating_mul(2));
            let app = PgApp::new(
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
