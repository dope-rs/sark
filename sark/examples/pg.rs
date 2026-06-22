use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use cartel_gen::{pg_instance, query_group};
use cartel_pg::{self, Config, PgHolding, PgOps, PgPool, PgTable, PickPolicy};
use dope::launcher::{Ctx, Launcher};
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Static;
use dope::manifold::env::Bundle;
use dope::manifold::listener::{Listener, config};
use dope::runtime::profile::Throughput;
use dope::transport::Tcp;
use dope::wire::Identity;
use dope::{DriverConfig, Executor};
use dope_extra::Trigger;
use http::StatusCode;
use o3::buffer::Owned;
use sark::date::{DateHost, Updater};
use sark::timer::TimerHost;
use sark::{Application, ServerCfg};
use sark_core::http::LocalFrameBytes;

type Env = Bundle<Tcp, Identity, Throughput>;
type PgClient<'d> = PgHolding<'d, Db, Static<Tcp>, Env>;
type PgConnector = Connector<0, cartel_pg::Session<Db>, Static<Tcp>, Env>;

#[derive(PgTable, Debug)]
struct User {
    #[pk]
    id: i64,
    name: String,
}

#[query_group]
impl User {
    fn by_id(id: i64) -> User {
        User::filter(|u| u.id == id).one()
    }

    fn above(min_id: i64) -> Vec<User> {
        User::filter(|u| u.id > min_id).all()
    }

    fn rename(id: i64, new_name: String) {
        User::filter(|u| u.id == id).update(|u| u.name = new_name)
    }
}

pg_instance! { Db: User }

#[derive(Clone)]
struct AppState<'d> {
    pg: PgClient<'d>,
}

#[sark_gen::response(raw)]
#[header("content-type", "text/plain; charset=utf-8")]
struct PlainTextResponse {
    status: StatusCode,
    body: Owned,
    #[header("x-pg-key")]
    pg_key: LocalFrameBytes,
}

#[sark_gen::request(ordered)]
struct UserByIdRequest {
    #[path("id", default = "0")]
    pub id: LocalFrameBytes,
}

#[sark_gen::request(ordered)]
struct TxRequest {
    #[path("id", default = "0")]
    pub id: LocalFrameBytes,
    #[path("name", default = "")]
    pub name: LocalFrameBytes,
}

#[sark_gen::request(ordered)]
struct ListRequest {}

#[sark_gen::handler]
async fn get_user(request: UserByIdRequest, state: &AppState<'_>) -> PlainTextResponseInner<'req> {
    let id: i64 = parse_i64(request.id.as_bytes()).unwrap_or(0);
    let body_str = match User::by_id(&state.pg, id).await {
        Ok(u) => format!("user {}: {}\n", u.id, u.name),
        Err(e) => format!("pg error: {}\n", e),
    };
    PlainTextResponseInner {
        status: StatusCode::OK,
        body: bytes_from_str(&body_str),
        pg_key: request.id,
    }
}

#[sark_gen::handler]
async fn rename_in_tx(request: TxRequest, state: &AppState<'_>) -> PlainTextResponseInner<'req> {
    let id: i64 = parse_i64(request.id.as_bytes()).unwrap_or(0);
    let name = std::str::from_utf8(request.name.as_bytes())
        .unwrap_or("")
        .to_owned();
    let body_str = match state.pg.begin().await {
        Ok(tx) => {
            let view = async {
                User::rename(&tx, id, name).await?;
                User::by_id(&tx, id).await
            }
            .await;
            tx.rollback().await.ok();
            match view {
                Ok(u) => format!("inside-tx user {}: {}\n", u.id, u.name),
                Err(e) => format!("inside-tx error: {}\n", e),
            }
        }
        Err(e) => format!("begin error: {}\n", e),
    };
    PlainTextResponseInner {
        status: StatusCode::OK,
        body: bytes_from_str(&body_str),
        pg_key: request.id,
    }
}

#[sark_gen::handler]
async fn list_users(_request: ListRequest, state: &AppState<'_>) -> PlainTextResponseInner<'req> {
    let body_str = match User::above(&state.pg, 0).await {
        Ok(users) => {
            let mut s = String::new();
            for u in users {
                s.push_str(&format!("{}: {}\n", u.id, u.name));
            }
            s
        }
        Err(e) => format!("pg error: {}\n", e),
    };
    PlainTextResponseInner {
        status: StatusCode::OK,
        body: bytes_from_str(&body_str),
        pg_key: LocalFrameBytes::from_slice(b""),
    }
}

sark_gen::define_route! {
    PgApp: AppState<'d> => {
        GET "/users/:id" => async get_user,
        POST "/tx/:id/:name" => async rename_in_tx,
        GET "/users" => async list_users,
    }
}

fn parse_i64(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

fn bytes_from_str(s: &str) -> Owned {
    let mut buf = Owned::with_capacity(s.len());
    buf.extend_from_slice(s.as_bytes());
    buf
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Dispatcher<'d, P>
where
    P: Application<Conn = sark::dispatch::conn_state::ConnState, Wire = dope::wire::Identity>
        + DateHost
        + TimerHost<'d>,
{
    #[pin]
    #[manifold(optional)]
    http: Option<Listener<1, P, Env>>,
    #[pin]
    #[manifold]
    date: Updater<2>,
    #[pin]
    #[manifold]
    pg: PgConnector,
    #[pin]
    #[manifold]
    timer: dope::manifold::timer::Timer<{ sark::timer::SARK_TIMER_ID }>,
    _ph: std::marker::PhantomData<&'d ()>,
}

struct PgArgs {
    addr: SocketAddr,
    config: Config,
    policy: PickPolicy,
}

fn run_thread(pg: PgArgs, cfg: ServerCfg, ctx: Ctx, shutdown: Option<&Trigger>) -> io::Result<()> {
    let driver_cfg =
        <dope::DriverCfg as dope::DriverConfig>::for_tcp_profile::<Throughput>(cfg.max_conn)
            .with_cpu_id(Some(ctx.cpu));
    let mut exec = Executor::new(driver_cfg)?;

    let pg_conn = {
        let drv = exec.driver_mut();
        if let Some(trigger) = shutdown {
            trigger.register(drv);
        }
        Connector::new(
            cartel_pg::Session::new(pg.config),
            Static::<Tcp>::new(vec![pg.addr], Duration::from_millis(500)),
            4,
            drv,
        )
    };
    let mut app = core::pin::pin!(Dispatcher::<_> {
        http: None::<Listener<1, _, Env>>,
        date: Updater::<2>::new(),
        pg: pg_conn,
        timer: dope::manifold::timer::Timer::new(),
        _ph: std::marker::PhantomData,
    });
    let client = app.as_mut().pg_handle();
    let timer_handle = app.as_mut().timer_handle();
    client.set_pick_policy(pg.policy);

    let listener_cfg = config::Config::<Tcp> {
        max_conn: cfg.max_conn,
        bind: cfg.bind,
        backlog: cfg.backlog,
        stream_opts: Default::default(),
        listener_opts: Default::default(),
    };
    let state: &'static AppState = Box::leak(Box::new(AppState { pg: client }));
    let app_state = pg_app::new(state);
    let mut http = {
        let drv = exec.driver_mut();
        Listener::<1, _, Env>::open_in(app_state, listener_cfg, drv)?
    };
    {
        let handler = http.handler_mut();
        handler.bind_timer(timer_handle);
        let stamp = std::ptr::NonNull::from(handler.date_stamp());
        app.as_mut().project().date.get_mut().bind(stamp);
    }
    app.as_mut().project().http.set(Some(http));
    exec.run(app.as_mut())
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
    let cfg = ServerCfg {
        bind,
        max_conn: 1024,
        backlog: 1024,
    };

    eprintln!("sark pg: listening on http://{bind}, upstream pg {pg_addr}, pick={policy:?}");

    Launcher::new(vec![0u16]).run(move |ctx| {
        let pg = PgArgs {
            addr: pg_addr,
            config: Config::new(pg_user.clone(), pg_password.clone(), pg_database.clone()),
            policy,
        };
        run_thread(pg, cfg.clone(), ctx, None)
    })
}
