use std::io;
use std::net::SocketAddr;

use cartel_redis::{DEFAULT_BACKOFF, Ops, Session};
use dope::fiber::Holding;
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
use o3::buffer::{Owned, Shared};
use sark::date::{DateHost, Updater};
use sark::timer::TimerHost;
use sark::{Application, ServerCfg};
use sark_core::http::LocalFrameBytes;

const BOARD_KEY: &[u8] = b"sark:leaderboard";

type Env = Bundle<Tcp, Identity, Throughput>;
type RedisConnector = Connector<0, Session, Static<Tcp>, Env>;
type RedisClient<'d> = Holding<'d, RedisConnector>;

#[derive(Clone)]
struct AppState<'d> {
    redis: RedisClient<'d>,
}

#[sark_gen::response(raw)]
#[header("content-type", "application/json")]
struct JsonResponse {
    status: StatusCode,
    body: Owned,
    #[header("x-board-key")]
    board_key: LocalFrameBytes,
}

#[sark_gen::request(ordered)]
struct PostScoreRequest {
    #[query("user", default = "")]
    pub user: LocalFrameBytes,
    #[query("value", default = "")]
    pub value: LocalFrameBytes,
}

#[sark_gen::request(ordered)]
struct GetTopRequest {
    #[query("n", default = "10")]
    pub n: LocalFrameBytes,
}

#[sark_gen::request(ordered)]
struct GetRankRequest {
    #[path("user", default = "")]
    pub user: LocalFrameBytes,
}

#[sark_gen::handler]
async fn post_score(request: PostScoreRequest, state: &AppState<'_>) -> JsonResponseInner<'req> {
    let user = request.user.as_bytes().to_vec();
    let value: Option<f64> = std::str::from_utf8(request.value.as_bytes())
        .ok()
        .and_then(|s| s.parse().ok());
    let body = match value {
        Some(v) if !user.is_empty() => match state.redis.zadd(BOARD_KEY, v, &user).await {
            Ok(added) => match state.redis.zcard(BOARD_KEY).await {
                Ok(total) => format!("{{\"ok\":true,\"added\":{},\"total\":{}}}\n", added, total),
                Err(e) => format!("{{\"error\":\"{}\"}}\n", e),
            },
            Err(e) => format!("{{\"error\":\"{}\"}}\n", e),
        },
        _ => "{\"error\":\"missing user or value\"}\n".to_string(),
    };
    JsonResponseInner {
        status: StatusCode::OK,
        body: bytes_from_str(&body),
        board_key: LocalFrameBytes::from_slice(BOARD_KEY),
    }
}

#[sark_gen::handler]
async fn get_top(request: GetTopRequest, state: &AppState<'_>) -> JsonResponseInner<'req> {
    let n: i64 = std::str::from_utf8(request.n.as_bytes())
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let stop = (n - 1).max(0);
    let stop_str = stop.to_string();
    let body = match state
        .redis
        .cmd::<Vec<(Shared, f64)>>(&[
            b"ZREVRANGE",
            BOARD_KEY,
            b"0",
            stop_str.as_bytes(),
            b"WITHSCORES",
        ])
        .await
    {
        Ok(rows) => format_top(&rows),
        Err(e) => format!("{{\"error\":\"{}\"}}\n", e),
    };
    JsonResponseInner {
        status: StatusCode::OK,
        body: bytes_from_str(&body),
        board_key: LocalFrameBytes::from_slice(BOARD_KEY),
    }
}

#[sark_gen::handler]
async fn get_rank(request: GetRankRequest, state: &AppState<'_>) -> JsonResponseInner<'req> {
    let user = request.user.as_bytes().to_vec();
    let user_str = String::from_utf8_lossy(&user).into_owned();
    let body = match state
        .redis
        .cmd::<Option<u64>>(&[b"ZREVRANK", BOARD_KEY, &user])
        .await
    {
        Ok(Some(rank)) => {
            let score = state.redis.zscore(BOARD_KEY, &user).await.ok().flatten();
            let score_str = score
                .map(|s| format!("{}", s))
                .unwrap_or_else(|| "null".to_string());
            format!(
                "{{\"user\":\"{}\",\"rank\":{},\"score\":{}}}\n",
                user_str, rank, score_str
            )
        }
        Ok(None) => "{\"error\":\"not found\"}\n".to_string(),
        Err(e) => format!("{{\"error\":\"{}\"}}\n", e),
    };
    JsonResponseInner {
        status: StatusCode::OK,
        body: bytes_from_str(&body),
        board_key: LocalFrameBytes::from_slice(BOARD_KEY),
    }
}

sark_gen::define_route! {
    LeaderboardApp: AppState<'d> => {
        POST "/score" => async post_score,
        GET "/top" => async get_top,
        GET "/rank/:user" => async get_rank,
    }
}

fn format_top(rows: &[(Shared, f64)]) -> String {
    let mut out = String::from("[");
    for (i, (member, score)) in rows.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let user = String::from_utf8_lossy(member.as_slice());
        out.push_str(&format!("{{\"user\":\"{}\",\"score\":{}}}", user, score));
    }
    out.push_str("]\n");
    out
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
    redis: RedisConnector,
    #[pin]
    #[manifold]
    timer: dope::manifold::timer::Timer<{ sark::timer::SARK_TIMER_ID }>,
    _ph: std::marker::PhantomData<&'d ()>,
}

fn run_thread(
    redis_addr: SocketAddr,
    cfg: ServerCfg,
    ctx: Ctx,
    shutdown: Option<&Trigger>,
) -> io::Result<()> {
    let driver_cfg =
        <dope::DriverCfg as dope::DriverConfig>::for_tcp_profile::<Throughput>(cfg.max_conn)
            .with_cpu_id(Some(ctx.cpu));
    let mut exec = Executor::new(driver_cfg)?;

    let redis_conn = {
        let drv = exec.driver_mut();
        if let Some(trigger) = shutdown {
            trigger.register(drv);
        }
        Connector::new(
            Session::new(),
            Static::<Tcp>::new(vec![redis_addr], DEFAULT_BACKOFF),
            1,
            drv,
        )
    };
    let mut app = core::pin::pin!(Dispatcher::<_> {
        http: None::<Listener<1, _, Env>>,
        date: Updater::<2>::new(),
        redis: redis_conn,
        timer: dope::manifold::timer::Timer::new(),
        _ph: std::marker::PhantomData,
    });
    let client = app.as_mut().redis_handle();
    let timer_handle = app.as_mut().timer_handle();

    let listener_cfg = config::Config::<Tcp> {
        max_conn: cfg.max_conn,
        bind: cfg.bind,
        backlog: cfg.backlog,
        stream_opts: Default::default(),
        listener_opts: Default::default(),
    };
    let state: &'static AppState = Box::leak(Box::new(AppState { redis: client }));
    let app_state = leaderboard_app::new(state);
    let mut http = {
        let drv = exec.driver_mut();
        Listener::<1, _, Env>::open_in(app_state, listener_cfg, drv)?
    };
    {
        let handler = http.handler_mut();
        handler.bind_timer(timer_handle, cfg.head_timeout);
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
    let redis_addr: SocketAddr = std::env::var("REDIS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6379".to_string())
        .parse()
        .expect("invalid REDIS_ADDR");

    let cfg = ServerCfg {
        bind,
        max_conn: 1024,
        backlog: 1024,
        head_timeout: std::time::Duration::from_secs(10),
    };

    eprintln!("sark leaderboard: listening on http://{bind}, upstream redis {redis_addr}");

    Launcher::new(vec![0u16]).run(move |ctx| run_thread(redis_addr, cfg.clone(), ctx, None))
}
