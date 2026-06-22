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
use o3::buffer::Owned;
use sark::date::{DateHost, Updater};
use sark::timer::TimerHost;
use sark::{Application, ServerCfg};
use sark_core::http::LocalFrameBytes;

type Env = Bundle<Tcp, Identity, Throughput>;
type RedisConnector = Connector<0, Session, Static<Tcp>, Env>;

#[derive(Clone)]
struct AppState<'d> {
    redis: Holding<'d, RedisConnector>,
}

#[sark_gen::response(raw)]
#[header("content-type", "text/plain; charset=utf-8")]
struct PlainTextResponse {
    status: StatusCode,
    body: Owned,
    #[header("x-redis-key")]
    redis_key: LocalFrameBytes,
}

#[sark_gen::request(ordered)]
struct EmptyRequest {}

#[sark_gen::handler]
async fn visit(_request: EmptyRequest, state: &AppState<'_>) -> PlainTextResponseInner<'req> {
    let body_str = match state.redis.incr(b"sark:visits").await {
        Ok(n) => format!("Visits: {}\n", n),
        Err(e) => format!("redis error: {}\n", e),
    };
    PlainTextResponseInner {
        status: StatusCode::OK,
        body: bytes_from_str(&body_str),
        redis_key: LocalFrameBytes::from_slice(b"sark:visits"),
    }
}

#[sark_gen::handler]
async fn reset(_request: EmptyRequest, state: &AppState<'_>) -> PlainTextResponseInner<'req> {
    let _ = state.redis.del(&[b"sark:visits"]).await;
    PlainTextResponseInner {
        status: StatusCode::OK,
        body: bytes_from_str("reset\n"),
        redis_key: LocalFrameBytes::from_slice(b"sark:visits"),
    }
}

sark_gen::define_route! {
    HttpWithRedisApp: AppState<'d> => {
        GET "/" => async visit,
        GET "/reset" => async reset,
    }
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
    let app_state = http_with_redis_app::new(state);
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
    let redis_addr: SocketAddr = std::env::var("REDIS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:6379".to_string())
        .parse()
        .expect("invalid REDIS_ADDR");

    let cfg = ServerCfg {
        bind,
        max_conn: 1024,
        backlog: 1024,
    };

    eprintln!("sark http_with_redis: listening on http://{bind}, upstream redis {redis_addr}");

    Launcher::new(vec![0u16]).run(move |ctx| run_thread(redis_addr, cfg.clone(), ctx, None))
}
