use std::io;
use std::net::SocketAddr;

use dope::launcher::Ctx;
use dope::manifold::env::Bundle;
use dope::manifold::listener::Listener;
use dope::manifold::listener::config::Config;
use dope::runtime::profile::Throughput;
use dope::transport::Tcp;
use dope::wire::Identity;
use dope::{DriverConfig, Executor};
use dope_extra::Trigger;

pub type WsEnv = Bundle<Tcp, Identity, Throughput>;

mod app;

pub use app::{App, ConnState, Handler, Message, Response};

#[derive(Clone, Debug)]
pub struct Cfg {
    pub bind: SocketAddr,
    pub max_conn: usize,
    pub backlog: i32,
    pub path: &'static str,
    pub max_frame_payload: usize,
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Dispatcher<H: Handler> {
    #[pin]
    #[manifold]
    listener: Listener<0, App<H>, WsEnv>,
}

pub fn serve<H: Handler>(
    handler: H,
    cfg: Cfg,
    ctx: Ctx,
    shutdown: Option<&Trigger>,
) -> io::Result<()> {
    let listener_cfg = Config::<Tcp> {
        max_conn: cfg.max_conn,
        bind: cfg.bind,
        backlog: cfg.backlog,
        stream_opts: Default::default(),
        listener_opts: dope::transport::config::tcp::ListenerOpts {
            reuseport: dope::transport::config::SocketToggle::Enabled,
            per_ip_cap: Some((cfg.max_conn / 2) as u32),
            ..Default::default()
        },
    };
    let driver_cfg = <dope::DriverCfg as DriverConfig>::for_tcp_profile::<Throughput>(cfg.max_conn)
        .with_cpu_id(Some(ctx.cpu));
    let mut exec = Executor::new(driver_cfg)?;
    let drv = exec.driver_mut();
    if let Some(trigger) = shutdown {
        trigger.register(drv);
    }
    let listener = Listener::<0, App<H>, WsEnv>::open_in(
        App::new(handler, cfg.path, cfg.max_frame_payload),
        listener_cfg,
        drv,
    )?;
    let mut app = core::pin::pin!(Dispatcher { listener });
    exec.run(app.as_mut())
}
