use std::io;
use std::net::SocketAddr;

use dope::manifold::env::Bundle;
use dope::manifold::listener::Listener;
use dope::runtime::profile::Throughput;
use dope::runtime::{ShutdownTrigger, WorkerContext};
use dope_net::tcp::Tcp;
use dope_net::wire::identity::Identity;

pub type WsEnv = Bundle<Tcp, Identity, Throughput>;

mod app;

pub use app::{App, ConnState, Handler, Message, Response};

#[derive(Clone, Debug)]
pub struct Config {
    pub bind: SocketAddr,
    pub max_connections: usize,
    pub backlog: i32,
    pub path: &'static str,
    pub max_frame_payload: usize,
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Dispatcher<'d, H: Handler> {
    #[pin]
    #[manifold]
    listener: Listener<'d, 0, App<H>, WsEnv>,
}

pub fn serve<H: Handler>(
    handler: H,
    cfg: Config,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()> {
    let listener_cfg = dope::manifold::listener::Config::<Tcp> {
        max_connections: cfg.max_connections,
        bind: cfg.bind,
        backlog: cfg.backlog,
        stream: Default::default(),
        transport: dope_net::tcp::listener::Config {
            reuse_port: true,
            per_ip_limit: Some(u32::try_from(cfg.max_connections / 2).unwrap_or(u32::MAX)),
            ..Default::default()
        },
        egress: Default::default(),
    };
    let driver_config = dope::driver::Config::for_tcp_profile::<Throughput>(cfg.max_connections);
    dope::runtime::Executor::with_seed(driver_config, context.seed())?.enter(|mut sess| {
        let hash_builder = sess.seed().derive(dope::hash::domain::ACCEPT).state();
        let listener = {
            let mut driver = sess.driver_access();
            if let Some(trigger) = shutdown {
                trigger.try_register(&mut driver)?;
            }
            Listener::<'_, 0, App<H>, WsEnv>::open_in(
                App::new(handler, cfg.path, cfg.max_frame_payload),
                listener_cfg,
                hash_builder,
                &mut driver,
            )?
        };
        let app = core::pin::pin!(o3::cell::BrandCell::new(Dispatcher { listener }));
        sess.run(app.as_ref())
    })
}
