use std::io;
use std::net::SocketAddr;

use dope::manifold::env::{Bundle, Env as ManifoldEnv};
use dope::manifold::listener::{self, Application, Listener};
use dope::runtime::profile::Throughput;
use dope::runtime::{Executor, Session, ShutdownTrigger, WorkerContext};
use dope_net::wire::identity::Identity;
use dope_net::{tcp, tcp::Tcp};
use dope_tls::tls::{Endpoint, Tls};

pub type Env = Bundle<Tcp, Identity, Throughput>;
pub type TlsEnv = Bundle<Tcp, Tls, Throughput>;

mod app;
mod body;
mod connection;
pub mod driver;
mod scheduler;
mod task;

pub use app::{App, ConnState, Handler, SyncApp, SyncConnState, SyncHandler};
pub use body::Body;
pub use connection::{Request, Response};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
    pub max_connections_per_ip: u32,
    pub listen_backlog: i32,
    pub max_handler_tasks: usize,
    pub max_request_body_bytes: usize,
    pub max_connection_body_bytes: usize,
    pub max_outbound_bytes: usize,
    pub socket_receive_buffer_bytes: Option<usize>,
    pub socket_send_buffer_bytes: Option<usize>,
    pub tcp_fast_open_backlog: Option<u32>,
    pub receive_buffer_bytes: usize,
    pub receive_buffer_count: u16,
}

impl Config {
    fn validate(self, asynchronous: bool) -> io::Result<Self> {
        if self.max_connections == 0 {
            return Err(invalid_config("max_connections must be greater than zero"));
        }
        if asynchronous && self.max_handler_tasks == 0 {
            return Err(invalid_config(
                "max_handler_tasks must be greater than zero",
            ));
        }
        if self.max_outbound_bytes == 0 {
            return Err(invalid_config(
                "max_outbound_bytes must be greater than zero",
            ));
        }
        if self.receive_buffer_bytes == 0 {
            return Err(invalid_config(
                "receive_buffer_bytes must be greater than zero",
            ));
        }
        if self.receive_buffer_count == 0 {
            return Err(invalid_config(
                "receive_buffer_count must be greater than zero",
            ));
        }
        if u32::try_from(self.max_handler_tasks).is_err() {
            return Err(invalid_config("max_handler_tasks exceeds u32::MAX"));
        }
        Ok(self)
    }

    fn listener(self) -> listener::Config<Tcp> {
        listener::Config {
            max_connections: self.max_connections,
            bind: self.bind_addr,
            backlog: self.listen_backlog,
            stream: tcp::stream::Config {
                recv_buffer_size: self.socket_receive_buffer_bytes,
                send_buffer_size: self.socket_send_buffer_bytes,
                ..Default::default()
            },
            transport: tcp::listener::Config {
                reuse_port: true,
                fast_open_backlog: self.tcp_fast_open_backlog,
                per_ip_limit: Some(self.max_connections_per_ip),
                ..Default::default()
            },
            egress: Default::default(),
        }
    }
}

fn invalid_config(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn run<H, F>(
    handler: H,
    config: Config,
    asynchronous: bool,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
    launch: F,
) -> io::Result<()>
where
    H: 'static,
    F: for<'scope, 'd> FnOnce(
        *const H,
        Session<'scope, 'd, H>,
        listener::Config<Tcp>,
        Config,
    ) -> io::Result<()>,
{
    let config = config.validate(asynchronous)?;
    let listener = config.listener();
    let driver = dope::driver::Config::for_tcp_profile::<Throughput>(config.max_connections)
        .with_provided(config.receive_buffer_bytes, config.receive_buffer_count);
    Executor::with_seed(driver, context.seed())?
        .with_storage(handler)
        .enter(|mut session| {
            if let Some(trigger) = shutdown {
                trigger.try_register(&mut session.driver_access())?;
            }
            let handler = session.storage() as *const H;
            launch(handler, session, listener, config)
        })
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Server<'d, A, E>
where
    A: Application<'d>,
    E: ManifoldEnv<Wire = A::Wire>,
{
    #[pin]
    #[manifold]
    listener: Listener<'d, 0, A, E>,
}

macro_rules! launch {
    ($handler:expr, $session:ident, $listener:ident, $app:ty, $env:ty $(, $endpoint:expr)?) => {{
        let hash_builder = $session
            .seed()
            .derive(dope::hash::domain::ACCEPT)
            .state();
        let listener = {
            let mut driver = $session.driver_access();
            Listener::<0, $app, $env>::open_in(
                $handler,
                $listener,
                hash_builder,
                &mut driver,
            )?
        };
        $(let mut listener = listener;
        listener.set_config($endpoint);)?
        let app = core::pin::pin!(o3::cell::BrandCell::new(Server { listener }));
        $session.run(app.as_ref())
    }};
}

macro_rules! server {
    ($(#[$attr:meta])* $name:ident, [$($bound:tt)*], $asynchronous:literal, $app:ident, $wire:ty, $env:ty $(, tls $tls_config:ident: $tls_type:ty => $endpoint:expr)?) => {
        $(#[$attr])*
        pub fn $name<H>(
            handler: H,
            config: Config,
            $($tls_config: $tls_type,)?
            context: WorkerContext,
            shutdown: Option<&ShutdownTrigger>,
        ) -> io::Result<()>
        where
            H: $($bound)*,
        {
            run(
                handler,
                config,
                $asynchronous,
                context,
                shutdown,
                |handler, mut session, listener_config, config| {
                    launch!(
                        $app::new(unsafe { &*handler }, config),
                        session,
                        listener_config,
                        $app<H, $wire>,
                        $env
                        $(, $endpoint)?
                    )
                },
            )
        }
    };
}

server!(serve, [Handler], true, App, Identity, Env);

pub fn serve_async<H: Handler>(
    handler: H,
    config: Config,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()> {
    serve(handler, config, context, shutdown)
}

server!(
    serve_sync,
    [Fn(Request) -> Response + 'static],
    false,
    SyncApp,
    Identity,
    Env
);
server!(
    serve_tls,
    [Handler],
    true,
    App,
    Tls,
    TlsEnv,
    tls tls_config: shin::server::Config => Endpoint::Server(Box::new(tls_config))
);
server!(
    serve_tls_sync,
    [Fn(Request) -> Response + 'static],
    false,
    SyncApp,
    Tls,
    TlsEnv,
    tls tls_config: shin::server::Config => Endpoint::Server(Box::new(tls_config))
);

#[cfg(feature = "rustls")]
pub type RustlsTlsEnv = Bundle<Tcp, dope_tls::rustls::RustTls, Throughput>;

server!(
    #[cfg(feature = "rustls")]
    serve_tls_rustls,
    [Handler],
    true,
    App,
    dope_tls::rustls::RustTls,
    RustlsTlsEnv,
    tls tls_config: std::sync::Arc<rustls::ServerConfig> => dope_tls::rustls::RustTlsEndpoint::Server(tls_config)
);
server!(
    #[cfg(feature = "rustls")]
    serve_tls_rustls_sync,
    [Fn(Request) -> Response + 'static],
    false,
    SyncApp,
    dope_tls::rustls::RustTls,
    RustlsTlsEnv,
    tls tls_config: std::sync::Arc<rustls::ServerConfig> => dope_tls::rustls::RustTlsEndpoint::Server(tls_config)
);
