#[cfg(feature = "rustls")]
use std::sync::Arc;
use std::{
    io::{self, Error},
    net::SocketAddr,
};

use dope::{
    manifold::{
        env::{self, Bundle},
        listener::{self, Listener, application::Application},
    },
    runtime::{
        executor::{Executor, Session},
        launcher::WorkerContext,
        profile::Throughput,
        trigger::ShutdownTrigger,
    },
};
use dope_net::{
    link::egress,
    tcp::{self, Tcp},
    wire::{Wire, identity::Identity},
};
#[cfg(feature = "rustls")]
use dope_tls::rustls::{RustTls, RustTlsEndpoint};
use dope_tls::tls::{Endpoint, SessionStorage, Tls};
use shin::server;

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

    fn listener(self) -> listener::config::Config<Tcp> {
        use dope_net::tcp::stream;
        listener::config::Config {
            max_connections: self.max_connections,
            bind: self.bind_addr,
            backlog: self.listen_backlog,
            stream: stream::Config {
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
    use std::io::ErrorKind;
    Error::new(ErrorKind::InvalidInput, message)
}

trait EgressHost {
    fn egress(&self) -> &egress::storage::Storage;
}

impl<T> EgressHost for (T, egress::storage::Storage, SessionStorage) {
    fn egress(&self) -> &egress::storage::Storage {
        &self.1
    }
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
        &'d H,
        Session<'scope, 'd, (H, egress::storage::Storage, SessionStorage)>,
        listener::config::Config<Tcp>,
        Config,
    ) -> io::Result<()>,
{
    let config = config.validate(asynchronous)?;
    let listener = config.listener();
    let driver = dope::driver::Config::for_tcp_profile::<Throughput>(config.max_connections)
        .with_recv(config.receive_buffer_bytes, config.receive_buffer_count);
    let tls_storage = SessionStorage::try_with_capacity(config.max_connections)?;
    Executor::with_seed(driver, context.seed())?
        .with_storage((handler, egress::storage::Storage::default(), tls_storage))
        .enter(|mut session| {
            if let Some(trigger) = shutdown {
                trigger.try_register(&mut session.driver_access())?;
            }
            let (handler, _, _) = session.storage();
            launch(handler, session, listener, config)
        })
}

#[pin_project::pin_project]
#[derive(dope_gen::Dispatcher)]
struct Server<'d, A, E>
where
    A: Application<'d>,
    E: env::Env<Wire = A::Wire>,
{
    #[pin]
    #[manifold]
    listener: Listener<'d, 'd, 0, A, E>,
}

fn start<'scope, 'd, A, E, S>(
    app: A,
    mut session: Session<'scope, 'd, S>,
    listener_config: listener::config::Config<Tcp>,
    wire_config: <A::Wire as Wire>::InitConfig<'d>,
) -> io::Result<()>
where
    A: Application<'d>,
    E: env::Env<Transport = Tcp, Wire = A::Wire>,
    S: EgressHost,
{
    use core::pin::pin;

    use dope::hash::domain::ACCEPT;
    let egress = session.storage().egress();
    let hash_builder = session.seed().derive(ACCEPT).state();
    let listener = {
        let mut driver = session.driver_access();
        Listener::<0, A, E>::open_in_with_wire(
            app,
            listener_config,
            wire_config,
            hash_builder,
            egress,
            &mut driver,
        )?
    };
    let server = pin!(o3::cell::BrandCell::new(Server { listener }));
    session.run(server.as_ref())
}

pub fn serve<H: Handler>(
    handler: H,
    config: Config,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()> {
    run(
        handler,
        config,
        true,
        context,
        shutdown,
        |handler, session, listener, config| {
            let app = App::new(handler, config)
                .map_err(|error| Error::new(io::ErrorKind::InvalidInput, error))?;
            start::<App<H>, Env, _>(app, session, listener, ())
        },
    )
}

pub fn serve_async<H: Handler>(
    handler: H,
    config: Config,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()> {
    serve(handler, config, context, shutdown)
}

pub fn serve_sync<H>(
    handler: H,
    config: Config,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()>
where
    H: Fn(Request) -> Response + 'static,
{
    run(
        handler,
        config,
        false,
        context,
        shutdown,
        |handler, session, listener, config| {
            let app = SyncApp::new(handler, config)
                .map_err(|error| Error::new(io::ErrorKind::InvalidInput, error))?;
            start::<SyncApp<H>, Env, _>(app, session, listener, ())
        },
    )
}

pub fn serve_tls<H: Handler>(
    handler: H,
    config: Config,
    tls_config: server::config::Config,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()> {
    let endpoint = Endpoint::server(tls_config).map_err(Error::other)?;
    run(
        handler,
        config,
        true,
        context,
        shutdown,
        move |handler, session, listener, config| {
            let app = App::new(handler, config)
                .map_err(|error| Error::new(io::ErrorKind::InvalidInput, error))?;
            let wire = endpoint.bind(&session.storage().2);
            start::<App<H, Tls>, TlsEnv, _>(app, session, listener, wire)
        },
    )
}

pub fn serve_tls_sync<H>(
    handler: H,
    config: Config,
    tls_config: server::config::Config,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()>
where
    H: Fn(Request) -> Response + 'static,
{
    let endpoint = Endpoint::server(tls_config).map_err(Error::other)?;
    run(
        handler,
        config,
        false,
        context,
        shutdown,
        move |handler, session, listener, config| {
            let app = SyncApp::new(handler, config)
                .map_err(|error| Error::new(io::ErrorKind::InvalidInput, error))?;
            let wire = endpoint.bind(&session.storage().2);
            start::<SyncApp<H, Tls>, TlsEnv, _>(app, session, listener, wire)
        },
    )
}

#[cfg(feature = "rustls")]
pub type RustlsTlsEnv = Bundle<Tcp, RustTls, Throughput>;

#[cfg(feature = "rustls")]
pub fn serve_tls_rustls<H: Handler>(
    handler: H,
    config: Config,
    tls_config: Arc<rustls::ServerConfig>,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()> {
    run(
        handler,
        config,
        true,
        context,
        shutdown,
        move |handler, session, listener, config| {
            let app = App::new(handler, config).map_err(Error::invalid_input)?;
            start::<App<H, RustTls>, RustlsTlsEnv, _>(
                app,
                session,
                listener,
                RustTlsEndpoint::Server(tls_config),
            )
        },
    )
}

#[cfg(feature = "rustls")]
pub fn serve_tls_rustls_sync<H>(
    handler: H,
    config: Config,
    tls_config: Arc<rustls::ServerConfig>,
    context: WorkerContext,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()>
where
    H: Fn(Request) -> Response + 'static,
{
    run(
        handler,
        config,
        false,
        context,
        shutdown,
        move |handler, session, listener, config| {
            let app = SyncApp::new(handler, config).map_err(Error::invalid_input)?;
            start::<SyncApp<H, RustTls>, RustlsTlsEnv, _>(
                app,
                session,
                listener,
                RustTlsEndpoint::Server(tls_config),
            )
        },
    )
}
