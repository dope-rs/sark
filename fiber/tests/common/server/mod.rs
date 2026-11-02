use std::{net, time};

use dope::{
    core::{
        driver::{
            life::recovery::Recovery,
            route::{FrameworkTag, Identity, KeyTag, Tag, kind, table::conn},
            settings::{self, claim::Needs, errors},
        },
        io::socket::{
            self,
            option::{Buffer, Switch},
        },
    },
    manifold::network::listener::config,
    net::tcp,
};
use dope_test::{harness::observe::VecCellExt as _, reclaim::Reports};
use sark_h1::server::{listener, run};

pub fn recover(failure: Recovery<'_, '_>) -> std::convert::Infallible {
    panic!("native recovery: {:?}", failure.cause())
}

/// Maximum connection count shared by deterministic server scenarios.
pub const MAX_CONNECTIONS: usize = 16;

/// Listener identifier used by the HTTP test topology.
pub const HTTP_LISTENER_ID: u8 = 0;

/// Date-updater identifier used by the HTTP test topology.
pub const DATE_UPDATER_ID: u8 = 1;

/// Canonical deterministic Sark server topology.
pub struct Plan {
    bind: net::SocketAddr,
    head_timeout: time::Duration,
    buffers: Option<Buffer>,
}

impl Plan {
    /// Observes retirement against this server's resolved descriptor allocation.
    pub fn try_reports(plan: &impl settings::Resource) -> Result<Reports<1>, errors::Claim> {
        Ok(Reports::new(
            Needs::from_tree(plan)?.resolve::<sark_fiber::Throughput>()?,
            [<FrameworkTag<kind::Any> as Tag>::ROUTE],
        ))
    }
    /// Defines a server bound to `bind` with the given request-head deadline.
    #[must_use]
    pub const fn new(bind: net::SocketAddr, timeout: time::Duration) -> Self {
        Self {
            bind,
            head_timeout: timeout,
            buffers: None,
        }
    }

    pub fn with_buffers(mut self, buffers: Buffer) -> Self {
        self.buffers = Some(buffers);
        self
    }

    /// Materializes wire-independent HTTP/1 listener settings.
    pub fn build(
        self,
    ) -> run::Plan<KeyTag<HTTP_LISTENER_ID>, KeyTag<DATE_UPDATER_ID>, sark_fiber::Throughput> {
        let mut config = listener(self.bind);
        if let Some(buffers) = self.buffers {
            config.stream.recv_buffer_size = Some(buffers);
            config.stream.send_buffer_size = Some(buffers);
        }
        listener::Config::new(config, self.head_timeout).plan()
    }
}

fn listener(bind: net::SocketAddr) -> config::Prepared<sark_fiber::Tcp> {
    config::Prepared::<sark_fiber::Tcp> {
        bind,
        connections: conn::Capacity::fixed::<MAX_CONNECTIONS>(),
        per_ip: None,
        direct_flights: MAX_CONNECTIONS.into(),
        backlog: socket::Backlog::fixed::<16>(),
        stream: tcp::StreamConfig {
            no_delay: Some(Switch::On),
            ..Default::default()
        },
        transport: tcp::Config::for_tcp(Some(socket::Reuse::Port), None),
        egress: Default::default(),
    }
}

pub mod grpc;
pub mod h3;

use std::cell::Cell;

use dope::{manifold::network::listener::conn::Id, net::wire};
use dope_tls::tls::{self, roles};
use sark_h1::server::listener::failure;
use wire::{open, terminal};

/// Test server state permitting clients to disconnect without close_notify.
pub struct State<'a>(pub &'a Cell<Vec<Failure>>);

#[derive(Debug)]
pub enum Failure {
    Open(dope_tls::Error),
    Wire {
        connection: u32,
        cause: dope_tls::Error,
        closing: Option<dope_tls::Error>,
    },
}

impl failure::Policy<tls::Tls<roles::Server>> for State<'_> {
    fn open_failed<'w>(&self, cause: open::Error<'w, tls::Tls<roles::Server>>)
    where
        tls::Tls<roles::Server>: 'w,
    {
        self.0.push(Failure::Open(cause.into_cause()));
    }

    fn wire_failed<'w, 'd: 'w, ID: Identity>(
        &self,
        conn: Id<'d, ID>,
        cause: terminal::Error<'w, tls::Tls<roles::Server>>,
    ) {
        let (cause, closing) = cause.into_parts();
        if !matches!(&cause, dope_tls::Error::Io(error) if error.kind() == std::io::ErrorKind::UnexpectedEof)
            || closing.is_some()
        {
            self.0.push(Failure::Wire {
                connection: conn.index().get(),
                cause,
                closing,
            });
        }
    }
}

pub mod peer;
