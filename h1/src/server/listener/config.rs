use std::{marker, net::SocketAddr, time};

use dope::{
    core::{
        driver::{
            route::{Identity, table::conn},
            settings,
        },
        io::socket::{self, option::Switch},
    },
    manifold::{network::listener::config, timing},
    net::tcp,
};
use sark_core::life::session;

use crate::server::{deadline::policies, run::Plan};

/// HTTP/1 listener settings independent of the selected wire.
#[must_use]
pub struct Config<ListenerId: Identity, P> {
    pub(in crate::server) listener: config::Prepared<tcp::Tcp>,
    pub(super) head_timeout: time::Duration,
    pub(super) session_lifetime: Option<session::Config>,
    pub(super) profile: marker::PhantomData<(P, ListenerId)>,
}

impl<ListenerId: Identity, P> Config<ListenerId, P>
where
    P: timing::Policy + settings::Profile,
{
    pub const fn new(listener: config::Prepared<tcp::Tcp>, head: time::Duration) -> Self {
        Self {
            listener,
            head_timeout: head,
            session_lifetime: None,
            profile: marker::PhantomData,
        }
    }

    /// Creates a TCP listener with one direct send per validated connection.
    /// Uses TCP_NODELAY, SO_REUSEPORT, backlog 1024, and the environment's per-IP default.
    /// Other socket and egress settings use defaults; use [`Self::new`] to customize.
    pub fn tcp(bind: SocketAddr, capacity: conn::Capacity, head: time::Duration) -> Self {
        Self::new(
            config::Prepared {
                bind,
                connections: capacity,
                per_ip: None,
                direct_flights: config::Flights::Exact(capacity.nonzero().get()),
                backlog: socket::Backlog::fixed::<1024>(),
                stream: tcp::StreamConfig {
                    no_delay: Some(Switch::On),
                    ..Default::default()
                },
                transport: tcp::Config::for_tcp(Some(socket::Reuse::Port), None),
                egress: Default::default(),
            },
            head,
        )
    }

    /// Overrides the profile's connection age with a validated maximum age and drain window.
    pub fn with_age(mut self, config: session::Config) -> Self {
        self.session_lifetime = Some(config);
        self
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        config::Prepared<tcp::Tcp>,
        time::Duration,
        Option<session::Config>,
    ) {
        (self.listener, self.head_timeout, self.session_lifetime)
    }

    /// Uses the configured window for the complete request input, without renewal on progress.
    pub fn plan<DateId: Identity>(self) -> Plan<ListenerId, DateId, P> {
        Plan::new(self, policies::Input)
    }
}

impl<ListenerId: Identity, P> Clone for Config<ListenerId, P> {
    fn clone(&self) -> Self {
        Self {
            listener: self.listener,
            head_timeout: self.head_timeout,
            session_lifetime: self.session_lifetime,
            profile: marker::PhantomData,
        }
    }
}
