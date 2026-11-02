use std::{cell::Cell, net::SocketAddr};

use dope::{
    core::{
        driver::{
            route::{KeyTag, table::conn},
            settings,
        },
        io::{socket, socket::option::Switch},
    },
    manifold::{app::env::quota, network::listener::config::Prepared},
    net::tcp,
};
use o3::buffer::resident::bound;
use sark_h2::server::{
    self,
    runtime::Plan,
    service::{Handler, Request},
};
use sark_http::payload::{
    fixed,
    response::{headers::Entry, statics, status::Code},
};

use crate::common::headers::TEXT;

pub const ROUTE: u8 = 7;

pub fn listener() -> Prepared<tcp::Tcp> {
    Prepared {
        bind: SocketAddr::from(([127, 0, 0, 1], 0)),
        connections: conn::Capacity::fixed::<1>(),
        per_ip: Some(quota::PerIp::fixed::<1>()),
        direct_flights: Default::default(),
        backlog: socket::Backlog::fixed::<1>(),
        stream: tcp::StreamConfig {
            no_delay: Some(Switch::On),
            ..Default::default()
        },
        transport: tcp::Config::for_tcp(Some(socket::Reuse::Port), None),
        egress: Default::default(),
    }
}

pub const fn config() -> server::Config {
    server::Config {
        max_request_body_bytes: 0,
        max_request_body_chunks: 0,
        max_connection_body_bytes: 0,
        max_connection_body_chunks: 0,
        max_outbound_bytes: 1,
    }
}

pub struct Lexical<'a>(pub &'a Cell<usize>);

pub trait Reply: Sized {
    fn reply(request: Request<'_, '_, '_>) -> Self;
}

impl Reply for statics::Direct<'_, [Entry<'_>; 0]> {
    fn reply(request: Request<'_, '_, '_>) -> Self {
        let body = match request.path() {
            Some(b"/") => fixed::Body::new(b"named"),
            _ => fixed::Body::new(b"missing"),
        };
        Self::new(Code::OK, [], body)
    }
}

impl Lexical<'_> {
    pub fn try_plan() -> Result<Plan<KeyTag<ROUTE>>, server::ConfigError> {
        Plan::new(
            listener(),
            settings::Receive::fixed::<2, 4096>(),
            server::Config {
                max_request_body_bytes: 1024,
                max_request_body_chunks: 8,
                max_connection_body_bytes: 1024,
                max_connection_body_chunks: 8,
                max_outbound_bytes: 4096,
            },
        )
    }
}

impl Handler for Lexical<'_> {
    type Egress<'d> = bound::Unbounded;
    type Output<'req, 'd>
        = statics::Direct<'d, [Entry<'req>; 1]>
    where
        Self: 'req,
        'd: 'req;

    fn handle<'req, 'd, 'id>(&'req self, _: Request<'req, 'd, 'id>) -> Self::Output<'req, 'd>
    where
        'd: 'req,
    {
        self.0.set(self.0.get() + 1);
        statics::Direct::new(Code::OK, [TEXT], fixed::Body::new(b""))
    }
}
