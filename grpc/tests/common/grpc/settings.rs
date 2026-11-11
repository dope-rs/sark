use std::{convert, net};

use dope::{
    core::{
        BranchError,
        driver::{
            life::recovery,
            route::{FrameworkTag, Identity, KeyTag, Tag, kind, table::conn},
            settings::{self, errors},
        },
        io::{
            Reclaimed, socket,
            socket::{Listener, option},
        },
    },
    manifold::{
        self,
        app::env::quota,
        network::listener::{config::Flights, handler::failures},
        timing,
    },
    net::{tcp, wire},
    runtime::{
        executor,
        executor::session::Completion,
        topology::{self, error, lending},
    },
};
use dope_test::reclaim::{self, Reports};
use o3::alloc;
use sark_grpc::server::{Bank, Plan, config::Limits, mode, service::handler::Handler};
use sark_h2::{
    conn::config,
    server,
    server::{runtime, runtime::modes},
};

const H2: u8 = 0;

#[derive(Debug)]
pub enum Error {
    Bank(alloc::Error),
    Config(server::ConfigError),
    Layout(errors::Claim),
    Reclaim(reclaim::Error),
}

type Run<'a> = error::Run<
    'a,
    convert::Infallible,
    manifold::BuildError<BranchError<convert::Infallible, config::Error>>,
    manifold::network::listener::ExecutionError,
>;

/// Test-only H2 transport settings paired with gRPC semantic limits.
#[derive(Clone)]
pub struct Settings {
    pub bind: net::SocketAddr,
    pub connections: conn::Capacity,
    pub backlog: socket::Backlog,
    pub limits: Limits,
}

impl Settings {
    pub fn new(bind: net::SocketAddr, limits: Limits) -> Self {
        Self {
            bind,
            connections: conn::Capacity::fixed::<4>(),
            backlog: socket::Backlog::fixed::<16>(),
            limits,
        }
    }

    pub fn h2<ID: Identity>(&self) -> Result<runtime::Plan<ID>, server::ConfigError> {
        use dope::manifold::network::listener::config;

        let input = self.limits.input();
        runtime::Plan::new(
            config::Prepared {
                bind: self.bind,
                connections: self.connections,
                per_ip: Some(quota::PerIp::from(self.connections.count())),
                direct_flights: Flights::Minimum,
                backlog: self.backlog,
                stream: tcp::StreamConfig {
                    no_delay: Some(option::Switch::On),
                    ..Default::default()
                },
                transport: tcp::Config::for_tcp(Some(socket::Reuse::Port), None),
                egress: Default::default(),
            },
            settings::Receive::fixed::<4, { 1 << 20 }>(),
            server::Config {
                max_request_body_bytes: input.stream_bytes(),
                max_request_body_chunks: input.segments(),
                max_connection_body_bytes: input.bytes(),
                max_connection_body_chunks: input.segments(),
                max_outbound_bytes: self.limits.output().wire_bytes(),
            },
        )
    }

    pub fn try_serve_on<H: Handler, C: executor::Build>(
        self,
        handler: H,
        context: C,
        listener: Listener,
        recover: impl recovery::Recover,
        done: impl for<'a> FnOnce(Result<(), Run<'a>>),
    ) -> Result<Vec<Reclaimed>, Error>
    where
        for<'a> Plan<'a, H, mode::Direct>:
            modes::Direct<modes::Serve<convert::Infallible>, wire::Identity, KeyTag<H2>>,
    {
        self.try_report(handler, context, listener, failures::Close, recover, done)
    }

    pub fn try_report<H: Handler, C: executor::Build, R>(
        self,
        handler: H,
        context: C,
        listener: Listener,
        report: R,
        recover: impl recovery::Recover,
        done: impl for<'a> FnOnce(Result<(), Run<'a>>),
    ) -> Result<Vec<Reclaimed>, Error>
    where
        for<'a> Plan<'a, H, mode::Direct, R>:
            modes::Direct<modes::Serve<convert::Infallible>, wire::Identity, KeyTag<H2>>,
    {
        let bank = Bank::try_new(self.connections.count(), self.limits).map_err(Error::Bank)?;
        let prepared = self
            .h2::<KeyTag<H2>>()
            .map_err(Error::Config)?
            .with_bound(listener)
            .with_plain::<_, modes::Serve<convert::Infallible>>(
                Plan::new(handler, &bank, mode::Direct).report(report),
            )
            .for_install::<timing::Throughput>()
            .map_err(Error::Layout)?;
        let mut reports =
            Reports::<_>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
        let mut finish = Completion::new();
        prepared.enter_with(
            context,
            |worker| {
                worker
                    .with(lending::Clean(&mut finish))
                    .map_err(topology::Failure::Install)?
                    .map_err(|error| error.map_install(|never| match never {}))
            },
            recovery::Policy::new(|report| reports.push(report), recover),
            |result| {
                let result = match result {
                    Ok(result) => result.map_err(topology::RunError::Failure),
                    Err(error) => Err(topology::RunError::from(error)),
                };
                done(result.as_ref().copied().map_err(topology::RunError::cause));
            },
        );
        reports.into_events().map_err(Error::Reclaim)
    }
}
