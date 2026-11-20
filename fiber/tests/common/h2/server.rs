use std::{convert::Infallible, time::Duration};

use dope::{
    core::{
        BranchError,
        driver::{
            route::{FrameworkTag, KeyTag, Tag, kind},
            settings::{self},
        },
        io::{
            Reclaimed,
            socket::{self, Listener, option::Switch},
        },
    },
    manifold::{
        app::env::quota::PerIp,
        network::listener::{config::Prepared, conn, handler},
        timing,
    },
    net::{tcp, wire},
    runtime::{
        executor::session,
        shutdown,
        topology::{self, lending},
    },
};
use dope_fiber::task::{storage::scope::Brand, time::sleep};
use dope_test::{
    harness::{scenario::run, server::Pair},
    reclaim,
};
use sark_core::{deadline::config::Config, life, life::mode, manifold::stream::Boundary};
use sark_h2::{
    ClientRole,
    conn::{config, machine::Connection},
    server::{self, runtime::Plan, task::timers},
    stream,
};
use sark_http::field::{self, Lower, Value};

use crate::common::{
    h2::{apps::*, facts::*, peer::Peer, *},
    server::reporting,
};

pub const CONFIG: server::Config = server::Config {
    max_request_body_bytes: 4 << 20,
    max_request_body_chunks: 4 << 10,
    max_connection_body_bytes: 8 << 20,
    max_connection_body_chunks: 16 << 10,
    max_outbound_bytes: 64 << 10,
};

pub fn check(
    config: server::Config,
    task_cap: usize,
    client: impl FnOnce(&mut Peer<'_>),
) -> Result<(), run::Error<Infallible>> {
    Pair::bind().expect("H2 harness").run(
        move |context, listener| {
            Server::new(config, task_cap, State::default())
                .try_run(context, listener, mode::Disabled)
                .expect("H2 setup");
            Ok(())
        },
        |address| {
            let (mut socket, mut connection) = Peer::connect(address);
            client(&mut Peer::new(&mut socket, &mut connection));
        },
    )
}

pub struct Server {
    config: server::Config,
    protocol: Option<config::Config>,
    task_cap: usize,
    lifetime: Option<handler::Lifetime>,
    pub state: State,
}

impl Server {
    pub const fn new(config: server::Config, task_cap: usize, state: State) -> Self {
        Self {
            config,
            protocol: None,
            task_cap,
            lifetime: None,
            state,
        }
    }

    pub const fn with_life(mut self, lifetime: handler::Lifetime) -> Self {
        self.lifetime = Some(lifetime);
        self
    }

    pub const fn with_config(mut self, protocol: config::Config) -> Self {
        self.protocol = Some(protocol);
        self
    }

    pub fn try_plan(&self, bind: SocketAddr) -> Result<Plan<KeyTag<ROUTE>>, server::ConfigError> {
        use dope::core::driver::route::table::conn;
        let plan = Plan::new(
            Prepared {
                bind,
                connections: conn::Capacity::fixed::<8>(),
                per_ip: Some(PerIp::fixed::<8>()),
                direct_flights: Default::default(),
                backlog: socket::Backlog::fixed::<32>(),
                stream: tcp::StreamConfig {
                    no_delay: Some(Switch::On),
                    ..Default::default()
                },
                transport: tcp::Config::for_tcp(Some(socket::Reuse::Port), None),
                egress: Default::default(),
            },
            settings::Receive::fixed::<64, { 32 << 10 }>(),
            self.config,
        )?;
        let plan = match self.protocol {
            Some(protocol) => plan.with_config(protocol),
            None => plan,
        };
        Ok(match self.lifetime {
            Some(lifetime) => plan.with_life(lifetime),
            None => plan,
        })
    }
}

pub trait RunMode {
    fn run<'report, 'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e>(
        self,
        plan: Plan<KeyTag<ROUTE>>,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, (), shutdown::ReadySource>,
        state: &State,
        task_cap: usize,
        finish: &'report mut session::Completion,
    ) -> Result<(), topology::Failure<'report, Install, ()>>;
}

impl RunMode for mode::Disabled {
    fn run<'report, 'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e>(
        self,
        plan: Plan<KeyTag<ROUTE>>,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, (), shutdown::ReadySource>,
        state: &State,
        task_cap: usize,
        finish: &'report mut session::Completion,
    ) -> Result<(), topology::Failure<'report, Install, ()>> {
        let mode::Disabled = self;
        Brand::with(|brand| {
            let fiber = sark_fiber::Config::new(task_cap);
            let router =
                H2TaskApp::multiplexed::<wire::Identity, KeyTag<ROUTE>, _>(brand, state, fiber);
            state.layout.set(Some(router.layout()));
            let wire = wire::domain::Local::new(wire::Identity);
            plan.tasks(router, task_cap)
                .try_install::<wire::Identity, _, _, _, _, _, _>(stage, wire, timers::Disabled, |listener, _wait, stage| {
                    let result = stage
                        .run_quiesce(finish, Host::new(listener), Duration::from_secs(2))
                        .map_err(topology::Failure::Run)?;
                    result.map_err(|error| topology::Failure::Run(session::Error::Drive(error)))
                })
                .map_err(topology::Failure::Install)?
        })
    }
}

impl RunMode for Config {
    fn run<'report, 'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e>(
        self,
        plan: Plan<KeyTag<ROUTE>>,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, (), shutdown::ReadySource>,
        state: &State,
        task_cap: usize,
        finish: &'report mut session::Completion,
    ) -> Result<(), topology::Failure<'report, Install, ()>> {
        Brand::with(|brand| {
            let fiber = sark_fiber::Config::new(task_cap);
            let router =
                H2TaskApp::multiplexed::<wire::Identity, KeyTag<ROUTE>, _>(brand, state, fiber);
            state.layout.set(Some(router.layout()));
            let wire = wire::domain::Local::new(wire::Identity);
            plan.tasks(router, task_cap)
                .try_install::<wire::Identity, _, _, _, _, _, _>(stage, wire, self, |listener, _wait, stage| {
                    let result = stage
                        .run_quiesce(finish, Host::new(listener), Duration::from_secs(2))
                        .map_err(topology::Failure::Run)?;
                    result.map_err(|error| topology::Failure::Run(session::Error::Drive(error)))
                })
                .map_err(topology::Failure::Install)?
        })
    }
}

impl<O> RunMode for mode::Observed<O>
where
    O: for<'d> life::Observer<conn::Id<'d, KeyTag<ROUTE>>, stream::Key>,
{
    fn run<'report, 'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e>(
        self,
        plan: Plan<KeyTag<ROUTE>>,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, (), shutdown::ReadySource>,
        state: &State,
        task_cap: usize,
        finish: &'report mut session::Completion,
    ) -> Result<(), topology::Failure<'report, Install, ()>> {
        Brand::with(|brand| {
            let fiber = sark_fiber::Config::new(task_cap);
            let router =
                H2TaskApp::multiplexed::<wire::Identity, KeyTag<ROUTE>, _>(brand, state, fiber);
            state.layout.set(Some(router.layout()));
            let wire = wire::domain::Local::new(wire::Identity);
            plan.tasks(router, task_cap)
                .observe(self.into_inner())
                .try_install::<wire::Identity, _, _, _, _, _, _>(stage, wire, timers::Disabled, |listener, _wait, stage| {
                    let result = stage
                        .run_quiesce(finish, Host::new(listener), Duration::from_secs(2))
                        .map_err(topology::Failure::Run)?;
                    result.map_err(|error| topology::Failure::Run(session::Error::Drive(error)))
                })
                .map_err(topology::Failure::Install)?
        })
    }
}

impl Server {
    pub fn try_run(
        &self,
        context: shutdown::ReadySource,
        listener: Listener,
        mode: impl RunMode,
    ) -> Result<(), Error> {
        let prepared = topology::Plan::new(
            (),
            self.try_plan(listener.local_addr())
                .map_err(Error::Config)?
                .with_bound(listener),
        )
        .for_receive::<timing::Throughput>()
        .map_err(Error::Layout)?;
        let Self { state, task_cap, .. } = self;
        let mut reports = reclaim::Reports::<1>::new(
            prepared.layout(),
            [<FrameworkTag<kind::Any> as Tag>::ROUTE],
        );
        let mut finish = session::Completion::new();
        let finish = &mut finish;
        prepared.enter_with(
            context,
            move |scope| {
                scope.lend(|plan, stage| mode.run(plan, stage, state, *task_cap, finish))
            },
            reporting(&mut reports),
            |result| {
                let result = match result {
                    Ok(result) => result.map_err(topology::RunError::Failure),
                    Err(error) => Err(topology::RunError::from(error)),
                };
                result
                    .as_ref()
                    .map_err(topology::RunError::cause)
                    .expect("H2 execution");
            },
        );
        let (invalid, timer, layout) = state.observed();
        if invalid || timer.is_some() || layout != Some(DUPLEX_LAYOUT) {
            return Err(Error::Handler {
                invalid,
                timer,
                layout,
            });
        }
        let events = reports.into_events().map_err(Error::Reclaim)?;
        if !events.is_empty() {
            return Err(Error::Retired(events));
        }
        Ok(())
    }
}

pub struct Request<'a> {
    path: &'a [u8],
}

impl<'a> Request<'a> {
    pub const fn new(path: &'a [u8]) -> Self {
        Self { path }
    }

    pub fn try_start(
        self,
        client: &mut Connection<ClientRole>,
        boundary: Boundary,
    ) -> Result<stream::Id, BranchError<field::Error, sark_h2::conn::Error>> {
        let fields = self.try_headers().map_err(BranchError::First)?;
        client
            .streams()
            .headers()
            .request(fields, boundary)
            .map_err(BranchError::Second)
    }

    pub fn try_headers(self) -> Result<[Entry<'a>; 4], field::Error> {
        Ok([
            Entry::pseudo(Pseudo::Method, field::literal!(Value::try_bytes(b"POST"))),
            Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"http"))),
            Entry::pseudo(Pseudo::Path, Value::try_bytes(self.path)?),
            Entry::pseudo(
                Pseudo::Authority,
                field::literal!(Value::try_bytes(b"localhost")),
            ),
        ])
    }

    pub fn try_length(self, length: &'a [u8]) -> Result<[Entry<'a>; 5], field::Error> {
        let [method, scheme, path, host] = self.try_headers()?;
        Ok([
            method,
            scheme,
            path,
            host,
            Entry::new(
                field::literal!(Lower::try_bytes(b"content-length")),
                Value::try_bytes(length)?,
            ),
        ])
    }

    pub fn try_named(self, name: &'a [u8]) -> Result<[Entry<'a>; 5], field::Error> {
        let [method, scheme, path, host] = self.try_headers()?;
        Ok([
            method,
            scheme,
            path,
            host,
            Entry::new(
                field::literal!(Lower::try_bytes(b"x-name")),
                Value::try_bytes(name)?,
            ),
        ])
    }
}

pub const CONNECT: [Entry<'static>; 3] = [
    Entry::pseudo(
        Pseudo::Method,
        field::literal!(Value::try_bytes(b"CONNECT")),
    ),
    Entry::pseudo(
        Pseudo::Authority,
        field::literal!(Value::try_bytes(b"duplex.example:443")),
    ),
    Entry::new(
        field::literal!(Lower::try_bytes(b"x-duplex-header")),
        field::literal!(Value::try_bytes(b"retained-after-pseudo-fields")),
    ),
];

pub const EXTENDED: [Entry<'static>; 6] = [
    Entry::pseudo(
        Pseudo::Method,
        field::literal!(Value::try_bytes(b"CONNECT")),
    ),
    Entry::pseudo(
        Pseudo::Protocol,
        field::literal!(Value::try_bytes(b"sark-echo")),
    ),
    Entry::pseudo(Pseudo::Scheme, field::literal!(Value::try_bytes(b"http"))),
    Entry::pseudo(
        Pseudo::Authority,
        field::literal!(Value::try_bytes(b"duplex.example")),
    ),
    Entry::pseudo(Pseudo::Path, field::literal!(Value::try_bytes(b"/duplex"))),
    Entry::new(
        field::literal!(Lower::try_bytes(b"x-duplex-header")),
        field::literal!(Value::try_bytes(b"retained-after-pseudo-fields")),
    ),
];

use dope::{core::driver::settings::errors, manifold};
use dope_fiber::task::storage::fixed::schedule::queue::PrepareError;
use sark_h2::server::runtime::startup::Failure;

#[derive(Debug)]
pub enum Error {
    Config(server::ConfigError),
    Layout(errors::Claim),
    Handler {
        invalid: bool,
        timer: Option<sleep::Error>,
        layout: Option<(usize, usize)>,
    },
    Reclaim(reclaim::Error),
    Retired(Vec<Reclaimed>),
}

type Install = manifold::BuildError<Failure<PrepareError>>;
