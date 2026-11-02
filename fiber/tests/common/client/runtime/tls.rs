use std::{cell::Cell, net::SocketAddr, rc::Rc, time};

use dope::{
    core::{
        driver,
        driver::{
            life::recovery,
            route::{KeyTag, table, table::conn::Capacity},
            settings, storage,
        },
    },
    manifold::{
        app::{Host, env},
        client::{attempt, session::policy::start},
        network::client,
        service,
        service::{health, sync},
        timing,
    },
    net,
    net::{
        tcp,
        wire::{domain, failure, open},
    },
    runtime::{executor, executor::session, topology, topology::lending},
};
use dope_test::{
    fibers,
    harness::{GUARD, observe::VecCellExt as _},
};
use dope_tls::{
    tls,
    tls::{endpoint, roles},
};
use sark_fiber::client::{Ready, channel, config, config::report, conn::Connector, request};
use settings::errors;

use crate::common::client::runtime::{self, Failure, WaitError};

type Network = env::Bundle<tcp::Tcp, tls::Tls<roles::Client>, timing::Balanced>;

type Owner<'s, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'w, 'd> = Host<
    client::Pool<
        'm,
        'h,
        'n,
        'w,
        'd,
        KeyTag<0>,
        MAX_CONNECTIONS,
        Connector<
            's,
            KeyTag<0>,
            channel::Port<
                'f,
                'e,
                'b,
                'c,
                'w,
                'd,
                KeyTag<0>,
                tls::Tls<roles::Client>,
                report::Retain,
            >,
            OpenFailures,
        >,
        sync::Preserve,
        SocketAddr,
        Network,
    >,
>;

type Handle<'app, 's, 'f, 'e, 'b, 'c, 'w, 'd> = request::Handle<
    'app,
    'f,
    'e,
    'b,
    'c,
    'w,
    'd,
    Connector<
        's,
        KeyTag<0>,
        channel::Port<'f, 'e, 'b, 'c, 'w, 'd, KeyTag<0>, tls::Tls<roles::Client>, report::Retain>,
        OpenFailures,
    >,
    KeyTag<0>,
    tls::Tls<roles::Client>,
    report::Retain,
>;

const MAX_CONNECTIONS: usize = u32::MAX as usize;

const BACKOFF: (time::Duration, time::Duration) = (
    time::Duration::from_millis(1),
    time::Duration::from_millis(10),
);

#[derive(Clone)]
pub struct OpenFailures(Rc<Cell<Vec<dope_tls::Error>>>);

impl OpenFailures {
    fn new() -> Self {
        Self(Rc::new(Cell::new(Vec::new())))
    }

    fn finish<R>(self, value: R) -> Result<R, Error<R>> {
        let failures = match Rc::try_unwrap(self.0) {
            Ok(failures) => failures.into_inner(),
            Err(shared) => {
                return Err(Error::Shared {
                    value,
                    errors: shared.take(),
                });
            }
        };
        if failures.is_empty() {
            Ok(value)
        } else {
            Err(Error::Open {
                value,
                errors: failures,
            })
        }
    }
}

impl<'w, 'd: 'w> start::Sink<'w, 'd, tls::Tls<roles::Client>, KeyTag<0>, failure::Possible>
    for OpenFailures
{
    fn fail(
        &self,
        _: attempt::Completion<'_, 'd>,
        cause: open::Error<'w, tls::Tls<roles::Client>>,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.0.push(cause.into_cause());
    }
}

#[derive(Debug)]
pub enum Build {
    Config(config::Error),
    Capacity(table::Invalid),
    Wire(net::Resource),
}

#[derive(Debug)]
pub enum Error<R> {
    Plan(config::Error),
    Claim(errors::Claim),
    Open {
        value: R,
        errors: Vec<dope_tls::Error>,
    },
    Shared {
        value: R,
        errors: Vec<dope_tls::Error>,
    },
}

pub struct Plan {
    http: channel::Plan<KeyTag<0>, tls::Tls<roles::Client>, report::Retain>,
    tls: domain::Owner<tls::Tls<roles::Client>, KeyTag<0>>,
    address: SocketAddr,
    endpoint: endpoint::Config<roles::Client>,
}

impl Plan {
    pub fn new(
        address: SocketAddr,
        config: config::Config,
        endpoint: endpoint::Config<roles::Client>,
        capacity: usize,
        timers: usize,
        target: usize,
    ) -> Result<Self, Build> {
        let http = channel::Plan::new(
            config,
            channel::Topology::new(capacity, timers).with_target(target),
        )
        .map_err(Build::Config)?;
        let capacity = Capacity::try_from(target).map_err(Build::Capacity)?;
        let tls = domain::Owner::try_new(capacity).map_err(Build::Wire)?;
        Ok(Self {
            http,
            tls,
            address,
            endpoint,
        })
    }

    pub fn enter<R, Out>(
        self,
        run: impl for<'app, 'life, 's, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'w, 'd> FnOnce(
            Scope<'app, 'life, 's, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'w, 'd>,
        ) -> R,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<R, runtime::Error<'a>>) -> Out,
    ) -> Result<Out, Error<Out>> {
        let mut finish = session::Completion::new();
        let finish = &mut finish;
        let Self {
            http,
            tls,
            address,
            endpoint,
        } = self;
        let plan = http.plan(BACKOFF.0, BACKOFF.1).map_err(Error::Plan)?;
        let prepared = topology::Plan::new(storage::Value::new(tls), (http, plan))
            .for_install::<timing::Balanced>()
            .map_err(Error::Claim)?;
        let failures = OpenFailures::new();
        let consumer = failures.clone();
        let value = prepared.with_local(
            move |worker| {
                worker.lend(|(http, plan), stage| {
                    http.try_with(stage, lending::Stage::driver, |port, stage| {
                        let hash = stage.hash_state(health::Domain::DEFAULT);
                        let snapshot = service::Snapshot::from_one(
                            service::Revision::new(1),
                            service::Endpoint::new(address, address),
                        );
                        let tls = stage.storage();
                        let wire = match tls.bind(endpoint) {
                            Ok(wire) => wire,
                            Err(never) => match never {},
                        };
                        let setup = port.bind_with(consumer);
                        let plan = plan.bind(wire, hash);
                        plan.install::<_, _, sync::Preserve, _, Network, 16, _, _>(
                            setup,
                            snapshot,
                            stage,
                            lending::Stage::driver,
                            |pool, stage| {
                                Ok::<_, Failure>(stage.with_quiesce(
                                    finish,
                                    Host::new(pool),
                                    GUARD,
                                    |mut app| {
                                        let handle = app.root_client();
                                        run(Scope { app, handle })
                                    },
                                ))
                            },
                        )
                        .map_err(Failure::Connector)?
                    })
                    .map_err(Failure::Port)?
                })
            },
            policy,
            |result| runtime::finish(result, done),
        );
        failures.finish(value)
    }
}

/// Live HTTPS client bound to one application and TLS-storage lifetime.
pub struct Scope<'app, 'life: 'app, 's, 'm, 'h, 'n, 'f: 'b, 'e, 'b, 'c, 'w, 'd: 'life + 'f + 'w> {
    pub app: session::App<'app, 'life, 'd, Owner<'s, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'w, 'd>>,
    pub handle: Handle<'app, 's, 'f, 'e, 'b, 'c, 'w, 'd>,
}

impl<'app, 'life: 'app, 's, 'm, 'h, 'n, 'f: 'b, 'e, 'b, 'c, 'w, 'd: 'life + 'f + 'w>
    Scope<'app, 'life, 's, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'w, 'd>
where
    Owner<'s, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'w, 'd>: executor::raw::App<'d>,
{
    pub fn wait(&mut self) -> Result<(), WaitError> {
        let handle = self.handle;
        fibers::TEST
            .try_run(&mut self.app, handle.wait_active())
            .map_err(WaitError::Drive)?
            .map_err(WaitError::Connector)
    }
}
