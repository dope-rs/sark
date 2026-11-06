use std::{net, time};

use dope::{
    core::driver::{life::recovery, route::KeyTag, schedule::clock},
    manifold::{
        app::{Host, env},
        network::client,
        service,
        service::{health, sync},
        timing,
    },
    net::{tcp, wire, wire::domain},
    runtime::{executor, executor::session, topology, topology::lending},
};
use dope_fiber::BlockOn;
use dope_test::{fibers, harness::GUARD};
use sark_client::protocol::response;
use sark_fiber::client::{Ready, Stream, channel, config, conn::Connector, request};

use crate::common::client::runtime::{Error, Failure, WaitError, cases::stream::Exchange};

type Network = env::Bundle<tcp::Tcp, wire::Identity, timing::Balanced>;

type Owner<'s, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd> = Host<
    client::Pool<
        'm,
        'h,
        'n,
        'd,
        'd,
        KeyTag<0>,
        MAX_CONNECTIONS,
        Connector<'s, KeyTag<0>, channel::Port<'f, 'e, 'b, 'c, 'd, 'd>>,
        sync::Preserve,
        net::SocketAddr,
        Network,
    >,
>;

type Handle<'app, 's, 'f, 'e, 'b, 'c, 'd> = request::Handle<
    'app,
    'f,
    'e,
    'b,
    'c,
    'd,
    'd,
    Connector<'s, KeyTag<0>, channel::Port<'f, 'e, 'b, 'c, 'd, 'd>>,
>;

const MAX_CONNECTIONS: usize = u32::MAX as usize;

const BACKOFF: (time::Duration, time::Duration) = (
    time::Duration::from_millis(200),
    time::Duration::from_secs(1),
);

pub struct Plan {
    http: channel::Plan,
    address: net::SocketAddr,
}

impl Plan {
    pub fn new(
        address: net::SocketAddr,
        config: config::Config,
        capacity: usize,
        timers: usize,
    ) -> Result<Self, config::Error> {
        let http = channel::Plan::new(
            config,
            channel::Topology::new(capacity, timers).with_target(capacity),
        )?;
        Ok(Self { http, address })
    }

    pub fn enter<C: executor::Build, R, Out>(
        self,
        context: C,
        run: impl for<'app, 'life, 's, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd> FnOnce(
            Scope<'app, 'life, 's, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd, C::Shutdown>,
        ) -> R,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<R, Error<'a>>) -> Out,
    ) -> Result<Out, Failure> {
        let mut finish = session::Completion::new();
        let finish = &mut finish;
        let Self { http, address } = self;
        let plan = http.plan(BACKOFF.0, BACKOFF.1).map_err(Failure::Plan)?;
        let prepared = topology::Plan::new((), (http, plan))
            .for_install::<timing::Balanced>()
            .map_err(Failure::Claim)?;
        Ok(prepared.enter_with(
            context,
            move |worker| {
                worker.lend(|(http, plan), stage| {
                    http.try_with(stage, lending::Stage::driver, |port, stage| {
                        let hash = stage.hash_state(health::Domain::DEFAULT);
                        let snapshot = service::Snapshot::from_one(
                            service::Revision::new(1),
                            service::Endpoint::new(address, address),
                        );
                        let setup = port.bind();
                        let wire = domain::Local::new(wire::Identity);
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
            |result| super::finish(result, done),
        ))
    }

    pub fn enter_at<R, Out>(
        self,
        offset: time::Duration,
        run: impl for<'app, 'life, 's, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd> FnOnce(
            Clock<'app, 'life, 's, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd>,
        ) -> R,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<R, Error<'a>>) -> Out,
    ) -> Result<Out, Failure> {
        let mut finish = session::Completion::new();
        let finish = &mut finish;
        let Self { http, address } = self;
        let plan = http.plan(BACKOFF.0, BACKOFF.1).map_err(Failure::Plan)?;
        let prepared = topology::Plan::new((), (http, plan))
            .for_install::<timing::Balanced>()
            .map_err(Failure::Claim)?;
        Ok(prepared.with_local(
            move |worker| {
                worker.lend(|(http, plan), stage| {
                    http.try_with(stage, lending::Stage::driver, |port, stage| {
                        let hash = stage.hash_state(health::Domain::DEFAULT);
                        let snapshot = service::Snapshot::from_one(
                            service::Revision::new(1),
                            service::Endpoint::new(address, address),
                        );
                        let setup = port.bind();
                        let wire = domain::Local::new(wire::Identity);
                        let plan = plan.bind(wire, hash);
                        plan.install::<_, _, sync::Preserve, _, Network, 16, _, _>(
                            setup,
                            snapshot,
                            stage,
                            lending::Stage::driver,
                            |pool, stage| {
                                let start = stage
                                    .driver()
                                    .driver_ref()
                                    .clock()
                                    .now()
                                    .try_add(offset)
                                    .map_err(Failure::Clock)?;
                                Ok::<_, Failure>(stage.with_at_for(
                                    finish,
                                    Host::new(pool),
                                    start,
                                    GUARD,
                                    |mut app| {
                                        let handle = app.root_client();
                                        run(Clock { app, handle, start })
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
            |result| super::finish(result, done),
        ))
    }
}

/// Live fixed-endpoint HTTP client bound to one application lifetime.
pub struct Scope<'app, 'life: 'app, 's, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd: 'life + 'f, Q = ()> {
    pub app: session::App<'app, 'life, 'd, Owner<'s, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd>, Q>,
    pub handle: Handle<'app, 's, 'f, 'e, 'b, 'c, 'd>,
}

impl<'app, 'life: 'app, 's, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd: 'life + 'f, Q>
    Scope<'app, 'life, 's, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd, Q>
where
    Owner<'s, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd>: executor::raw::App<'d>,
    Q: executor::Startup,
{
    /// Waits until the fixed connector admits its endpoint.
    pub fn wait(&mut self) -> Result<(), WaitError> {
        let handle = self.handle;
        fibers::TEST
            .try_run(&mut self.app, handle.wait_active())
            .map_err(WaitError::Drive)?
            .map_err(WaitError::Connector)
    }

    pub fn drain<P: ?Sized + 'app>(
        &mut self,
        stream: &mut Stream<'app, 'e, 'b, 'c, 'd, P, KeyTag<0>>,
    ) -> Result<(usize, Option<u16>, usize), Exchange<'d>> {
        let mut status = None;
        let mut body = 0;
        let mut extras = 0;
        while let Some(event) = self
            .app
            .block_on(stream.next_event())
            .map_err(Exchange::Drive)?
        {
            match event.map_err(|error| Exchange::Response(error.widen()))? {
                response::Event::Head(head) => status = Some(head.status().as_u16()),
                response::Event::Data(data) => body += data.len(),
                response::Event::Informational(_) | response::Event::Trailers(_) => extras += 1,
            }
        }
        Ok((extras, status, body))
    }
}

/// Logical-clock fixed client bound to one application lifetime.
pub struct Clock<'app, 'life: 'app, 's, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd: 'life + 'f> {
    pub app: session::Manual<'app, 'life, 'd, Owner<'s, 'm, 'h, 'n, 'f, 'e, 'b, 'c, 'd>>,
    pub handle: Handle<'app, 's, 'f, 'e, 'b, 'c, 'd>,
    pub start: clock::Deadline<'d>,
}
