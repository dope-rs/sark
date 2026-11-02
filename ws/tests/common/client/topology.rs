use std::{net, pin::Pin, time};

use dope::{
    core::driver::{
        life::recovery,
        route::KeyTag,
        settings,
        settings::{claim, demands, errors},
        storage,
    },
    manifold::{
        app::{Host, creation, env},
        client::{
            app::{admit::Registered, connect::Full, failure::Observed, request},
            session::{Target, codec::staging, policy},
        },
        network::client::{Connector, topology::plan},
        service,
        service::{bindings, bindings::endpoint::Provider as _, discover, health, sync},
        timing,
    },
    net::{tcp, wire},
    runtime::{
        executor,
        executor::{drive, session},
        topology::{self, error},
    },
};
use dope_fiber::{BlockOn as _, abi, wait};
use dope_test::{
    fibers,
    fibers::Wait,
    harness::{GUARD, signals},
    scope::Scoped,
};
use o3::{alloc, buffer::storage::Shared};
use sark_ws::{client, client::channel};

const MAX_CONNECTIONS: usize = u32::MAX as usize;

const BACKOFF: (time::Duration, time::Duration) = (
    time::Duration::from_millis(200),
    time::Duration::from_secs(1),
);

pub struct Layout {
    port: channel::Factory,
    connector: plan::Layout<KeyTag<0>, MAX_CONNECTIONS>,
    receive: demands::Receive,
}

impl settings::Resource for Layout {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = self.port.include_in(needs)?;
        let needs = self.connector.include_in(needs)?;
        needs.include(claim::Claim::new(settings::Routes::EMPTY).receive(self.receive))
    }
}

impl storage::Plan for Layout {
    type Output<'p, 'd>
        = channel::Port<'p, 'd>
    where
        Self: 'd,
        'd: 'p;
}

impl storage::Build for Layout {
    type Error = alloc::Error;
    type Store<'d>
        = <channel::Factory as storage::Build>::Store<'d>
    where
        Self: 'd;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd,
    {
        self.port.try_store()
    }

    fn try_build<'p, 'd>(
        self,
        backing: &'p mut Self::Store<'d>,
        context: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        self.port.try_build(backing, context)
    }
}

/// Failure while constructing or running one WebSocket client.
pub enum Error {
    Config(client::Error),
    Claim(errors::Claim),
}

#[derive(Debug)]
pub enum Kind<'a> {
    Config(&'a client::Error),
    Claim(&'a errors::Claim),
}

impl Error {
    pub fn kind(&self) -> Kind<'_> {
        match self {
            Self::Config(cause) => Kind::Config(cause),
            Self::Claim(cause) => Kind::Claim(cause),
        }
    }
}

#[derive(Debug)]
pub enum Failure {
    Plan(channel::PlanError),
    Connector(creation::Error),
}

#[derive(Debug)]
pub enum Run<'a> {
    Setup(&'a Failure),
    Session(session::Cause<'a>),
    Topology(error::Kind<'a, alloc::Error, Result<Result<(), session::Cause<'a>>, &'a Failure>>),
}

/// Failure while waiting for a WebSocket connection.
#[derive(Debug)]
pub enum WaitError {
    Drive(fibers::Run),
    Deadline,
}

/// Fully specified WebSocket client test topology.
pub struct Plan {
    address: net::SocketAddr,
    config: client::Config,
    capacity: usize,
    waiters: usize,
    backoff: (time::Duration, time::Duration),
    receive: demands::Receive,
}

impl Plan {
    #[must_use]
    pub const fn new(
        addr: net::SocketAddr,
        config: client::Config,
        cap: usize,
        waits: usize,
    ) -> Self {
        Self {
            address: addr,
            config,
            capacity: cap,
            waiters: waits,
            backoff: BACKOFF,
            receive: demands::Receive::ZERO,
        }
    }

    #[must_use]
    pub const fn with_backoff(mut self, retry: (time::Duration, time::Duration)) -> Self {
        self.backoff = retry;
        self
    }

    #[must_use]
    pub fn with_receive(mut self, receive: settings::Receive) -> Self {
        self.receive = receive.into();
        self
    }

    /// Runs one WebSocket client in its exact driver domain.
    pub fn enter<R, Out>(
        self,
        run: impl for<'report, 'scope, 'life, 'e, 'd> FnOnce(
            Domain<'report, 'scope, 'life, 'e, 'd>,
        ) -> Result<
            Result<R, session::Error<'report, R>>,
            Failure,
        >,
        policy: impl recovery::Observe,
        done: impl for<'a> FnOnce(Result<R, Run<'a>>) -> Out,
    ) -> Result<Out, Error> {
        let mut finish = session::Completion::new();
        let finish = &mut finish;
        let Self {
            address: addr,
            config,
            capacity: cap,
            waiters: waits,
            backoff: retry,
            receive,
        } = self;
        let port = channel::Port::factory(config, cap, waits).map_err(Error::Config)?;
        let layout = plan::Layout::new(cap).map_err(Error::Claim)?;
        let domain = topology::Domain::for_profile::<timing::Balanced>(Layout {
            port,
            connector: layout,
            receive,
        })
        .map_err(Error::Claim)?;
        Ok(domain.try_local(
            Scoped::new(move |scope| {
                run(Domain {
                    session: scope,
                    address: addr,
                    backoff: retry,
                    finish,
                })
            }),
            policy,
            |result| match result {
                Ok(Ok(Ok(value))) => done(Ok(value)),
                Ok(Ok(Err(error))) => done(Err(Run::Session(error.cause()))),
                Ok(Err(error)) => done(Err(Run::Setup(&error))),
                Err(error) => done(Err(Run::Topology(error.kind().map(|output| {
                    output
                        .as_ref()
                        .map(|session| session.as_ref().map(|_| ()).map_err(session::Error::cause))
                })))),
            },
        ))
    }
}

/// Entered WebSocket driver domain awaiting one lifetime-bound handler.
pub struct Domain<'report, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e> {
    session: session::Scope<'scope, 'life, 'e, 'd, Layout>,
    address: net::SocketAddr,
    backoff: (time::Duration, time::Duration),
    finish: &'report mut session::Completion,
}

impl<'report, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e> Domain<'report, 'scope, 'life, 'e, 'd> {
    /// Installs one lifetime-bound handler in this exact driver domain.
    pub fn run<H, M, R>(
        self,
        app: channel::App<H, M>,
        run: impl for<'app, 'q, 'h, 'p> FnOnce(
            Scope<
                'app,
                'life,
                'e,
                'd,
                Host<
                    Connector<
                        'q,
                        'h,
                        'p,
                        'd,
                        'd,
                        KeyTag<0>,
                        MAX_CONNECTIONS,
                        bindings::Bound<
                            'life,
                            'd,
                            channel::Port<'e, 'd>,
                            client::Logic<H, M>,
                            KeyTag<0>,
                            MAX_CONNECTIONS,
                        >,
                        discover::Watched<channel::Demand<'life, 'e, 'd, net::SocketAddr>>,
                        sync::Preserve,
                        net::SocketAddr,
                        (),
                    >,
                >,
            >,
        ) -> R,
    ) -> Result<Result<R, session::Error<'report, R>>, Failure>
    where
        client::Logic<H, M>: policy::Connected<
                'life,
                'd,
                channel::Port<'e, 'd>,
                KeyTag<0>,
                MAX_CONNECTIONS,
                Activation = Registered,
                Send = Shared,
            > + policy::Lifecycle<'life, 'd, channel::Port<'e, 'd>, KeyTag<0>, MAX_CONNECTIONS>,
        bindings::Bound<
            'life,
            'd,
            channel::Port<'e, 'd>,
            client::Logic<H, M>,
            KeyTag<0>,
            MAX_CONNECTIONS,
        >: Target<
                'd,
                KeyTag<0>,
                MAX_CONNECTIONS,
                Connect = Full,
                Failure = Observed,
                Requests = request::Enabled,
                Send = Shared,
            > + plan::Staged<'d, KeyTag<0>, staging::Buffered>,
    {
        let Domain {
            session: mut scope,
            address: addr,
            backoff: retry,
            finish,
        } = self;
        let hash = scope.hash_state(health::Domain::DEFAULT);
        let port = scope.storage();
        let set = service::Snapshot::from_one(
            service::Revision::new(1),
            service::Endpoint::new(addr, addr),
        );
        let plan = port.plan(retry.0, retry.1).map_err(Failure::Plan)?;
        let setup = app.bind(port);
        use wire::domain::Local;
        let wire = Local::new(wire::Identity);
        let plan = plan.bind(wire, hash);
        plan.watch::<_, _, _, sync::Preserve, _, env::Bundle<tcp::Tcp, wire::Identity, timing::Balanced>, 16, _, _>(
            setup, set, port.demand(), &mut scope, session::Scope::driver,
            |pool, scope| {
                scope.with_quiesce(finish, Host::new(pool), GUARD, |mut app| {
                    let handle = app.client_with(
                        |root| root,
                        |context| {
                            let (root, bound) = context.into_parts();
                            root.manifold().get_ref().session().root().provide(bound)
                        },
                    );
                    run(Scope { run: app, handle })
                })
            },
        ).map_err(Failure::Connector)
    }
}

/// Active WebSocket client bound to one application lifetime.
pub struct Scope<'app, 'life: 'app, 'e: 'life, 'd: 'e, D: executor::raw::App<'d>> {
    run: session::App<'app, 'life, 'd, D>,
    handle: client::Handle<'app, 'life, 'e, 'd, D>,
}

impl<'app, 'life: 'app, 'e: 'life, 'd: 'e, D> Scope<'app, 'life, 'e, 'd, D>
where
    D: executor::raw::App<'d>,
{
    #[must_use]
    pub const fn client(&self) -> client::Handle<'app, 'life, 'e, 'd, D> {
        self.handle
    }

    pub fn inspect<R>(&mut self, inspect: impl for<'a> FnOnce(Pin<&'a D>) -> R) -> R {
        self.run.inspect(|root| root, inspect)
    }

    pub fn drive<F>(&mut self, fiber: F) -> Result<F::Output, drive::Error>
    where
        F: abi::Fiber<'d>,
    {
        self.run.block_on(fiber)
    }

    /// Drives one fiber under the deterministic test watchdog.
    pub fn drive_within<F>(&mut self, fiber: F) -> Result<Option<F::Output>, fibers::Run>
    where
        F: abi::Fiber<'d>,
    {
        self.run
            .block_on(fibers::TEST.within(fiber))
            .map_err(fibers::Run::Drive)?
            .map_err(fibers::Run::Timer)
    }

    pub fn pause(&mut self, duration: time::Duration) -> Result<(), fibers::Run> {
        fibers::TEST.try_pause(&mut self.run, duration)
    }

    /// Waits until the peer's opening response is admitted.
    pub fn wait(
        &mut self,
        check: impl FnOnce(Result<(), client::Active<wait::Error<'d>>>),
    ) -> Result<(), WaitError> {
        let handle = self.handle;
        check(
            self.drive_within(handle.wait_active())
                .map_err(WaitError::Drive)?
                .ok_or(WaitError::Deadline)?,
        );
        Ok(())
    }

    /// Drives until a causal test signal reaches the requested count.
    pub fn try_until(
        &mut self,
        gate: &(impl signals::Progress + ?Sized),
        want: u32,
    ) -> Result<Wait, fibers::Run> {
        fibers::TEST.try_wait(&mut self.run, gate, want)
    }
}
