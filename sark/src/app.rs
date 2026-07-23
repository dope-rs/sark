use std::io;
use std::marker::PhantomData;
use std::time::Duration;

use dope::driver::token::Token;
use dope::manifold::env::Bundle;
use dope::manifold::listener::{self, Application, Listener};
use dope::manifold::{Manifold, TypedToken};
use dope::runtime::{
    Dispatcher as RuntimeDispatcher, Idle, Launcher, Session, ShutdownTrigger, StorageFactory,
    WorkerContext, WorkerEntry,
};
use dope::{DriverContext, Event};
use dope_net::wire::Wire;
use dope_net::wire::identity::Identity;
use dope_net::{Transport, tcp::Tcp};
use dope_tls::tls::{Endpoint, Tls};
use o3::cell::BrandCell as Branded;

use crate::date::{DateHost, Updater};
use crate::timer::{TimedListener, TimerHost};

pub use dope::driver;
pub use dope::runtime::Executor;
pub use dope::runtime::profile::RuntimeProfile;
pub use dope::runtime::profile::{Balanced, LowLatency, Throughput};

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub timer_capacity: usize,
    pub task_capacity: usize,
}

macro_rules! run_methods {
    () => {
        pub fn run<D, F>(self, cpu_ids: Vec<u16>, driver_config: D, worker: F) -> io::Result<()>
        where
            D: FnOnce(u16) -> driver::Config + Clone + Send,
            F: for<'scope, 'd> FnOnce(Self, &mut Session<'scope, 'd>) -> io::Result<()>
                + Clone
                + Send,
        {
            run_server(self, cpu_ids, driver_config, worker)
        }

        pub fn run_with_storage<S, D, SF, F>(
            self,
            cpu_ids: Vec<u16>,
            driver_config: D,
            storage_factory: SF,
            worker: F,
        ) -> io::Result<()>
        where
            S: StorageFactory,
            D: FnOnce(u16) -> driver::Config + Clone + Send,
            SF: FnOnce(u16, dope::hash::Seed) -> S + Clone + Send,
            F: for<'scope, 'd> FnOnce(
                    Self,
                    &mut Session<'scope, 'd, S::Output<'d>>,
                ) -> io::Result<()>
                + Clone
                + Send,
        {
            run_server_with_storage(self, cpu_ids, driver_config, storage_factory, worker)
        }

        pub fn run_worker<F>(self, driver_config: driver::Config, worker: F) -> io::Result<()>
        where
            F: for<'scope, 'd> FnOnce(Self, &mut Session<'scope, 'd>) -> io::Result<()>,
        {
            run_server_worker(self, driver_config, worker)
        }

        pub fn run_worker_with_storage<S, F>(
            self,
            driver_config: driver::Config,
            storage_factory: S,
            worker: F,
        ) -> io::Result<()>
        where
            S: StorageFactory,
            F: for<'scope, 'd> FnOnce(
                Self,
                &mut Session<'scope, 'd, S::Output<'d>>,
            ) -> io::Result<()>,
        {
            run_server_worker_with_storage(self, driver_config, storage_factory, worker)
        }
    };
}

pub struct HttpServer<const LISTENER_ID: u8, const DATE_ID: u8, P> {
    listener: listener::Config<Tcp>,
    head_timeout: Duration,
    profile: PhantomData<fn() -> P>,
}

impl<const LISTENER_ID: u8, const DATE_ID: u8, P> Clone for HttpServer<LISTENER_ID, DATE_ID, P> {
    fn clone(&self) -> Self {
        Self {
            listener: clone_listener_config(&self.listener),
            head_timeout: self.head_timeout,
            profile: PhantomData,
        }
    }
}

impl<const LISTENER_ID: u8, const DATE_ID: u8, P> HttpServer<LISTENER_ID, DATE_ID, P>
where
    P: RuntimeProfile,
{
    pub fn new(listener: listener::Config<Tcp>, head_timeout: Duration) -> Self {
        Self {
            listener,
            head_timeout,
            profile: PhantomData,
        }
    }

    pub fn listener_config(&self) -> &listener::Config<Tcp> {
        &self.listener
    }

    pub fn head_timeout(&self) -> Duration {
        self.head_timeout
    }

    run_methods!();

    pub fn serve<'scope, 'd: 'scope, A, S>(
        self,
        session: &mut Session<'scope, 'd, S>,
        app: A,
        shutdown: Option<&ShutdownTrigger>,
    ) -> io::Result<()>
    where
        A: Application<'d, Wire = Identity> + DateHost + TimerHost<'d>,
    {
        run::<LISTENER_ID, DATE_ID, A, Identity, P, S>(
            session,
            app,
            self.listener,
            self.head_timeout,
            |_| {},
            shutdown,
        )
    }

    pub fn serve_with_resource<'scope, 'd: 'scope, A, R, S>(
        self,
        session: &mut Session<'scope, 'd, S>,
        app: A,
        resource: R,
        shutdown: Option<&ShutdownTrigger>,
    ) -> io::Result<()>
    where
        A: Application<'d, Wire = Identity> + DateHost + TimerHost<'d>,
        R: Manifold<'d>,
    {
        run_with_resource::<LISTENER_ID, DATE_ID, A, Identity, P, R, S>(
            session,
            app,
            self.listener,
            self.head_timeout,
            resource,
            |_| {},
            shutdown,
        )
    }
}

pub struct HttpsServer<const LISTENER_ID: u8, const DATE_ID: u8, P> {
    listener: listener::Config<Tcp>,
    head_timeout: Duration,
    tls: shin::server::Config,
    profile: PhantomData<fn() -> P>,
}

impl<const LISTENER_ID: u8, const DATE_ID: u8, P> Clone for HttpsServer<LISTENER_ID, DATE_ID, P> {
    fn clone(&self) -> Self {
        Self {
            listener: clone_listener_config(&self.listener),
            head_timeout: self.head_timeout,
            tls: self.tls.clone(),
            profile: PhantomData,
        }
    }
}

impl<const LISTENER_ID: u8, const DATE_ID: u8, P> HttpsServer<LISTENER_ID, DATE_ID, P>
where
    P: RuntimeProfile,
{
    pub fn new(
        listener: listener::Config<Tcp>,
        head_timeout: Duration,
        tls: shin::server::Config,
    ) -> Self {
        Self {
            listener,
            head_timeout,
            tls,
            profile: PhantomData,
        }
    }

    pub fn listener_config(&self) -> &listener::Config<Tcp> {
        &self.listener
    }

    pub fn head_timeout(&self) -> Duration {
        self.head_timeout
    }

    pub fn tls_config(&self) -> &shin::server::Config {
        &self.tls
    }

    run_methods!();

    pub fn serve<'scope, 'd: 'scope, A, S>(
        self,
        session: &mut Session<'scope, 'd, S>,
        app: A,
        shutdown: Option<&ShutdownTrigger>,
    ) -> io::Result<()>
    where
        A: Application<'d, Wire = Tls> + DateHost + TimerHost<'d>,
    {
        run::<LISTENER_ID, DATE_ID, A, Tls, P, S>(
            session,
            app,
            self.listener,
            self.head_timeout,
            move |listener| listener.set_config(Endpoint::Server(Box::new(self.tls))),
            shutdown,
        )
    }

    pub fn serve_with_resource<'scope, 'd: 'scope, A, R, S>(
        self,
        session: &mut Session<'scope, 'd, S>,
        app: A,
        resource: R,
        shutdown: Option<&ShutdownTrigger>,
    ) -> io::Result<()>
    where
        A: Application<'d, Wire = Tls> + DateHost + TimerHost<'d>,
        R: Manifold<'d>,
    {
        run_with_resource::<LISTENER_ID, DATE_ID, A, Tls, P, R, S>(
            session,
            app,
            self.listener,
            self.head_timeout,
            resource,
            move |listener| listener.set_config(Endpoint::Server(Box::new(self.tls))),
            shutdown,
        )
    }
}

fn run_server<T, D, F>(server: T, cpu_ids: Vec<u16>, driver_config: D, worker: F) -> io::Result<()>
where
    T: Clone + Send,
    D: FnOnce(u16) -> driver::Config + Clone + Send,
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd>) -> io::Result<()> + Clone + Send,
{
    let launcher = Launcher::pinned(cpu_ids)?;
    let inputs = worker_inputs(launcher.worker_count(), server, driver_config, worker);
    launcher.run::<ServerEntry<T, D, F>>(inputs)
}

fn run_server_with_storage<T, S, D, SF, F>(
    server: T,
    cpu_ids: Vec<u16>,
    driver_config: D,
    storage_factory: SF,
    worker: F,
) -> io::Result<()>
where
    T: Clone + Send,
    S: StorageFactory,
    D: FnOnce(u16) -> driver::Config + Clone + Send,
    SF: FnOnce(u16, dope::hash::Seed) -> S + Clone + Send,
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd, S::Output<'d>>) -> io::Result<()>
        + Clone
        + Send,
{
    let launcher = Launcher::pinned(cpu_ids)?;
    let inputs = worker_inputs(
        launcher.worker_count(),
        server,
        (driver_config, storage_factory),
        worker,
    );
    launcher.run::<StorageEntry<T, S, D, SF, F>>(inputs)
}

struct WorkerInput<T, D, F> {
    server: T,
    factory: D,
    worker: F,
}

fn worker_inputs<T, D, F>(len: usize, server: T, factory: D, worker: F) -> Vec<WorkerInput<T, D, F>>
where
    T: Clone,
    D: Clone,
    F: Clone,
{
    let mut inputs = Vec::with_capacity(len);
    for _ in 1..len {
        inputs.push(WorkerInput {
            server: server.clone(),
            factory: factory.clone(),
            worker: worker.clone(),
        });
    }
    inputs.push(WorkerInput {
        server,
        factory,
        worker,
    });
    inputs
}

struct ServerEntry<T, D, F>(PhantomData<fn(T, D, F)>);

impl<T, D, F> WorkerEntry for ServerEntry<T, D, F>
where
    T: Send,
    D: FnOnce(u16) -> driver::Config + Send,
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd>) -> io::Result<()> + Send,
{
    type Input = WorkerInput<T, D, F>;

    fn run(input: Self::Input, context: WorkerContext) -> io::Result<()> {
        let cpu = context.cpu().expect("pinned launcher worker has a CPU");
        run_server_worker_with_context(
            input.server,
            (input.factory)(cpu),
            Some(context.seed()),
            Some(&context),
            input.worker,
        )
    }
}

struct StorageEntry<T, S, D, SF, F>(
    PhantomData<fn(T)>,
    PhantomData<fn(S)>,
    PhantomData<fn(D)>,
    PhantomData<fn(SF)>,
    PhantomData<fn(F)>,
);

impl<T, S, D, SF, F> WorkerEntry for StorageEntry<T, S, D, SF, F>
where
    T: Send,
    S: StorageFactory,
    D: FnOnce(u16) -> driver::Config + Send,
    SF: FnOnce(u16, dope::hash::Seed) -> S + Send,
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd, S::Output<'d>>) -> io::Result<()> + Send,
{
    type Input = WorkerInput<T, (D, SF), F>;

    fn run(input: Self::Input, context: WorkerContext) -> io::Result<()> {
        let (driver_config, storage_factory) = input.factory;
        let cpu = context.cpu().expect("pinned launcher worker has a CPU");
        let seed = context.seed();
        run_server_worker_with_storage_and_context(
            input.server,
            driver_config(cpu),
            Some(seed),
            storage_factory(cpu, seed),
            Some(&context),
            input.worker,
        )
    }
}

fn run_server_worker<T, F>(server: T, driver_config: driver::Config, worker: F) -> io::Result<()>
where
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd>) -> io::Result<()>,
{
    run_server_worker_with_context(server, driver_config, None, None, worker)
}

fn run_server_worker_with_context<T, F>(
    server: T,
    driver_config: driver::Config,
    seed: Option<dope::hash::Seed>,
    worker_context: Option<&WorkerContext>,
    worker: F,
) -> io::Result<()>
where
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd>) -> io::Result<()>,
{
    let executor = match seed {
        Some(seed) => Executor::with_seed(driver_config, seed)?,
        None => Executor::new(driver_config)?,
    };
    executor.enter(|mut session| {
        if let Some(context) = worker_context {
            context.try_register_shutdown(&mut session.driver_access())?;
        }
        worker(server, &mut session)
    })
}

fn run_server_worker_with_storage<T, S, F>(
    server: T,
    driver_config: driver::Config,
    storage_factory: S,
    worker: F,
) -> io::Result<()>
where
    S: StorageFactory,
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd, S::Output<'d>>) -> io::Result<()>,
{
    run_server_worker_with_storage_and_context(
        server,
        driver_config,
        None,
        storage_factory,
        None,
        worker,
    )
}

fn run_server_worker_with_storage_and_context<T, S, F>(
    server: T,
    driver_config: driver::Config,
    seed: Option<dope::hash::Seed>,
    storage_factory: S,
    worker_context: Option<&WorkerContext>,
    worker: F,
) -> io::Result<()>
where
    S: StorageFactory,
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd, S::Output<'d>>) -> io::Result<()>,
{
    let executor = match seed {
        Some(seed) => Executor::with_seed(driver_config, seed)?,
        None => Executor::new(driver_config)?,
    };
    executor
        .with_storage_factory(storage_factory)
        .enter(|mut session| {
            if let Some(context) = worker_context {
                context.try_register_shutdown(&mut session.driver_access())?;
            }
            worker(server, &mut session)
        })
}

fn run<'scope, 'd: 'scope, const LISTENER_ID: u8, const DATE_ID: u8, A, W, P, S>(
    session: &mut Session<'scope, 'd, S>,
    app: A,
    listener: listener::Config<Tcp>,
    head_timeout: Duration,
    configure: impl FnOnce(&mut Listener<'d, LISTENER_ID, A, Bundle<Tcp, W, P>>),
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    W: Wire,
    P: RuntimeProfile,
{
    let hash_builder = session
        .seed()
        .derive(dope::hash::domain::ACCEPT ^ LISTENER_ID as u64)
        .state();
    let mut listener = {
        let mut driver = session.driver_access();
        if let Some(trigger) = shutdown {
            trigger.try_register(&mut driver)?;
        }
        Listener::open_in(app, listener, hash_builder, &mut driver)?
    };
    configure(&mut listener);
    listener.handler().timer().set_head_timeout(head_timeout);
    let dispatcher = core::pin::pin!(Branded::new(Dispatcher::<
        'd,
        LISTENER_ID,
        DATE_ID,
        A,
        Tcp,
        W,
        P,
    > {
        listener: TimedListener::new(listener, session.driver()),
        date: Updater::new(),
    }));
    session.run(dispatcher.as_ref())
}

fn run_with_resource<'scope, 'd: 'scope, const LISTENER_ID: u8, const DATE_ID: u8, A, W, P, R, S>(
    session: &mut Session<'scope, 'd, S>,
    app: A,
    listener: listener::Config<Tcp>,
    head_timeout: Duration,
    resource: R,
    configure: impl FnOnce(&mut Listener<'d, LISTENER_ID, A, Bundle<Tcp, W, P>>),
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    W: Wire,
    P: RuntimeProfile,
    R: Manifold<'d>,
{
    let hash_builder = session
        .seed()
        .derive(dope::hash::domain::ACCEPT ^ LISTENER_ID as u64)
        .state();
    let mut listener = {
        let mut driver = session.driver_access();
        if let Some(trigger) = shutdown {
            trigger.try_register(&mut driver)?;
        }
        Listener::open_in(app, listener, hash_builder, &mut driver)?
    };
    configure(&mut listener);
    listener.handler().timer().set_head_timeout(head_timeout);
    let dispatcher = core::pin::pin!(Branded::new(ResourceDispatcher::<
        'd,
        LISTENER_ID,
        DATE_ID,
        A,
        Tcp,
        W,
        P,
        R,
    > {
        listener: TimedListener::new(listener, session.driver()),
        date: Updater::new(),
        resource,
    }));
    session.run(dispatcher.as_ref())
}

#[pin_project::pin_project]
struct Dispatcher<'d, const LISTENER_ID: u8, const DATE_ID: u8, A, T, W, P>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    T: Transport,
    W: Wire,
    P: RuntimeProfile,
{
    #[pin]
    listener: TimedListener<'d, LISTENER_ID, A, Bundle<T, W, P>>,
    date: Updater<DATE_ID>,
}

#[pin_project::pin_project]
struct ResourceDispatcher<'d, const LISTENER_ID: u8, const DATE_ID: u8, A, T, W, P, R>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    T: Transport,
    W: Wire,
    P: RuntimeProfile,
    R: Manifold<'d>,
{
    #[pin]
    listener: TimedListener<'d, LISTENER_ID, A, Bundle<T, W, P>>,
    date: Updater<DATE_ID>,
    #[pin]
    resource: R,
}

impl<'d, const LISTENER_ID: u8, const DATE_ID: u8, A, T, W, P>
    Dispatcher<'d, LISTENER_ID, DATE_ID, A, T, W, P>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    T: Transport,
    W: Wire,
    P: RuntimeProfile,
{
    const ROUTES_UNIQUE: () = assert!(
        LISTENER_ID != DATE_ID,
        "listener and date manifolds require distinct route IDs"
    );
}

impl<'d, const LISTENER_ID: u8, const DATE_ID: u8, A, T, W, P> RuntimeDispatcher<'d>
    for Dispatcher<'d, LISTENER_ID, DATE_ID, A, T, W, P>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    T: Transport,
    W: Wire,
    P: RuntimeProfile,
{
    fn dispatch(
        mut self: core::pin::Pin<&mut Self>,
        event: Event<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let _: () = Self::ROUTES_UNIQUE;
        let route = event.route();
        if route == LISTENER_ID {
            Manifold::dispatch(self.project().listener, event, driver);
        } else if route == DATE_ID {
            let mut fields = self.as_mut().project();
            let handler = fields.listener.as_mut().handler_mut();
            let stamp = DateHost::stamp(handler.as_ref());
            fields.date.dispatch(event, stamp.get_ref(), driver);
        }
    }

    fn activate(
        self: core::pin::Pin<&mut Self>,
        target: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let _: () = Self::ROUTES_UNIQUE;
        if target.route() == LISTENER_ID {
            let target =
                TypedToken::<TimedListener<'d, LISTENER_ID, A, Bundle<T, W, P>>>::try_new::<'d>(
                    target,
                )
                .expect("dispatcher selected the listener route");
            Manifold::activate(self.project().listener, target, driver);
        }
    }

    fn pre_park(mut self: core::pin::Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let fields = self.as_mut().project();
        Manifold::pre_park(fields.listener, driver);
        fields.date.pre_park(driver);
    }

    fn idle(self: core::pin::Pin<&Self>) -> Idle {
        let fields = self.project_ref();
        Manifold::idle(fields.listener).reduce(fields.date.idle())
    }

    fn shutdown(mut self: core::pin::Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let fields = self.as_mut().project();
        Manifold::shutdown(fields.listener, driver);
        fields.date.shutdown(driver);
    }
}

impl<'d, const LISTENER_ID: u8, const DATE_ID: u8, A, T, W, P, R>
    ResourceDispatcher<'d, LISTENER_ID, DATE_ID, A, T, W, P, R>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    T: Transport,
    W: Wire,
    P: RuntimeProfile,
    R: Manifold<'d>,
{
    const ROUTES_UNIQUE: () = assert!(
        LISTENER_ID != DATE_ID && LISTENER_ID != R::ID && DATE_ID != R::ID,
        "listener, date, and resource manifolds require distinct route IDs"
    );
}

impl<'d, const LISTENER_ID: u8, const DATE_ID: u8, A, T, W, P, R> RuntimeDispatcher<'d>
    for ResourceDispatcher<'d, LISTENER_ID, DATE_ID, A, T, W, P, R>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    T: Transport,
    W: Wire,
    P: RuntimeProfile,
    R: Manifold<'d>,
{
    fn dispatch(
        mut self: core::pin::Pin<&mut Self>,
        event: Event<'d>,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let _: () = Self::ROUTES_UNIQUE;
        let route = event.route();
        if route == LISTENER_ID {
            Manifold::dispatch(self.project().listener, event, driver);
        } else if route == DATE_ID {
            let mut fields = self.as_mut().project();
            let handler = fields.listener.as_mut().handler_mut();
            let stamp = DateHost::stamp(handler.as_ref());
            fields.date.dispatch(event, stamp.get_ref(), driver);
        } else if route == R::ID {
            Manifold::dispatch(self.project().resource, event, driver);
        }
    }

    fn activate(
        self: core::pin::Pin<&mut Self>,
        target: Token,
        driver: &mut DriverContext<'_, 'd>,
    ) {
        let _: () = Self::ROUTES_UNIQUE;
        let route = target.route();
        if route == LISTENER_ID {
            let target =
                TypedToken::<TimedListener<'d, LISTENER_ID, A, Bundle<T, W, P>>>::try_new::<'d>(
                    target,
                )
                .expect("dispatcher selected the listener route");
            Manifold::activate(self.project().listener, target, driver);
        } else if route == R::ID {
            let target = TypedToken::<R>::try_new::<'d>(target)
                .expect("dispatcher selected the resource route");
            Manifold::activate(self.project().resource, target, driver);
        }
    }

    fn pre_park(mut self: core::pin::Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let fields = self.as_mut().project();
        Manifold::pre_park(fields.listener, driver);
        fields.date.pre_park(driver);
        Manifold::pre_park(fields.resource, driver);
    }

    fn idle(self: core::pin::Pin<&Self>) -> Idle {
        let fields = self.project_ref();
        Manifold::idle(fields.listener)
            .reduce(fields.date.idle())
            .reduce(Manifold::idle(fields.resource))
    }

    fn shutdown(mut self: core::pin::Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let fields = self.as_mut().project();
        Manifold::shutdown(fields.listener, driver);
        fields.date.shutdown(driver);
        Manifold::shutdown(fields.resource, driver);
    }
}

fn clone_listener_config(config: &listener::Config<Tcp>) -> listener::Config<Tcp> {
    listener::Config {
        max_connections: config.max_connections,
        bind: config.bind,
        backlog: config.backlog,
        stream: config.stream,
        transport: config.transport,
        egress: config.egress,
    }
}
