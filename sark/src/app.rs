use std::io::{self, Error};
use std::marker::PhantomData;
use std::pin::{Pin, pin};
use std::time::Duration;

use dope::driver::token::Token;
use dope::hash::{Seed, domain::ACCEPT};
use dope::manifold::Manifold;
use dope::manifold::env::Bundle;
use dope::manifold::listener::{self, Listener, application::Application};
use dope::manifold::typed::TypedToken;
use dope::runtime::dispatcher::{self, Idle};
use dope::runtime::executor::{Session, StorageFactory};
use dope::runtime::launcher::{Launcher, WorkerContext, WorkerEntry};
use dope::runtime::trigger::ShutdownTrigger;
use dope::{DriverContext, Event};
use dope_net::link::egress;
use dope_net::wire::Wire;
use dope_net::wire::identity::Identity;
use dope_net::{Transport, tcp::Tcp};
use dope_tls::tls::{Endpoint, Tls};
use o3::cell::{self, RegionToken};
use shin::server;

use crate::date::{DateHost, Updater};
use crate::timer::{TimedListener, TimerHost};

pub use dope::driver;
pub use dope::runtime::executor::Executor;
pub use dope::runtime::profile::RuntimeProfile;
pub use dope::runtime::profile::{Balanced, LowLatency, Throughput};

pub struct ServerStorage<T, W: Wire = Identity> {
    value: T,
    egress: egress::storage::Storage,
    wire: W::ConnectionStorage,
}

impl<T, W: Wire> ServerStorage<T, W> {
    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<W: Wire> ServerStorage<(), W> {
    pub fn try_with_capacity(capacity: usize) -> io::Result<Self> {
        Ok(Self {
            value: (),
            egress: egress::storage::Storage::default(),
            wire: W::connection_storage(capacity)?,
        })
    }
}

impl<T, W: Wire> std::ops::Deref for ServerStorage<T, W> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T, W: Wire> std::ops::DerefMut for ServerStorage<T, W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<T, W: Wire> AsRef<T> for ServerStorage<T, W> {
    fn as_ref(&self) -> &T {
        &self.value
    }
}

pub trait EgressHost {
    fn egress_storage(&self) -> &egress::storage::Storage;
}

impl EgressHost for egress::storage::Storage {
    fn egress_storage(&self) -> &egress::storage::Storage {
        self
    }
}

impl<T, W: Wire> EgressHost for ServerStorage<T, W> {
    fn egress_storage(&self) -> &egress::storage::Storage {
        &self.egress
    }
}

pub trait WireStorageHost<W: Wire> {
    fn wire_storage(&self) -> &W::ConnectionStorage;
}

impl<T, W: Wire> WireStorageHost<W> for ServerStorage<T, W> {
    fn wire_storage(&self) -> &W::ConnectionStorage {
        &self.wire
    }
}

struct WithServerStorage<S, W: Wire> {
    value: S,
    wire: W::ConnectionStorage,
}

impl<S: StorageFactory, W: Wire> StorageFactory for WithServerStorage<S, W> {
    type Output<'d> = ServerStorage<S::Output<'d>, W>;

    fn build<'d>(self, driver: &mut DriverContext<'_, 'd>) -> Self::Output<'d> {
        ServerStorage {
            value: self.value.build(driver),
            egress: egress::storage::Storage::default(),
            wire: self.wire,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub task_capacity: usize,
}

pub struct Server<const LISTENER_ID: u8, const DATE_ID: u8, P, W> {
    listener: listener::config::Config<Tcp>,
    head_timeout: Duration,
    protocol: PhantomData<fn() -> (P, W)>,
}

pub type HttpServer<const LISTENER_ID: u8, const DATE_ID: u8, P> =
    Server<LISTENER_ID, DATE_ID, P, Identity>;
pub type HttpsServer<const LISTENER_ID: u8, const DATE_ID: u8, P> =
    Server<LISTENER_ID, DATE_ID, P, Tls>;

impl<const LISTENER_ID: u8, const DATE_ID: u8, P, W> Clone for Server<LISTENER_ID, DATE_ID, P, W> {
    fn clone(&self) -> Self {
        Self {
            listener: clone_listener_config(&self.listener),
            head_timeout: self.head_timeout,
            protocol: PhantomData,
        }
    }
}

impl<const LISTENER_ID: u8, const DATE_ID: u8, P, W> Server<LISTENER_ID, DATE_ID, P, W>
where
    P: RuntimeProfile,
    W: Wire,
{
    pub fn new(listener: listener::config::Config<Tcp>, head_timeout: Duration) -> Self {
        Self {
            listener,
            head_timeout,
            protocol: PhantomData,
        }
    }

    pub fn listener_config(&self) -> &listener::config::Config<Tcp> {
        &self.listener
    }

    pub fn head_timeout(&self) -> Duration {
        self.head_timeout
    }

    pub fn run<D, F>(self, cpu_ids: Vec<u16>, driver_config: D, worker: F) -> io::Result<()>
    where
        D: FnOnce(u16) -> driver::Config + Clone + Send,
        F: for<'scope, 'd> FnOnce(
                Self,
                &mut Session<'scope, 'd, ServerStorage<(), W>>,
            ) -> io::Result<()>
            + Clone
            + Send,
    {
        let capacity = self.listener.max_connections;
        run_server::<_, W, _, _>(self, cpu_ids, capacity, driver_config, worker)
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
        SF: FnOnce(u16, Seed) -> S + Clone + Send,
        F: for<'scope, 'd> FnOnce(
                Self,
                &mut Session<'scope, 'd, ServerStorage<S::Output<'d>, W>>,
            ) -> io::Result<()>
            + Clone
            + Send,
    {
        let capacity = self.listener.max_connections;
        run_server_with_storage::<_, W, _, _, _, _>(
            self,
            cpu_ids,
            capacity,
            driver_config,
            storage_factory,
            worker,
        )
    }

    pub fn run_worker<F>(self, driver_config: driver::Config, worker: F) -> io::Result<()>
    where
        F: for<'scope, 'd> FnOnce(
            Self,
            &mut Session<'scope, 'd, ServerStorage<(), W>>,
        ) -> io::Result<()>,
    {
        let capacity = self.listener.max_connections;
        run_server_worker::<_, W, _>(self, driver_config, capacity, worker)
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
            &mut Session<'scope, 'd, ServerStorage<S::Output<'d>, W>>,
        ) -> io::Result<()>,
    {
        let capacity = self.listener.max_connections;
        run_server_worker_with_storage::<_, W, _, _>(
            self,
            driver_config,
            capacity,
            storage_factory,
            worker,
        )
    }
}

impl<const LISTENER_ID: u8, const DATE_ID: u8, P> Server<LISTENER_ID, DATE_ID, P, Identity>
where
    P: RuntimeProfile,
{
    pub fn serve<'scope, 'd: 'scope, A, S>(
        self,
        session: &mut Session<'scope, 'd, S>,
        app: A,
        shutdown: Option<&ShutdownTrigger>,
    ) -> io::Result<()>
    where
        A: Application<'d, Wire = Identity> + DateHost + TimerHost<'d>,
        S: EgressHost,
    {
        run::<LISTENER_ID, DATE_ID, A, Identity, P, NoResource, S>(
            session,
            app,
            self.listener,
            self.head_timeout,
            NoResource,
            (),
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
        S: EgressHost,
    {
        run::<LISTENER_ID, DATE_ID, A, Identity, P, R, S>(
            session,
            app,
            self.listener,
            self.head_timeout,
            resource,
            (),
            shutdown,
        )
    }
}

impl<const LISTENER_ID: u8, const DATE_ID: u8, P> Server<LISTENER_ID, DATE_ID, P, Tls>
where
    P: RuntimeProfile,
{
    pub fn serve<'scope, 'd: 'scope, A, S>(
        self,
        session: &mut Session<'scope, 'd, S>,
        app: A,
        tls: server::config::Config,
        shutdown: Option<&ShutdownTrigger>,
    ) -> io::Result<()>
    where
        A: Application<'d, Wire = Tls> + DateHost + TimerHost<'d>,
        S: EgressHost + WireStorageHost<Tls>,
    {
        let endpoint = Endpoint::server(tls)
            .map_err(Error::other)?
            .bind(session.storage().wire_storage());
        run::<LISTENER_ID, DATE_ID, A, Tls, P, NoResource, S>(
            session,
            app,
            self.listener,
            self.head_timeout,
            NoResource,
            endpoint,
            shutdown,
        )
    }

    pub fn serve_with_resource<'scope, 'd: 'scope, A, R, S>(
        self,
        session: &mut Session<'scope, 'd, S>,
        app: A,
        resource: R,
        tls: server::config::Config,
        shutdown: Option<&ShutdownTrigger>,
    ) -> io::Result<()>
    where
        A: Application<'d, Wire = Tls> + DateHost + TimerHost<'d>,
        R: Manifold<'d>,
        S: EgressHost + WireStorageHost<Tls>,
    {
        let endpoint = Endpoint::server(tls)
            .map_err(Error::other)?
            .bind(session.storage().wire_storage());
        run::<LISTENER_ID, DATE_ID, A, Tls, P, R, S>(
            session,
            app,
            self.listener,
            self.head_timeout,
            resource,
            endpoint,
            shutdown,
        )
    }
}

fn run_server<T, W, D, F>(
    server: T,
    cpu_ids: Vec<u16>,
    capacity: usize,
    driver_config: D,
    worker: F,
) -> io::Result<()>
where
    T: Clone + Send,
    W: Wire,
    D: FnOnce(u16) -> driver::Config + Clone + Send,
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd, ServerStorage<(), W>>) -> io::Result<()>
        + Clone
        + Send,
{
    let launcher = Launcher::pinned(cpu_ids)?;
    let inputs = worker_inputs(
        launcher.worker_count(),
        server,
        driver_config,
        worker,
        capacity,
    );
    launcher.run::<ServerEntry<T, W, D, F>>(inputs)
}

fn run_server_with_storage<T, W, S, D, SF, F>(
    server: T,
    cpu_ids: Vec<u16>,
    capacity: usize,
    driver_config: D,
    storage_factory: SF,
    worker: F,
) -> io::Result<()>
where
    T: Clone + Send,
    W: Wire,
    S: StorageFactory,
    D: FnOnce(u16) -> driver::Config + Clone + Send,
    SF: FnOnce(u16, Seed) -> S + Clone + Send,
    F: for<'scope, 'd> FnOnce(
            T,
            &mut Session<'scope, 'd, ServerStorage<S::Output<'d>, W>>,
        ) -> io::Result<()>
        + Clone
        + Send,
{
    let launcher = Launcher::pinned(cpu_ids)?;
    let inputs = worker_inputs(
        launcher.worker_count(),
        server,
        (driver_config, storage_factory),
        worker,
        capacity,
    );
    launcher.run::<StorageEntry<T, W, S, D, SF, F>>(inputs)
}

struct WorkerInput<T, D, F> {
    server: T,
    factory: D,
    worker: F,
    capacity: usize,
}

fn worker_inputs<T, D, F>(
    len: usize,
    server: T,
    factory: D,
    worker: F,
    capacity: usize,
) -> Vec<WorkerInput<T, D, F>>
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
            capacity,
        });
    }
    inputs.push(WorkerInput {
        server,
        factory,
        worker,
        capacity,
    });
    inputs
}

struct ServerEntry<T, W, D, F>(PhantomData<fn(T, W, D, F)>);

impl<T, W, D, F> WorkerEntry for ServerEntry<T, W, D, F>
where
    T: Send,
    W: Wire,
    D: FnOnce(u16) -> driver::Config + Send,
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd, ServerStorage<(), W>>) -> io::Result<()>
        + Send,
{
    type Input = WorkerInput<T, D, F>;

    fn run(input: Self::Input, context: WorkerContext) -> io::Result<()> {
        let cpu = context.cpu().expect("pinned launcher worker has a CPU");
        run_server_worker_with_context::<_, W, _>(
            input.server,
            (input.factory)(cpu),
            input.capacity,
            Some(context.seed()),
            Some(&context),
            input.worker,
        )
    }
}

struct StorageEntry<T, W, S, D, SF, F>(
    PhantomData<fn(T)>,
    PhantomData<fn(W)>,
    PhantomData<fn(S)>,
    PhantomData<fn(D)>,
    PhantomData<fn(SF)>,
    PhantomData<fn(F)>,
);

impl<T, W, S, D, SF, F> WorkerEntry for StorageEntry<T, W, S, D, SF, F>
where
    T: Send,
    W: Wire,
    S: StorageFactory,
    D: FnOnce(u16) -> driver::Config + Send,
    SF: FnOnce(u16, Seed) -> S + Send,
    F: for<'scope, 'd> FnOnce(
            T,
            &mut Session<'scope, 'd, ServerStorage<S::Output<'d>, W>>,
        ) -> io::Result<()>
        + Send,
{
    type Input = WorkerInput<T, (D, SF), F>;

    fn run(input: Self::Input, context: WorkerContext) -> io::Result<()> {
        let (driver_config, storage_factory) = input.factory;
        let cpu = context.cpu().expect("pinned launcher worker has a CPU");
        let seed = context.seed();
        run_server_worker_with_storage_and_context::<_, W, _, _>(
            input.server,
            driver_config(cpu),
            input.capacity,
            Some(seed),
            storage_factory(cpu, seed),
            Some(&context),
            input.worker,
        )
    }
}

fn run_server_worker<T, W, F>(
    server: T,
    driver_config: driver::Config,
    capacity: usize,
    worker: F,
) -> io::Result<()>
where
    W: Wire,
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd, ServerStorage<(), W>>) -> io::Result<()>,
{
    run_server_worker_with_context::<_, W, _>(server, driver_config, capacity, None, None, worker)
}

fn run_server_worker_with_context<T, W, F>(
    server: T,
    driver_config: driver::Config,
    capacity: usize,
    seed: Option<Seed>,
    worker_context: Option<&WorkerContext>,
    worker: F,
) -> io::Result<()>
where
    W: Wire,
    F: for<'scope, 'd> FnOnce(T, &mut Session<'scope, 'd, ServerStorage<(), W>>) -> io::Result<()>,
{
    let executor = match seed {
        Some(seed) => Executor::with_seed(driver_config, seed)?,
        None => Executor::new(driver_config)?,
    };
    let wire = W::connection_storage(capacity)?;
    executor
        .with_storage(ServerStorage {
            value: (),
            egress: egress::storage::Storage::default(),
            wire,
        })
        .enter(|mut session| {
            if let Some(context) = worker_context {
                context.try_register_shutdown(&mut session.driver_access())?;
            }
            worker(server, &mut session)
        })
}

fn run_server_worker_with_storage<T, W, S, F>(
    server: T,
    driver_config: driver::Config,
    capacity: usize,
    storage_factory: S,
    worker: F,
) -> io::Result<()>
where
    W: Wire,
    S: StorageFactory,
    F: for<'scope, 'd> FnOnce(
        T,
        &mut Session<'scope, 'd, ServerStorage<S::Output<'d>, W>>,
    ) -> io::Result<()>,
{
    run_server_worker_with_storage_and_context::<_, W, _, _>(
        server,
        driver_config,
        capacity,
        None,
        storage_factory,
        None,
        worker,
    )
}

fn run_server_worker_with_storage_and_context<T, W, S, F>(
    server: T,
    driver_config: driver::Config,
    capacity: usize,
    seed: Option<Seed>,
    storage_factory: S,
    worker_context: Option<&WorkerContext>,
    worker: F,
) -> io::Result<()>
where
    W: Wire,
    S: StorageFactory,
    F: for<'scope, 'd> FnOnce(
        T,
        &mut Session<'scope, 'd, ServerStorage<S::Output<'d>, W>>,
    ) -> io::Result<()>,
{
    let executor = match seed {
        Some(seed) => Executor::with_seed(driver_config, seed)?,
        None => Executor::new(driver_config)?,
    };
    let wire = W::connection_storage(capacity)?;
    executor
        .with_storage_factory(WithServerStorage {
            value: storage_factory,
            wire,
        })
        .enter(|mut session| {
            if let Some(context) = worker_context {
                context.try_register_shutdown(&mut session.driver_access())?;
            }
            worker(server, &mut session)
        })
}

trait ResourcePolicy<'d>: Sized {
    const ROUTE: Option<u8> = None;

    fn dispatch(self: Pin<&mut Self>, event: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        let _ = (event, driver);
    }

    fn activate(self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>) {
        let _ = (target, driver);
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let _ = driver;
    }

    fn idle(self: Pin<&Self>, region: &RegionToken<'d>) -> Idle {
        let _ = region;
        Idle::Park(None)
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let _ = driver;
    }
}

struct NoResource;

impl<'d> ResourcePolicy<'d> for NoResource {}

impl<'d, R> ResourcePolicy<'d> for R
where
    R: Manifold<'d>,
{
    const ROUTE: Option<u8> = Some(R::ID);

    fn dispatch(self: Pin<&mut Self>, event: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        Manifold::dispatch(self, event, driver);
    }

    fn activate(self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>) {
        let target =
            TypedToken::<R>::try_new::<'d>(target).expect("dispatcher selected the resource route");
        Manifold::activate(self, target, driver);
    }

    fn pre_park(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        Manifold::pre_park(self, driver);
    }

    fn idle(self: Pin<&Self>, region: &RegionToken<'d>) -> Idle {
        Manifold::idle(self, region)
    }

    fn shutdown(self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        Manifold::shutdown(self, driver);
    }
}

fn run<'scope, 'd: 'scope, const LISTENER_ID: u8, const DATE_ID: u8, A, W, P, R, S>(
    session: &mut Session<'scope, 'd, S>,
    app: A,
    listener: listener::config::Config<Tcp>,
    head_timeout: Duration,
    resource: R,
    wire: W::InitConfig<'d>,
    shutdown: Option<&ShutdownTrigger>,
) -> io::Result<()>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    W: Wire,
    P: RuntimeProfile,
    R: ResourcePolicy<'d>,
    S: EgressHost,
{
    let egress = session.storage().egress_storage();
    let hash_builder = session.seed().derive(ACCEPT ^ LISTENER_ID as u64).state();
    let listener = {
        let mut driver = session.driver_access();
        if let Some(trigger) = shutdown {
            trigger.try_register(&mut driver)?;
        }
        let listener =
            Listener::open_in_with_wire(app, listener, wire, hash_builder, egress, &mut driver)?;
        listener
            .handler()
            .timer()
            .bind(driver.timer(), listener.capacity())?;
        listener
    };
    listener.handler().timer().set_head_timeout(head_timeout);
    let dispatcher = pin!(cell::BrandCell::new(Dispatcher::<
        'd,
        LISTENER_ID,
        DATE_ID,
        A,
        Tcp,
        W,
        P,
        R,
    > {
        listener: TimedListener::new(listener),
        date: Updater::new(),
        resource,
    }));
    session.run(dispatcher.as_ref())
}

#[pin_project::pin_project]
struct Dispatcher<'d, const LISTENER_ID: u8, const DATE_ID: u8, A, T, W, P, R>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    T: Transport,
    W: Wire,
    P: RuntimeProfile,
    R: ResourcePolicy<'d>,
{
    #[pin]
    listener: TimedListener<'d, LISTENER_ID, A, Bundle<T, W, P>>,
    date: Updater<DATE_ID>,
    #[pin]
    resource: R,
}

impl<'d, const LISTENER_ID: u8, const DATE_ID: u8, A, T, W, P, R>
    Dispatcher<'d, LISTENER_ID, DATE_ID, A, T, W, P, R>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    T: Transport,
    W: Wire,
    P: RuntimeProfile,
    R: ResourcePolicy<'d>,
{
    const ROUTES_UNIQUE: () = match R::ROUTE {
        Some(resource_id) => {
            assert!(
                LISTENER_ID != DATE_ID && LISTENER_ID != resource_id && DATE_ID != resource_id,
                "listener, date, and resource manifolds require distinct route IDs"
            );
        }
        None => {
            assert!(
                LISTENER_ID != DATE_ID,
                "listener and date manifolds require distinct route IDs"
            );
        }
    };
}

impl<'d, const LISTENER_ID: u8, const DATE_ID: u8, A, T, W, P, R> dispatcher::Dispatcher<'d>
    for Dispatcher<'d, LISTENER_ID, DATE_ID, A, T, W, P, R>
where
    A: Application<'d, Wire = W> + DateHost + TimerHost<'d>,
    T: Transport,
    W: Wire,
    P: RuntimeProfile,
    R: ResourcePolicy<'d>,
{
    fn dispatch(mut self: Pin<&mut Self>, event: Event<'d>, driver: &mut DriverContext<'_, 'd>) {
        let _: () = Self::ROUTES_UNIQUE;
        let route = event.route();
        if route == LISTENER_ID {
            Manifold::dispatch(self.project().listener, event, driver);
        } else if route == DATE_ID {
            let mut fields = self.as_mut().project();
            let handler = fields.listener.as_mut().handler_mut();
            let stamp = DateHost::stamp(handler.as_ref());
            fields.date.dispatch(event, stamp.get_ref(), driver);
        } else if let Some(resource_id) = R::ROUTE
            && route == resource_id
        {
            ResourcePolicy::dispatch(self.project().resource, event, driver);
        }
    }

    fn activate(self: Pin<&mut Self>, target: Token, driver: &mut DriverContext<'_, 'd>) {
        let _: () = Self::ROUTES_UNIQUE;
        let route = target.route();
        if route == LISTENER_ID {
            let target =
                TypedToken::<TimedListener<'d, LISTENER_ID, A, Bundle<T, W, P>>>::try_new::<'d>(
                    target,
                )
                .expect("dispatcher selected the listener route");
            Manifold::activate(self.project().listener, target, driver);
        } else if let Some(resource_id) = R::ROUTE
            && route == resource_id
        {
            ResourcePolicy::activate(self.project().resource, target, driver);
        }
    }

    fn pre_park(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let fields = self.as_mut().project();
        Manifold::pre_park(fields.listener, driver);
        fields.date.pre_park(driver);
        if R::ROUTE.is_some() {
            ResourcePolicy::pre_park(fields.resource, driver);
        }
    }

    fn idle(self: Pin<&Self>, region: &RegionToken<'d>) -> Idle {
        let fields = self.project_ref();
        let idle = Manifold::idle(fields.listener, region).reduce(fields.date.idle());
        if R::ROUTE.is_some() {
            idle.reduce(ResourcePolicy::idle(fields.resource, region))
        } else {
            idle
        }
    }

    fn shutdown(mut self: Pin<&mut Self>, driver: &mut DriverContext<'_, 'd>) {
        let fields = self.as_mut().project();
        Manifold::shutdown(fields.listener, driver);
        fields.date.shutdown(driver);
        if R::ROUTE.is_some() {
            ResourcePolicy::shutdown(fields.resource, driver);
        }
    }
}

fn clone_listener_config(config: &listener::config::Config<Tcp>) -> listener::config::Config<Tcp> {
    listener::config::Config {
        max_connections: config.max_connections,
        bind: config.bind,
        backlog: config.backlog,
        stream: config.stream,
        transport: config.transport,
        egress: config.egress,
    }
}
