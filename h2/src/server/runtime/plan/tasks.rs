use std::mem;

use dope::{
    core::driver::{
        self,
        route::{Identity, KeyTag},
        storage,
    },
    manifold::{
        self,
        app::{creation, env, receive},
        network::listener::{self, handler, handler::failures},
        timing,
    },
    net::{
        link::{
            egress::data,
            pool::{change, recv::input},
        },
        tcp,
    },
    runtime::{executor, topology::lending},
};
use sark_core::{
    decode::{self, request},
    life::{mode, session},
    manifold::task::app::Bind,
};

use crate::server::{
    self, Target,
    runtime::{Plan, app::Report, startup::Failure},
    task::{self, service, works},
};

type Bound<'n, 'p, 's, 'w, 'd, ID, R, W, M, D, P, A = service::Native<R>> = listener::Listener<
    'n,
    'p,
    's,
    'w,
    'd,
    ID,
    task::App<'d, ID, R, W, M, D, P, A>,
    env::Bundle<tcp::Tcp, W, timing::Throughput>,
>;

/// Listener storage prepared before a provider issues the application's clients.
pub struct Prepared<'n, 'p, 's, 'w, 'd: 'w, ID: Identity, W: input::Execution, B, E, M, P>
where
    E: data::raw::Payload<'d> + 'd,
{
    listener: listener::Prepared<
        'n,
        'p,
        's,
        'w,
        'd,
        ID,
        env::Bundle<tcp::Tcp, W, timing::Throughput>,
        B,
        E,
    >,
    protocol: server::Plan,
    task_cap: usize,
    mode: M,
    fault: P,
}

impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity + 'd, W, B, E, M, P>
    Prepared<'n, 'p, 's, 'w, 'd, ID, W, B, E, M, P>
where
    W: input::Execution + 'd,
    B: change::Prepare<'w, 'd, ID, W>,
    E: data::raw::Payload<'d> + 'd,
{
    /// Binds the late factory and consumes its exact listener storage.
    pub fn build<F, D>(
        self,
        factory: F,
        deadlines: D,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<
        Bound<'n, 'p, 's, 'w, 'd, ID, F::Runtime, W, M, D, P>,
        manifold::BuildError<Failure<F::Error>>,
    >
    where
        F: Bind<'d, Target = Target<'d, ID>>,
        <<F::Runtime as decode::Decode>::Head as request::Head>::Start: works::Family,
        task::App<'d, ID, F::Runtime, W, M, D, P>: handler::Application<'d, ID, Wire = W, Input = receive::Scoped, Egress = E>
            + handler::Runtime<'w, 'd, ID>,
    {
        self.bind(factory, deadlines, service::Native::new(), driver)
    }

    fn bind<F, D, A>(
        self,
        factory: F,
        deadlines: D,
        service: A,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<
        Bound<'n, 'p, 's, 'w, 'd, ID, F::Runtime, W, M, D, P, A>,
        manifold::BuildError<Failure<F::Error>>,
    >
    where
        F: Bind<'d, Target = Target<'d, ID>>,
        <<F::Runtime as decode::Decode>::Head as request::Head>::Start: works::Family,
        A: service::Select<F::Runtime>,
        task::App<'d, ID, F::Runtime, W, M, D, P, A>: handler::Application<'d, ID, Wire = W, Input = receive::Scoped, Egress = E>
            + handler::Runtime<'w, 'd, ID>,
    {
        let binding = self.listener.binding(driver.driver_ref());
        let app = task::App::bind(
            factory,
            self.protocol,
            self.task_cap,
            self.mode,
            deadlines,
            self.fault,
            binding,
            service,
        )
        .map_err(manifold::BuildError::Factory)?;
        self.listener
            .build(app, driver)
            .map_err(manifold::BuildError::Create)
    }
}

impl<'n, 'p, 's, 'w, 'd: 'w, ID: Identity + 'd, W, B, E, P>
    Prepared<'n, 'p, 's, 'w, 'd, ID, W, B, E, mode::Disabled, P>
where
    W: input::Execution + 'd,
    B: change::Prepare<'w, 'd, ID, W>,
    E: data::raw::Payload<'d> + 'd,
{
    /// Installs a companion with independent HTTP and service deadline policies.
    pub fn build_with<F, D, A>(
        self,
        factory: F,
        deadlines: D,
        service: A,
        driver: &mut driver::Context<'_, 'd>,
    ) -> Result<
        Bound<'n, 'p, 's, 'w, 'd, ID, F::Runtime, W, mode::Disabled, D, P, A>,
        manifold::BuildError<Failure<F::Error>>,
    >
    where
        F: Bind<'d, Target = Target<'d, ID>>,
        <<F::Runtime as decode::Decode>::Head as request::Head>::Start: works::Family,
        A: service::Select<F::Runtime>,
        task::App<'d, ID, F::Runtime, W, mode::Disabled, D, P, A>: handler::Application<'d, ID, Wire = W, Input = receive::Scoped, Egress = E>
            + handler::Runtime<'w, 'd, ID>,
    {
        self.bind(factory, deadlines, service, driver)
    }
}

/// Generated tasks and their exact listener configuration.
#[must_use = "generated tasks must be consumed inside their storage loans"]
pub struct Tasks<ID: Identity, F, M = mode::Disabled, P = failures::Close> {
    plan: Plan<ID>,
    factory: F,
    task_cap: usize,
    mode: M,
    fault: P,
}

impl<ID: Identity, F> Tasks<ID, F> {
    pub(in crate::server) const fn new(plan: Plan<ID>, factory: F, task_cap: usize) -> Self {
        Self {
            plan,
            factory,
            task_cap,
            mode: mode::Disabled,
            fault: failures::Close,
        }
    }
}

impl<ID: Identity, F, P> Tasks<ID, F, mode::Disabled, P> {
    pub fn observe<O>(self, observer: O) -> Tasks<ID, F, mode::Observed<O>, P> {
        Tasks {
            plan: self.plan,
            factory: self.factory,
            task_cap: self.task_cap,
            mode: mode::Observed::new(observer),
            fault: self.fault,
        }
    }
}

impl<ID: Identity, F, M> Tasks<ID, F, M> {
    /// Consumes exact wire failures separately from task lifecycle diagnostics.
    /// Fallible wires, including TLS, require a reporter.
    pub fn report<C>(self, report: C) -> Tasks<ID, F, M, Report<C>> {
        Tasks {
            plan: self.plan,
            factory: self.factory,
            task_cap: self.task_cap,
            mode: self.mode,
            fault: Report::new(report),
        }
    }
}

impl<ID: Identity, M, P> Tasks<ID, (), M, P> {
    /// Opens the storage loans while the generated factory is still unbound.
    pub fn prepare_in<'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, W, B, E, S, Q, N, R>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        wire: B,
        next: N,
    ) -> Result<R, creation::Error>
    where
        ID: 'd,
        W: input::Execution + 'd,
        B: change::Prepare<'life, 'd, ID, W>,
        E: data::raw::Payload<'d> + 'd,
        S: storage::Plan,
        Q: executor::Startup,
        N: for<'n, 'p, 's> FnOnce(
            Prepared<'n, 'p, 's, 'life, 'd, ID, W, B, E, M, P>,
            listener::Wait<'p, 'd>,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        ) -> R,
    {
        let (install, protocol) = self.plan.into_install(wire);
        let hash = stage.hash_state(listener::Domain::for_listener(ID::ID.raw()));
        install.prepare(hash, stage, lending::Stage::driver, |listener, wait, stage| {
            next(
                Prepared {
                    listener,
                    protocol,
                    task_cap: self.task_cap,
                    mode: self.mode,
                    fault: self.fault,
                },
                wait,
                stage,
            )
        })
    }
}

impl<ID: Identity, F, M, P> Tasks<ID, F, M, P> {
    pub fn with_age(mut self, config: session::Config) -> Self {
        self.plan = self
            .plan
            .with_life(handler::Lifetime::graceful(config.age(), config.drain()));
        self
    }
    /// Consumes the installed listener before its backing storage is released.
    pub fn try_install<'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, W, B, S, Q, D, N, R>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        wire: B,
        deadlines: D,
        next: N,
    ) -> Result<R, manifold::BuildError<Failure<F::Error>>>
    where
        ID: 'd,
        F: Bind<'d, Target = Target<'d, ID>>,
        W: input::Execution + 'd,
        B: change::Prepare<'life, 'd, ID, W>,
        S: storage::Plan,
        Q: executor::Startup,
        <<F::Runtime as decode::Decode>::Head as request::Head>::Start: works::Family,
        task::App<'d, ID, F::Runtime, W, M, D, P>: handler::Application<'d, ID, Wire = W, Input = receive::Scoped>
            + handler::Runtime<'life, 'd, ID>,
        N: for<'n, 'p, 's> FnOnce(
            Bound<'n, 'p, 's, 'life, 'd, ID, F::Runtime, W, M, D, P>,
            listener::Wait<'p, 'd>,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        ) -> R,
    {
        let Self {
            plan,
            factory,
            task_cap,
            mode,
            fault,
        } = self;
        let (install, proto) = plan.into_install(wire);
        install.build_in(
            stage,
            |binding| {
                task::App::bind(
                    factory,
                    proto,
                    task_cap,
                    mode,
                    deadlines,
                    fault,
                    binding,
                    service::Native::new(),
                )
            },
            next,
        )
    }
}

const _: () = {
    assert!(mem::size_of::<Tasks<KeyTag<0>, ()>>() == mem::size_of::<(Plan<KeyTag<0>>, usize)>());
    assert!(mem::align_of::<Tasks<KeyTag<0>, ()>>() == mem::align_of::<(Plan<KeyTag<0>>, usize)>());
    assert!(
        mem::size_of::<Tasks<KeyTag<0>, (), mode::Observed<()>>>()
            == mem::size_of::<(Plan<KeyTag<0>>, usize)>()
    );
};
