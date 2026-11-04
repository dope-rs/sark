use std::mem;

use dope::{
    core::{
        driver,
        driver::{
            route::{Identity, KeyTag},
            storage,
        },
    },
    manifold::{
        self,
        app::{env, receive},
        network::listener::{self, handler, handler::failures},
        timing,
    },
    net::{
        link::pool::{change, recv::input},
        tcp,
    },
    runtime::{executor, topology::lending},
};
use sark_core::{
    deadline::config,
    decode::{self, request},
    life::{mode, session},
    manifold::task::app::Bind,
};

use crate::server::{
    Target,
    runtime::{Plan, app::Report, startup::Failure},
    task::{self, works},
};

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

impl<ID: Identity, F, M, P> Tasks<ID, F, M, P> {
    pub fn with_age(mut self, config: session::Config) -> Self {
        self.plan = self
            .plan
            .with_life(handler::Lifetime::graceful(config.age(), config.drain()));
        self
    }
    /// Consumes the installed listener before its backing storage is released.
    pub fn try_install<'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, W, B, S, Q, N, R>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        wire: B,
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
        task::App<'d, ID, F::Runtime, W, M, task::timers::Disabled, P>: handler::Application<'d, ID, Wire = W, Input = receive::Scoped>
            + handler::Runtime<'life, 'd, ID>,
        N: for<'n, 'p, 's> FnOnce(
            listener::Listener<
                'n,
                'p,
                's,
                'life,
                'd,
                ID,
                task::App<'d, ID, F::Runtime, W, M, task::timers::Disabled, P>,
                env::Bundle<tcp::Tcp, W, timing::Throughput>,
            >,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        ) -> R,
    {
        self.build(stage, wire, task::App::try_bind, next)
    }
    fn build<'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, W, B, S, Q, A, C, N, R>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        wire: B,
        make: C,
        next: N,
    ) -> Result<R, manifold::BuildError<Failure<F::Error>>>
    where
        ID: 'd,
        F: Bind<'d, Target = Target<'d, ID>>,
        W: input::Execution + 'd,
        B: change::Prepare<'life, 'd, ID, W>,
        S: storage::Plan,
        Q: executor::Startup,
        A: handler::Application<'d, ID, Wire = W, Input = receive::Scoped>
            + handler::Runtime<'life, 'd, ID>,
        C: FnOnce(F, task::Config, M, P, driver::Binding<'d>) -> Result<A, Failure<F::Error>>,
        N: for<'n, 'p, 's> FnOnce(
            listener::Listener<
                'n,
                'p,
                's,
                'life,
                'd,
                ID,
                A,
                env::Bundle<tcp::Tcp, W, timing::Throughput>,
            >,
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
        let (listener, proto, bound, lifetime) = plan.into_parts();
        let install =
            listener::Install::<ID, env::Bundle<tcp::Tcp, W, timing::Throughput>, _>::new(
                listener, wire,
            )
            .with_bound(bound);
        let install = match lifetime {
            Some(lifetime) => install.with_life(lifetime),
            None => install,
        };
        install.build_in(
            stage,
            |binding| {
                make(
                    factory,
                    task::Config::new(proto, task_cap),
                    mode,
                    fault,
                    binding,
                )
            },
            next,
        )
    }
}

impl<ID: Identity, F, M, P> Tasks<ID, F, M, P> {
    /// Consumes the installed listener before its backing storage is released.
    pub fn try_due<'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, W, B, S, Q, N, R>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        wire: B,
        timer: config::Config,
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
        task::App<'d, ID, F::Runtime, W, M, config::Config, P>: handler::Application<'d, ID, Wire = W, Input = receive::Scoped>
            + handler::Runtime<'life, 'd, ID>,
        N: for<'n, 'p, 's> FnOnce(
            listener::Listener<
                'n,
                'p,
                's,
                'life,
                'd,
                ID,
                task::App<'d, ID, F::Runtime, W, M, config::Config, P>,
                env::Bundle<tcp::Tcp, W, timing::Throughput>,
            >,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        ) -> R,
    {
        self.build(
            stage,
            wire,
            |factory, config, mode, fault, binding| {
                task::App::try_bind(factory, config, mode, fault, binding)
                    .map(|app| app.with_timer(timer))
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
