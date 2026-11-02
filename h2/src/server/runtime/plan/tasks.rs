use std::mem;

use dope::{
    core::{
        driver,
        driver::route::{Identity, KeyTag},
    },
    manifold::{
        self,
        app::{env, receive},
        network::listener::{self, handler},
        timing,
    },
    net::{
        tcp,
        wire::{self, domain},
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
    runtime::{Plan, startup::Failure},
    task::{self, works},
};

/// Generated tasks and their exact listener configuration.
#[must_use = "generated tasks must be consumed inside their storage loans"]
pub struct Tasks<ID: Identity, F, M = mode::Disabled> {
    plan: Plan<ID>,
    factory: F,
    task_cap: usize,
    mode: M,
}

impl<ID: Identity, F> Tasks<ID, F> {
    pub(in crate::server) const fn new(plan: Plan<ID>, factory: F, task_cap: usize) -> Self {
        Self {
            plan,
            factory,
            task_cap,
            mode: mode::Disabled,
        }
    }

    pub fn observe<O>(self, observer: O) -> Tasks<ID, F, mode::Observed<O>> {
        Tasks {
            plan: self.plan,
            factory: self.factory,
            task_cap: self.task_cap,
            mode: mode::Observed::new(observer),
        }
    }
}

impl<ID: Identity, F, M> Tasks<ID, F, M> {
    pub fn with_age(mut self, config: session::Config) -> Self {
        self.plan = self
            .plan
            .with_life(handler::Lifetime::graceful(config.age(), config.drain()));
        self
    }
    /// Consumes the installed listener before its backing storage is released.
    pub fn try_install<'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, Q, N, R>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, (), Q>,
        next: N,
    ) -> Result<R, manifold::BuildError<Failure<F::Error>>>
    where
        ID: 'd,
        F: Bind<'d, Target = Target<'d, ID>>,
        Q: executor::Startup,
        <<F::Runtime as decode::Decode>::Head as request::Head>::Start: works::Family,
        task::App<'d, ID, F::Runtime, wire::Identity, M>: handler::Application<'d, ID, Wire = wire::Identity, Input = receive::Scoped>
            + handler::Runtime<'d, 'd, ID>,
        N: for<'n, 'p, 's> FnOnce(
            listener::Listener<
                'n,
                'p,
                's,
                'd,
                'd,
                ID,
                task::App<'d, ID, F::Runtime, wire::Identity, M>,
                env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
            >,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, (), Q>,
        ) -> R,
    {
        self.build(stage, task::App::try_bind, next)
    }
    fn build<'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, Q, A, C, N, R>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, (), Q>,
        make: C,
        next: N,
    ) -> Result<R, manifold::BuildError<Failure<F::Error>>>
    where
        ID: 'd,
        F: Bind<'d, Target = Target<'d, ID>>,
        Q: executor::Startup,
        A: handler::Application<'d, ID, Wire = wire::Identity, Input = receive::Scoped>
            + handler::Runtime<'d, 'd, ID>,
        C: FnOnce(F, task::Config, M, driver::Binding<'d>) -> Result<A, Failure<F::Error>>,
        N: for<'n, 'p, 's> FnOnce(
            listener::Listener<
                'n,
                'p,
                's,
                'd,
                'd,
                ID,
                A,
                env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
            >,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, (), Q>,
        ) -> R,
    {
        let Self {
            plan,
            factory,
            task_cap,
            mode,
        } = self;
        let (listener, proto, bound, lifetime) = plan.into_parts();
        let install = listener::Install::<
            ID,
            env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
            _,
        >::new(
            listener,
            domain::Local::<wire::Identity, ID>::new(wire::Identity),
        )
        .with_bound(bound);
        let install = match lifetime {
            Some(lifetime) => install.with_life(lifetime),
            None => install,
        };
        install.build_in(
            stage,
            |binding| make(factory, task::Config::new(proto, task_cap), mode, binding),
            next,
        )
    }
}

impl<ID: Identity, F, M> Tasks<ID, F, M> {
    /// Consumes the installed listener before its backing storage is released.
    pub fn try_due<'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, Q, N, R>(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, (), Q>,
        timer: config::Config,
        next: N,
    ) -> Result<R, manifold::BuildError<Failure<F::Error>>>
    where
        ID: 'd,
        F: Bind<'d, Target = Target<'d, ID>>,
        Q: executor::Startup,
        <<F::Runtime as decode::Decode>::Head as request::Head>::Start: works::Family,
        task::App<'d, ID, F::Runtime, wire::Identity, M, config::Config>: handler::Application<'d, ID, Wire = wire::Identity, Input = receive::Scoped>
            + handler::Runtime<'d, 'd, ID>,
        N: for<'n, 'p, 's> FnOnce(
            listener::Listener<
                'n,
                'p,
                's,
                'd,
                'd,
                ID,
                task::App<'d, ID, F::Runtime, wire::Identity, M, config::Config>,
                env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
            >,
            &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, (), Q>,
        ) -> R,
    {
        self.build(
            stage,
            |factory, config, mode, binding| {
                task::App::try_bind(factory, config, mode, binding).map(|app| app.with_timer(timer))
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
