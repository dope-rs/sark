use std::convert;

use dope::{
    core,
    core::driver::{
        route::{Identity, KeyTag},
        storage,
    },
    manifold::{
        self,
        app::{Host, env, receive},
        network::listener::{
            self, conn,
            handler::{Application, Runtime, failures},
            topology::factory,
        },
        timing,
    },
    net::{link::pool::recv::input, tcp, wire},
    runtime::{
        executor::Startup,
        topology::{lending, recipe},
    },
};
use o3::buffer::resident::bound::Unbounded;
use sark_h2::server::app;
use sark_h2::{
    conn::event,
    server,
    server::runtime::{app::App, modes},
};

use self::{
    runtime::{direct, executor, prepare::Prepared, run, stream::Policy, unary},
    service::{
        handler,
        task::{stream, unary::Handler},
    },
};
use crate::{protocol::section, server::runtime::state::active};

mod bank;
#[doc(hidden)]
pub mod call;
pub mod calls;
pub mod config;
pub(in crate::server) mod egress;
#[doc(hidden)]
pub mod mode;
#[doc(hidden)]
pub mod runtime;
pub mod service;

pub use bank::Bank;
pub(in crate::server) use bank::lease::Lease;

/// A service recipe paired with its gRPC limits and server mode.
#[must_use = "a gRPC plan must be built by a protocol host"]
pub struct Plan<'a, F, M, R = failures::Close> {
    recipe: F,
    bank: &'a Bank,
    mode: M,
    report: R,
}

impl<'a, F, M> Plan<'a, F, M> {
    pub const fn new(recipe: F, bank: &'a Bank, mode: M) -> Self {
        Self {
            recipe,
            bank,
            mode,
            report: failures::Close,
        }
    }

    pub fn report<R>(self, report: R) -> Plan<'a, F, M, R> {
        Plan {
            recipe: self.recipe,
            bank: self.bank,
            mode: self.mode,
            report,
        }
    }
}

impl<'a, S, F, M> recipe::Build<S> for Plan<'a, F, M>
where
    S: storage::Plan,
    F: recipe::Build<S>,
{
    type Output<'life, 'p, 'd>
        = Prepared<'a, F::Output<'life, 'p, 'd>, M>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd;
    type Error = F::Error;

    fn build<'life, 'p, 'd>(
        self,
        store: &'life S::Output<'p, 'd>,
    ) -> Result<Self::Output<'life, 'p, 'd>, Self::Error>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        S: 'd,
    {
        let app = self.recipe.build(store)?;
        Ok(Prepared::new(app, self.bank, self.mode))
    }
}

impl<'a, H, W, F, ID: Identity> modes::Direct<modes::Serve<convert::Infallible>, W, ID>
    for Plan<'a, H, mode::Direct, F>
where
    H: handler::Handler,
    W: input::Execution,
    for<'d> run::Service<'a, direct::Policy<H, active::Entry<'a>>>: app::Application<
            'd,
            event::Ignore,
            conn::Id<'d, ID>,
            Fields = section::Plan,
            Failure = run::Failure,
            Egress<'d> = Unbounded,
        >,
    for<'d> App<'d, run::Service<'a, direct::Policy<H, active::Entry<'a>>>, W, F>:
        Runtime<'d, 'd, ID> + Application<'d, ID, Wire = W, Input = receive::Scoped<true>>,
{
    type Application<'d>
        = App<'d, run::Service<'a, direct::Policy<H, active::Entry<'a>>>, W, F>
    where
        Self: 'd,
        W: 'd,
        ID: 'd,
        <modes::Serve<convert::Infallible> as factory::ReceiveSpec>::Input: 'd;

    fn build<'d>(
        self,
        conn: server::Plan,
    ) -> Result<Self::Application<'d>, <modes::Serve<convert::Infallible> as modes::Failure>::Error>
    where
        Self: 'd,
        W: 'd,
        ID: 'd,
        <modes::Serve<convert::Infallible> as factory::ReceiveSpec>::Input: 'd,
    {
        let app = self.mode.build(self.recipe, self.bank);
        App::new(app, conn, self.report).map_err(core::BranchError::Second)
    }
}

impl<'a, 'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, H, Q, N, O>
    modes::Family<lending::Stage<'stage, 'scope, 'life, 'p, 'd, storage::Value<H>, Q>, N, KeyTag<0>>
    for Plan<'a, recipe::Borrow, mode::Demand<mode::UnaryTag>>
where
    H: Handler + 'd,
    Self: 'd,
    Q: Startup,
    N: for<'s, 'n, 'r, 'f> lending::Next<
            lending::Stage<'stage, 'scope, 'life, 'p, 'd, storage::Value<H>, Q>,
            Host<
                listener::Listener<
                    'n,
                    'r,
                    'f,
                    'd,
                    'd,
                    KeyTag<0>,
                    App<'d, run::Service<'a, unary::Policy<'s, 'a, 'life, 'd, H>>, wire::Identity>,
                    env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
                >,
            >,
            Output = O,
        >,
{
    type Output = O;
    type Error = manifold::BuildError<
        core::BranchError<
            <Self as recipe::Build<storage::Value<H>>>::Error,
            <modes::Serve<mode::Error> as modes::Failure>::Error,
        >,
    >;

    fn install(
        self,
        plan: sark_h2::server::runtime::Plan<KeyTag<0>>,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, storage::Value<H>, Q>,
        next: N,
    ) -> Result<O, Self::Error> {
        let built = recipe::Build::<storage::Value<H>>::build(self, stage.storage())
            .map_err(core::BranchError::First)
            .map_err(manifold::BuildError::Factory)?;
        let (app, bank, exec) = built.into_parts();
        let (install, conn) = plan.into_install(wire::domain::Local::new(wire::Identity));
        let binding = install.binding(stage.driver().driver_ref());
        let capacity = exec.capacity();
        executor::with(capacity, binding.owners(), |tasks| {
            let policy = unary::Policy::new(app, tasks);
            let app = run::Service::new(policy, bank);
            let app = App::new(app, conn, failures::Close)
                .map_err(core::BranchError::Second)
                .map_err(core::BranchError::Second)
                .map_err(manifold::BuildError::Factory)?;
            install
                .in_stage(stage, app, |listener, _wait, stage| {
                    next.run(Host::new(listener), stage)
                })
                .map_err(manifold::BuildError::Create)
        })
        .map_err(|cause| mode::Error::Task {
            requested: capacity,
            cause,
        })
        .map_err(core::BranchError::First)
        .map_err(core::BranchError::Second)
        .map_err(manifold::BuildError::Factory)?
    }
}

impl<'a, 'stage, 'scope, 'life: 'scope, 'p: 'life, 'd: 'p, H, Q, N, O>
    modes::Family<lending::Stage<'stage, 'scope, 'life, 'p, 'd, storage::Value<H>, Q>, N, KeyTag<0>>
    for Plan<'a, recipe::Borrow, mode::Demand<mode::StreamTag>>
where
    H: stream::Handler + 'd,
    Self: 'd,
    Q: Startup,
    N: for<'s, 'n, 'r, 'f> lending::Next<
            lending::Stage<'stage, 'scope, 'life, 'p, 'd, storage::Value<H>, Q>,
            Host<
                listener::Listener<
                    'n,
                    'r,
                    'f,
                    'd,
                    'd,
                    KeyTag<0>,
                    App<'d, run::Service<'a, Policy<'s, 'a, 'life, 'd, H>>, wire::Identity>,
                    env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
                >,
            >,
            Output = O,
        >,
{
    type Output = O;
    type Error = manifold::BuildError<
        core::BranchError<
            <Self as recipe::Build<storage::Value<H>>>::Error,
            <modes::Serve<mode::Error> as modes::Failure>::Error,
        >,
    >;

    fn install(
        self,
        plan: sark_h2::server::runtime::Plan<KeyTag<0>>,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'p, 'd, storage::Value<H>, Q>,
        next: N,
    ) -> Result<O, Self::Error> {
        let built = recipe::Build::<storage::Value<H>>::build(self, stage.storage())
            .map_err(core::BranchError::First)
            .map_err(manifold::BuildError::Factory)?;
        let (app, bank, exec) = built.into_parts();
        let (install, conn) = plan.into_install(wire::domain::Local::new(wire::Identity));
        let binding = install.binding(stage.driver().driver_ref());
        let capacity = exec.capacity();
        executor::with(capacity, binding.owners(), |tasks| {
            let policy = Policy::new(app, tasks);
            let app = run::Service::new(policy, bank);
            let app = App::new(app, conn, failures::Close)
                .map_err(core::BranchError::Second)
                .map_err(core::BranchError::Second)
                .map_err(manifold::BuildError::Factory)?;
            install
                .in_stage(stage, app, |listener, _wait, stage| {
                    next.run(Host::new(listener), stage)
                })
                .map_err(manifold::BuildError::Create)
        })
        .map_err(|cause| mode::Error::Task {
            requested: capacity,
            cause,
        })
        .map_err(core::BranchError::First)
        .map_err(core::BranchError::Second)
        .map_err(manifold::BuildError::Factory)?
    }
}
