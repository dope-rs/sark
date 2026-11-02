use std::convert;

use dope::{
    core,
    core::driver::{
        self,
        route::{Identity, KeyTag},
        storage,
    },
    manifold::{
        app::receive,
        network::listener::{
            conn,
            handler::{Application, Runtime, failures},
            topology::factory,
        },
    },
    net::{link::pool::recv::input, wire},
    runtime::topology::recipe,
};
use o3::buffer::{resident, resident::bound::Unbounded};
use sark_core::manifold::server::app;
use sark_h2::{
    ServerRole,
    conn::{event, machine},
    server,
    server::runtime::{app::App, modes},
};
use sark_http::field::metadata::section::rules;

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
            machine::Connection<
                ServerRole,
                section::Plan,
                resident::Shared<'d>,
                rules::RejectPush,
                event::Ignore,
            >,
            conn::Id<'d, ID>,
            Fields = section::Plan,
            Failure = run::Failure,
            Egress<'d> = Unbounded,
        >,
    for<'d> App<run::Service<'a, direct::Policy<H, active::Entry<'a>>>, W, F>:
        Runtime<'d, 'd, ID> + Application<'d, ID, Wire = W, Input = receive::Scoped>,
{
    type Application<'d>
        = App<run::Service<'a, direct::Policy<H, active::Entry<'a>>>, W, F>
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

impl<'a, H: Handler> modes::Family<storage::Value<H>, KeyTag<0>>
    for Plan<'a, recipe::Borrow, mode::Demand<mode::UnaryTag>>
{
    type Failure = <modes::Serve<mode::Error> as modes::Failure>::Error;

    type Application<'s, 'life, 'p, 'd>
        = App<run::Service<'a, unary::Policy<'s, 'a, 'life, 'd, H>>, wire::Identity>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        storage::Value<H>: 'd;

    fn with<'life, 'p, 'd, O>(
        built: Self::Output<'life, 'p, 'd>,
        conn: server::Plan,
        binding: driver::Binding<'d>,
        run: impl for<'s> FnOnce(Self::Application<'s, 'life, 'p, 'd>) -> O,
    ) -> Result<O, Self::Failure>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        storage::Value<H>: 'd,
    {
        let (app, bank, exec) = built.into_parts();
        let capacity = exec.capacity();
        executor::with(capacity, binding.owners(), |tasks| {
            let policy = unary::Policy::new(app, tasks);
            let app = run::Service::new(policy, bank);
            let app = App::new(app, conn, failures::Close).map_err(core::BranchError::Second)?;
            Ok(run(app))
        })
        .map_err(|cause| mode::Error::Task {
            requested: capacity,
            cause,
        })
        .map_err(core::BranchError::First)?
    }
}

impl<'a, H: stream::Handler> modes::Family<storage::Value<H>, KeyTag<0>>
    for Plan<'a, recipe::Borrow, mode::Demand<mode::StreamTag>>
{
    type Failure = <modes::Serve<mode::Error> as modes::Failure>::Error;

    type Application<'s, 'life, 'p, 'd>
        = App<run::Service<'a, Policy<'s, 'a, 'life, 'd, H>>, wire::Identity>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        storage::Value<H>: 'd;

    fn with<'life, 'p, 'd, O>(
        built: Self::Output<'life, 'p, 'd>,
        conn: server::Plan,
        binding: driver::Binding<'d>,
        run: impl for<'s> FnOnce(Self::Application<'s, 'life, 'p, 'd>) -> O,
    ) -> Result<O, Self::Failure>
    where
        'd: 'p,
        'p: 'life,
        'd: 'life,
        Self: 'd,
        storage::Value<H>: 'd,
    {
        let (app, bank, exec) = built.into_parts();
        let capacity = exec.capacity();
        executor::with(capacity, binding.owners(), |tasks| {
            let policy = Policy::new(app, tasks);
            let app = run::Service::new(policy, bank);
            let app = App::new(app, conn, failures::Close).map_err(core::BranchError::Second)?;
            Ok(run(app))
        })
        .map_err(|cause| mode::Error::Task {
            requested: capacity,
            cause,
        })
        .map_err(core::BranchError::First)?
    }
}
