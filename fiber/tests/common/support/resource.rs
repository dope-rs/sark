use core::BranchError;
use std::{convert, time::Duration};

use dope::{
    core::{
        self,
        driver::{
            self, route,
            route::{KeyTag, indices::SlotIndex},
            settings,
            settings::{claim, errors},
            storage,
        },
    },
    manifold::{
        self,
        app::{creation, dispatch},
        timing::{interval, interval::Component},
    },
    net::{wire, wire::domain},
    runtime::{
        client::{Bound, Context, Provider},
        executor::{self, drive, session},
        topology::{self, lending},
    },
};
use dope_tls::tls::{self, endpoint, roles};
use sark_fiber::{Config, Throughput};
use sark_h1::server::{date, listener::bind, run};
use support::{Routes, server};

use crate::common::support;
type ID = KeyTag<2>;

#[pin_project::pin_project]
#[derive(dope_gen::Application)]
pub struct Tick<'d> {
    #[pin]
    #[manifold]
    pub interval: interval::Interval<'d, ID>,
    #[dispatcher(state)]
    pub target: route::Target<'d, ID>,
}

// SAFETY: Tick forwards shutdown and quiescence to its sole pinned interval.
// Its Target is an identity-only Copy value; clients own no native work.
// The interval's Resume proof covers timer retirement and subsequent release.
unsafe impl<'d> lending::raw::Resume<'d> for Tick<'d> where
    interval::Interval<'d, ID>: dispatch::raw::Resume<'d>
{
}

pub struct Install(interval::Component<ID>);

impl Default for Install {
    fn default() -> Self {
        Self(Component::every_second())
    }
}

impl settings::Resource for Install {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        settings::Resource::include_in(&self.0, needs)
    }
}

impl<S: storage::Plan> topology::Installer<S> for Install {
    type App<'life, 'p, 'd>
        = Tick<'d>
    where
        'd: 'p,
        'p: 'life,
        Self: 'd,
        S: 'd;
    type Error = creation::Error;

    fn install<'scope, 'life, 'p, 'd, Q>(
        self,
        stage: &mut topology::Stage<'_, 'scope, 'life, 'p, 'd, S, Q>,
    ) -> Result<Self::App<'life, 'p, 'd>, Self::Error>
    where
        'd: 'p,
        'p: 'life,
        'life: 'scope,
        Self: 'd,
        S: 'd,
        Q: executor::Startup,
    {
        let mut driver = stage.driver();
        let target = driver
            .driver_ref()
            .targets::<ID>()
            .bind(SlotIndex::ZERO, route::Epoch::INITIAL);
        Ok(Tick {
            interval: interval::Interval::<ID>::every_second(&mut driver)?,
            target,
        })
    }
}

pub struct Guard<'app, 'd: 'app>(Bound<'app, 'd, Tick<'d>, route::Target<'d, ID>>);

impl<'d> Provider<'d> for Tick<'d> {
    type Client<'app>
        = Guard<'app, 'd>
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        let (provider, bound) = context.into_parts();
        Guard(bound.bind(provider.get_ref().target))
    }
}

type Driven = (Result<(), drive::Error>, bool);
type Run<'report> = Result<Driven, lending::Error<'report, convert::Infallible, Driven>>;

struct Serve<'d, A> {
    app: A,
    target: route::Target<'d, ID>,
}

impl<'d, A: executor::raw::App<'d>> lending::Build<'d, Tick<'d>> for Serve<'d, A> {
    type App<'app>
        = A
    where
        Self: 'app,
        'd: 'app;
    type Data = bool;
    type Error = convert::Infallible;

    fn build<'app>(
        self,
        guard: Guard<'app, 'd>,
        _: driver::Context<'_, 'd>,
    ) -> Result<(Self::App<'app>, Self::Data), Self::Error>
    where
        Self: 'app,
        'd: 'app,
    {
        Ok((self.app, *guard.0 == self.target))
    }
}

impl<'life, 'd: 'life, A, Q> lending::Execute<'life, 'd, Tick<'d>, Q> for Serve<'d, A>
where
    A: executor::raw::App<'d>,
{
    type Output = Driven;

    fn run<'app, 'run>(
        matched: Self::Data,
        app: session::App<'run, 'life, 'd, lending::raw::Bound<'app, 'd, Tick<'d>, A>, Q>,
    ) -> Self::Output
    where
        Self: 'app,
        'd: 'app,
        'app: 'run,
        'life: 'run,
    {
        (app.run().map(|_| ()), matched)
    }
}

pub struct Plain<'state, F> {
    pub plan: run::Plan<KeyTag<0>, KeyTag<1>, Throughput>,
    pub state: server::State<'state>,
    pub finish: F,
}

impl<F> settings::Resource for Plain<'_, F> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        settings::Resource::include_in(&self.plan, needs)
    }
}

impl<'scope, 'life: 'scope, 'e: 'life, 'd: 'e, Q, F>
    lending::Compose<'scope, 'life, 'e, 'd, storage::With<(), date::Clock>, Q, Tick<'d>>
    for Plain<'_, F>
where
    Q: executor::Startup,
    F: for<'report> FnOnce(Run<'report>),
{
    type Output = Result<(), BranchError<convert::Infallible, manifold::BuildError<bind::Error>>>;

    fn compose(
        self,
        owner: Tick<'d>,
        stage: &mut lending::Stage<'_, 'scope, 'life, 'e, 'd, storage::With<(), date::Clock>, Q>,
    ) -> Self::Output {
        let mut finish = session::Completion::new();
        let target = owner.target;
        let app = Routes::new(&self.state, Config::new(server::MAX_CONNECTIONS));
        self.plan
            .install(
                stage,
                domain::Local::<wire::Identity, KeyTag<0>>::new(wire::Identity),
                app,
                |root, _wait, stage| {
                    let build = Serve { app: root, target };
                    (self.finish)(stage.lend_for(
                        &mut finish,
                        owner,
                        build,
                        Duration::from_secs(2),
                    ));
                },
            )
            .map_err(BranchError::Second)
    }
}

pub struct Tls<'state, F> {
    pub plan: run::Plan<KeyTag<0>, KeyTag<1>, Throughput>,
    pub wire: endpoint::Config<roles::Server>,
    pub state: server::State<'state>,
    pub finish: F,
}

impl<F> settings::Resource for Tls<'_, F> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        settings::Resource::include_in(&self.plan, needs)
    }
}

impl<'scope, 'life: 'scope, 'e: 'life, 'd: 'e, Q, F>
    lending::Compose<
        'scope,
        'life,
        'e,
        'd,
        storage::With<
            storage::Value<domain::Owner<tls::Tls<roles::Server>, KeyTag<0>>>,
            date::Clock,
        >,
        Q,
        Tick<'d>,
    > for Tls<'_, F>
where
    Q: executor::Startup,
    F: for<'report> FnOnce(Run<'report>),
{
    type Output = Result<(), BranchError<convert::Infallible, manifold::BuildError<bind::Error>>>;

    fn compose(
        self,
        owner: Tick<'d>,
        stage: &mut lending::Stage<
            '_,
            'scope,
            'life,
            'e,
            'd,
            storage::With<
                storage::Value<domain::Owner<tls::Tls<roles::Server>, KeyTag<0>>>,
                date::Clock,
            >,
            Q,
        >,
    ) -> Self::Output {
        let mut finish = session::Completion::new();
        let target = owner.target;
        let app = Routes::new(&self.state, Config::new(server::MAX_CONNECTIONS));
        let wire = stage
            .storage()
            .0
            .bind(self.wire)
            .map_err(BranchError::First)?;
        self.plan
            .install(stage, wire, app, |root, _wait, stage| {
                let build = Serve { app: root, target };
                (self.finish)(stage.lend_for(&mut finish, owner, build, Duration::from_secs(2)));
            })
            .map_err(BranchError::Second)
    }
}
