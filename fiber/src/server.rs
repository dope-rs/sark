use std::{convert::Infallible, marker::PhantomData};

use dope::{
    core::{
        BranchError,
        driver::{life::recovery, route::Identity, settings, storage},
    },
    manifold::{self, app::receive, timing},
    net::{
        link::pool::{
            change,
            recv::{input, reserve},
        },
        wire::{self, domain},
    },
    runtime::{
        executor::{self, session::Completion},
        shutdown::Requested,
        topology,
    },
};
use dope_fiber::task::storage::scope::Brand;
use sark_h1::server::{
    date,
    listener::{bind, failure},
    run::{self, Plan},
};

/// Preparation failure before a dependent server enters its driver.
#[derive(Debug)]
pub enum PrepareError {
    Date(date::Error),
    Layout(settings::errors::Claim),
}

type Preparation<T> = Result<T, BranchError<run::prepare::Error, settings::errors::Claim>>;
type Install<E> = BranchError<E, manifold::BuildError<bind::Error>>;
type Outcome<'a, E, O, Q> =
    Result<Requested<Q>, run::RunError<'a, Infallible, Install<E>, O, Requested<Q>, Q>>;

/// Runs a generated HTTP/1 application in its worker's driver.
pub trait Serve<S>: Sized {
    type Config;
    type Input: receive::Resources;

    #[doc(hidden)]
    fn plan<ID: Identity, Date: Identity, P: settings::Profile + timing::Policy>(
        plan: Plan<ID, Date, P>,
    ) -> Plan<ID, Date, P, Self::Input>;

    #[doc(hidden)]
    fn app<'brand: 'a, 'a, 'w, 'd: 'w + 'a, W, ID>(
        brand: Brand<'brand>,
        state: &'a S,
        config: Self::Config,
    ) -> impl bind::Bind<'w, 'd, ID, Wire = W, Input = Self::Input, Error = bind::Error> + 'a
    where
        W: input::Execution + 'd,
        W::Receive: wire::receive::Policy<W, Reserve: reserve::Choice<W, W::Parked>>,
        ID: Identity + 'd,
        S: failure::Policy<W>;

    fn serve<ID, Date, P, C, T>(
        plan: Plan<ID, Date, P>,
        context: C,
        state: S,
        config: Self::Config,
        report: impl recovery::Observe,
        finish: impl for<'a> FnOnce(
            Outcome<
                'a,
                Infallible,
                (
                    Plan<ID, Date, P, Self::Input>,
                    Run<'a, Self, S, wire::Identity, wire::Identity>,
                ),
                C::Shutdown,
            >,
        ) -> T,
    ) -> Preparation<T>
    where
        ID: Identity,
        Date: Identity,
        P: settings::Profile + timing::Policy,
        C: executor::Build,
        Self::Input: input::Mode<wire::Identity>,
    {
        let prepared = Self::plan(plan).prepare(())?;
        Ok(enter::<Self, _, _, _, _, _, _, _, _, _>(
            prepared,
            wire::Identity,
            context,
            state,
            config,
            report,
            finish,
        ))
    }

    fn serve_with<W, B, ID, Date, P, C, T>(
        plan: Plan<ID, Date, P>,
        wire: B,
        context: C,
        state: S,
        config: Self::Config,
        report: impl recovery::Observe,
        finish: impl for<'a> FnOnce(
            Outcome<
                'a,
                B::Error,
                (Plan<ID, Date, P, Self::Input>, Run<'a, Self, S, W, B>),
                C::Shutdown,
            >,
        ) -> T,
    ) -> Preparation<T>
    where
        W: input::Execution,
        W::Receive: wire::receive::Policy<W, Reserve: reserve::Choice<W, W::Parked>>,
        W::Bootstrap: domain::Allocate<W> + domain::Bootstrap<W>,
        B: domain::Plan<W, ID>,
        ID: Identity,
        Date: Identity,
        P: settings::Profile + timing::Policy,
        C: executor::Build,
        S: failure::Policy<W>,
        Self::Input: input::Mode<W>,
    {
        let plan = Self::plan(plan);
        let owner = plan
            .owner::<W>()
            .map_err(|e| BranchError::First(run::prepare::Error::Resource(e)))?;
        let prepared = plan.prepare(storage::Value::new(owner))?;
        Ok(enter::<Self, _, W, _, _, _, _, _, _, _>(
            prepared, wire, context, state, config, report, finish,
        ))
    }
}

/// The operation retained by a rejected driver entry.
pub struct Run<'a, A: Serve<S>, S, W, B> {
    finish: &'a mut Completion,
    state: S,
    config: A::Config,
    wire: B,
    marker: PhantomData<fn() -> (A, W)>,
}

trait Binding<W: input::Execution, ID: Identity, B>: storage::Direct {
    type Error;
    fn bind<'w, 'p: 'w, 'd: 'p>(
        storage: &'w Self::Output<'p, 'd>,
        wire: B,
    ) -> Result<impl change::Prepare<'w, 'd, ID, W>, Self::Error>
    where
        Self: 'd,
        W: 'd,
        ID: 'd;
}

impl<ID: Identity> Binding<wire::Identity, ID, wire::Identity> for () {
    type Error = Infallible;
    fn bind<'w, 'p: 'w, 'd: 'p>(
        _: &'w (),
        wire: wire::Identity,
    ) -> Result<impl change::Prepare<'w, 'd, ID, wire::Identity>, Self::Error>
    where
        Self: 'd,
        ID: 'd,
    {
        Ok(domain::Local::new(wire))
    }
}

impl<W, ID, B> Binding<W, ID, B> for storage::Value<domain::Owner<W, ID>>
where
    W: input::Execution,
    W::Bootstrap: domain::Bootstrap<W>,
    ID: Identity,
    B: domain::Plan<W, ID>,
{
    type Error = B::Error;
    fn bind<'w, 'p: 'w, 'd: 'p>(
        owner: &'w domain::Owner<W, ID>,
        wire: B,
    ) -> Result<impl change::Prepare<'w, 'd, ID, W>, B::Error>
    where
        Self: 'd,
        W: 'd,
        ID: 'd,
    {
        owner.bind(wire)
    }
}

fn enter<A, S, W, B, V, ID, Date, P, C, T>(
    prepared: topology::Prepared<storage::With<V, date::Clock>, Plan<ID, Date, P, A::Input>>,
    wire: B,
    context: C,
    state: S,
    config: A::Config,
    report: impl recovery::Observe,
    finish: impl for<'a> FnOnce(
        Outcome<'a, V::Error, (Plan<ID, Date, P, A::Input>, Run<'a, A, S, W, B>), C::Shutdown>,
    ) -> T,
) -> T
where
    A: Serve<S>,
    S: failure::Policy<W>,
    W: input::Execution,
    W::Receive: wire::receive::Policy<W, Reserve: reserve::Choice<W, W::Parked>>,
    A::Input: input::Mode<W>,
    V: Binding<W, ID, B>,
    ID: Identity,
    Date: Identity,
    P: settings::Profile + timing::Policy,
    C: executor::Build,
{
    let mut completion = Completion::new();
    let op = Run {
        finish: &mut completion,
        state,
        config,
        wire,
        marker: PhantomData,
    };
    prepared.enter(context, op, report, |outcome| {
        finish(match outcome {
            Ok(result) => result.map_err(run::RunError::Failure),
            Err(error) => Err(run::RunError::from(error)),
        })
    })
}

impl<'r, 'scope, 'life, 'p, 'd, A, S, W, B, V, ID, Date, P, Q, E>
    topology::ops::Operation<
        'd,
        topology::Worker<
            'scope,
            'life,
            'p,
            'd,
            storage::With<V, date::Clock>,
            Plan<ID, Date, P, A::Input>,
            Q,
        >,
    > for Run<'r, A, S, W, B>
where
    A: Serve<S>,
    S: failure::Policy<W>,
    W: input::Execution,
    W::Receive: wire::receive::Policy<W, Reserve: reserve::Choice<W, W::Parked>>,
    A::Input: input::Mode<W>,
    V: Binding<W, ID, B, Error = E>,
    ID: Identity,
    Date: Identity,
    P: settings::Profile + timing::Policy,
    Q: executor::Startup,
{
    type Output = Result<Requested<Q>, run::Failure<'r, Install<E>, Requested<Q>>>;
    fn run(
        self,
        worker: topology::Worker<
            'scope,
            'life,
            'p,
            'd,
            storage::With<V, date::Clock>,
            Plan<ID, Date, P, A::Input>,
            Q,
        >,
    ) -> Self::Output
    where
        Self: 'd,
    {
        let Self {
            finish,
            state,
            config,
            wire,
            marker: _,
        } = self;
        worker.lend(|plan, stage| {
            let wire = V::bind(&stage.storage().0, wire)
                .map_err(|e| run::Failure::Install(BranchError::First(e)))?;
            Brand::with(|brand| {
                let app = A::app::<W, ID>(brand, &state, config);
                plan.install(stage, wire, app, |root, wait, stage| {
                    wait.watch()
                        .finish(stage.with_app(finish, root, |app| wait.run(app)))
                })
                .map_err(|e| run::Failure::Install(BranchError::Second(e)))?
            })
        })
    }
}
