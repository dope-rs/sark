use std::{convert, fmt, time::Duration};

use dope::{
    core::driver::{
        self, route,
        route::{KeyTag, indices::SlotIndex},
        settings,
        settings::{claim, errors},
        storage,
    },
    manifold::{
        app::{creation, dispatch},
        timing::{interval, interval::Component},
    },
    runtime::{
        client::{Bound, Context, Provider},
        executor::{self, drive, session},
        topology::{self, lending},
    },
};
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

impl<'d> Tick<'d> {
    pub fn serve<S: storage::Plan + 'd, Q: executor::Startup>(
        self,
        app: impl executor::raw::App<'d>,
        stage: &mut lending::Stage<'_, '_, '_, '_, 'd, S, Q>,
    ) {
        let target = self.target;
        let mut finish = session::Completion::new();
        let (drive, matched) = stage
            .lend_for(
                &mut finish,
                self,
                Serve { app, target },
                Duration::from_secs(2),
            )
            .expect("provider session");
        assert!(matched, "client must retain the installed interval route");
        drive.expect("HTTP listener drive");
    }
}

pub fn completed<I: fmt::Debug, O, Q>(
    outcome: Result<
        Result<(), I>,
        topology::EnterError<'_, convert::Infallible, O, Result<(), I>, Q>,
    >,
) {
    outcome
        .as_ref()
        .map_err(topology::EnterError::kind)
        .expect("resource topology")
        .as_ref()
        .expect("interval and HTTP installation");
}

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
    Q: executor::Startup,
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
