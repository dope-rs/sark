use crate::server::app;
use std::{convert::Infallible, marker};

use dope::core::driver::schedule::clock;
use o3::buffer::hold::limits;
use sark_core::{
    decode,
    manifold::{
        schedule,
        server::{self, drain, timing},
    },
};
use sark_http::field::metadata::section::{
    self,
    state::{RetainedHead, Selected},
};

pub(crate) type Connection<'d, A, E = crate::conn::event::Retirement> =
    crate::server::runtime::Connection<'d, A, E>;

pub(crate) type State<'d, ID, A> = <A as app::Application<
    'd,
    crate::conn::event::Retirement,
    dope::manifold::network::listener::conn::Id<'d, ID>,
>>::State;

/// Selects the generated decoder; `Egress` covers both applications' retained output.
pub trait Select<R: decode::Decode>: app::Spec {
    type Retention;

    const ENABLED: bool;

    fn try_retention(&self) -> Result<Self::Retention, limits::Error>;

    fn retention(admitted: &Self::Retention) -> limits::Bound;

    fn try_head(
        router: &R,
        head: RetainedHead<<Self::Fields as section::Policy>::Block, Selected<Self::Fields>>,
    ) -> Result<R::Head, decode::HeadError<R::Cause>>;
}

/// The generated application's native header plan, with no companion state.
#[doc(hidden)]
pub struct Native<R>(marker::PhantomData<fn() -> R>);

impl<R> Native<R> {
    pub(in crate::server) const fn new() -> Self {
        Self(marker::PhantomData)
    }
}

impl<R: decode::Decode> app::Spec for Native<R> {
    type Failure = Infallible;
    type Fields = R::Plan;
    type Egress<'d> = R::Egress<'d>;

    fn layout(&self) -> app::Layout {
        app::Layout::new(0, 0)
    }
}

impl<R: decode::Decode> Select<R> for Native<R> {
    type Retention = ();

    const ENABLED: bool = false;

    fn try_retention(&self) -> Result<(), limits::Error> {
        Ok(())
    }

    fn retention(_: &()) -> limits::Bound {
        limits::Bound::new(0)
    }

    fn try_head(
        router: &R,
        head: RetainedHead<<R::Plan as section::Policy>::Block, Selected<R::Plan>>,
    ) -> Result<R::Head, decode::HeadError<R::Cause>> {
        router.try_head(head)
    }
}

impl<'d, E: crate::conn::event::Evidence, O, R> app::Application<'d, E, O> for Native<R>
where
    R: decode::Decode<Egress<'d>: 'd>,
{
    type State = ();
    type Timing = timing::On;
    type Build = Infallible;

    fn state(&self, _: app::Capacity) -> Result<(), Infallible> {
        Ok(())
    }

    fn drive<H>(
        &mut self,
        state: &mut (),
        carrier: &mut Connection<'d, Self, E>,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        _: schedule::Egress,
        context: &mut H,
        mut protocol: impl FnMut(
            &mut H,
            &mut Connection<'d, Self, E>,
            &mut drain::Events<'_, 'd>,
            &Self::State,
        ) -> Result<
            Option<server::Event<Connection<'d, Self, E>>>,
            crate::conn::Error,
        >,
        _: impl FnMut(&mut H, schedule::Wake<crate::stream::Key>),
        finish: impl FnOnce(&mut H, &mut Connection<'d, Self, E>),
        _: impl FnMut(&mut H, app::Error<Self::Failure>),
    ) -> Result<drain::Progress, app::Error<Self::Failure>> {
        drain::Events::fair(turn.work(), |mut drain| {
            while protocol(context, carrier, &mut drain, state)
                .map_err(app::Error::Connection)?
                .is_some()
            {}
            finish(context, carrier);
            Ok(drain.finish())
        })
    }

    fn close(&mut self, _: &mut (), _: &mut Connection<'d, Self, E>, _: O) {}

    fn selected(_: &(), _: &server::Event<Connection<'d, Self, E>>) -> bool {
        false
    }

    fn wake(_: &mut (), _: schedule::Wake<crate::stream::Key>) -> bool {
        false
    }

    fn released(_: &mut ()) {}

    fn activity(_: &()) -> schedule::Quiescence {
        schedule::Quiescence::Quiet
    }
}

impl<'d, E: crate::conn::event::Evidence, O, R> app::Timed<'d, E, O> for Native<R>
where
    R: decode::Decode<Egress<'d>: 'd>,
{
    fn deadline(&self, _: &()) -> Option<clock::Deadline<'d>> {
        None
    }
}

const _: () = assert!(size_of::<Native<()>>() == 0);
