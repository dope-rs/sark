use crate::server::app;
use std::convert::Infallible;

use o3::alloc;
use sark_core::{
    decode::{self, request},
    manifold::{
        schedule,
        server::{Event, drain, timing},
    },
};

use crate::{
    conn::config,
    server::{
        self,
        runtime::Connection,
        sync::{Book, Dispatch},
    },
};

#[doc(hidden)]
pub struct Route<R> {
    router: R,
    limits: server::Config,
}

impl<R> Route<R> {
    pub(in crate::server) const fn new(router: R, limits: server::Config) -> Self {
        Self { router, limits }
    }

    pub(super) const fn router(&self) -> &R {
        &self.router
    }

    pub(super) const fn limits(&self) -> server::Config {
        self.limits
    }
}

impl<R> app::Spec for Route<R>
where
    R: decode::Decode,
    R::Prepared: request::Synchronous,
{
    type Failure = Infallible;
    type Fields = R::Plan;
    type Egress<'d> = R::Egress<'d>;

    fn layout(&self) -> app::Layout {
        app::Layout::new(
            config::DEFAULT_STREAM_CAPACITY,
            self.limits.max_connection_body_bytes,
        )
    }
}

impl<'d, R, O> app::Application<'d, crate::conn::event::Ignore, O> for Route<R>
where
    R: Dispatch<'d>,
    O: Copy,
{
    type State = Book<'d, R>;
    type Selection = ();
    type Timing = timing::Off;
    type Build = alloc::Error;

    fn state(&self, capacity: app::Capacity) -> Result<Self::State, alloc::Error> {
        let streams = capacity.streams();
        Book::try_new(streams)
    }

    fn drive<H>(
        &mut self,
        state: &mut Self::State,
        carrier: &mut Connection<'d, Self>,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        egress: schedule::Egress,
        context: &mut H,
        _: impl FnMut(
            &mut H,
            &mut Connection<'d, Self>,
            &mut drain::Events<'_, 'd>,
            Option<&Self::Selection>,
        ) -> Result<Option<Event<Connection<'d, Self>>>, crate::conn::Error>,
        _: impl FnMut(&mut H, schedule::Wake<crate::stream::Key>),
        finish: impl FnOnce(&mut H, &mut Connection<'d, Self>),
        mut report: impl FnMut(&mut H, app::Error<Self::Failure>),
    ) -> Result<drain::Progress, app::Error<Self::Failure>> {
        let mut report = |error| report(context, app::Error::Connection(error));
        if matches!(egress, schedule::Egress::Released) {
            match Self::State::has_input(carrier) {
                true => {
                    self.router.pump::<true, _>(
                        state,
                        carrier,
                        self.limits,
                        egress,
                        || turn.work().permit(),
                        &mut report,
                    );
                }
                false => {
                    self.router.pump::<false, _>(
                        state,
                        carrier,
                        self.limits,
                        egress,
                        || turn.work().permit(),
                        &mut report,
                    );
                }
            }
        }
        let progress = self
            .router
            .drive(state, carrier, self.limits, turn.work(), &mut report);
        if !Self::State::has_events(carrier) {
            self.router.pump::<false, _>(
                state,
                carrier,
                self.limits,
                schedule::Egress::Unchanged,
                || turn.work().permit(),
                &mut report,
            );
        }
        if Self::State::has_events(carrier) || state.has_ready() {
            turn.wake().wake();
        }
        finish(context, carrier);
        Ok(progress)
    }

    fn close(&mut self, state: &mut Self::State, carrier: &mut Connection<'d, Self>, _: O) {
        self.router.clear(state, carrier);
    }

    fn selected(_: Option<&()>, _: &Event<Connection<'d, Self>>) -> bool {
        true
    }

    fn wake(state: &mut Self::State, wake: schedule::Wake<crate::stream::Key>) -> bool {
        state.runtime.wake(wake) != 0
    }

    fn released(state: &mut Self::State) {
        state.runtime.wake_egress();
    }

    fn activity(state: &Self::State) -> schedule::Quiescence {
        if state.runtime.is_empty() {
            schedule::Quiescence::Quiet
        } else {
            schedule::Quiescence::Busy
        }
    }
}
