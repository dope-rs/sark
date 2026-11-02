use std::convert::Infallible;

use o3::{alloc, buffer::resident};
use sark_core::{
    decode::{self, request},
    manifold::{
        schedule,
        server::{app, drain, timing},
    },
};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{config, event, machine},
    server::{
        self,
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

impl<'d, R, O>
    app::Application<
        'd,
        machine::Connection<
            crate::ServerRole,
            R::Plan,
            resident::Shared<'d>,
            rules::RejectPush,
            event::Ignore,
        >,
        O,
    > for Route<R>
where
    R: Dispatch<'d>,
    O: Copy,
{
    type State = Book<'d, R>;
    type Timing = timing::Off;
    type Build = alloc::Error;

    fn state(&self, capacity: app::Capacity) -> Result<Self::State, alloc::Error> {
        let streams = capacity.streams();
        Book::try_new(streams)
    }

    fn drive(
        &mut self,
        state: &mut Self::State,
        carrier: &mut machine::Connection<
            crate::ServerRole,
            R::Plan,
            resident::Shared<'d>,
            rules::RejectPush,
            event::Ignore,
        >,
        turn: &mut app::Turn<'_, '_, '_, '_, 'd, O>,
        egress: schedule::Egress,
        _: impl FnMut(app::Error<crate::conn::Error, Self::Failure>),
    ) -> Result<drain::Progress, app::Error<crate::conn::Error, Self::Failure>> {
        if matches!(egress, schedule::Egress::Released) {
            match Self::State::has_input(carrier) {
                true => {
                    self.router
                        .pump::<true, _>(state, carrier, self.limits, egress, || {
                            turn.work().permit()
                        });
                }
                false => {
                    self.router
                        .pump::<false, _>(state, carrier, self.limits, egress, || {
                            turn.work().permit()
                        });
                }
            }
        }
        let progress = self.router.drive(state, carrier, self.limits, turn.work());
        if !Self::State::has_events(carrier) {
            self.router.pump::<false, _>(
                state,
                carrier,
                self.limits,
                schedule::Egress::Unchanged,
                || turn.work().permit(),
            );
        }
        if Self::State::has_events(carrier) || state.has_ready() {
            turn.wake().wake();
        }
        Ok(progress)
    }

    fn close(
        &mut self,
        state: &mut Self::State,
        carrier: &mut machine::Connection<
            crate::ServerRole,
            R::Plan,
            resident::Shared<'d>,
            rules::RejectPush,
            event::Ignore,
        >,
        _: O,
    ) {
        self.router.clear(state, carrier);
    }

    fn activity(state: &Self::State) -> schedule::Quiescence {
        if state.runtime.is_empty() {
            schedule::Quiescence::Quiet
        } else {
            schedule::Quiescence::Busy
        }
    }
}
