pub(in crate::manifold::server) mod state;
use std::convert::Infallible;

use dope::core::driver::schedule::clock;
use o3::depot::{
    arena::{hash::runtime, slab::key},
    capacity, heap,
};
use sark_core::deadline::{self, config, timeline};

use crate::{
    frame,
    manifold::{
        self,
        requests::{
            self,
            book::timers::{Due, Error},
            entries,
        },
    },
};

pub(super) enum Expire {
    Target(manifold::Error),
    Credit(manifold::Error),
    Stop(manifold::Error),
    Request(requests::Error),
    Schedule(Error),
}

pub(super) const fn code(error: Error) -> frame::ErrorCode {
    match error {
        Error::Schedule(_) => frame::ErrorCode::Internal,
    }
}

/// Connection-local request deadline state for the finite policy.
pub struct Controller<'d> {
    config: config::Config,
    book: deadline::Agenda<
        key::Handle<runtime::Domain<entries::Tag>>,
        clock::Deadline<'d>,
        Infallible,
    >,
}

#[derive(Clone, Copy, Default)]
pub struct Disabled;

/// Compile-time request deadline policy and its exact connection state.
pub trait Policy<'d> {
    type State: state::State<'d>;

    fn try_state(&self, cap: capacity::Count) -> Result<Self::State, heap::Error>;
}

impl<'d> Policy<'d> for Disabled {
    type State = Self;

    fn try_state(&self, _: capacity::Count) -> Result<Self::State, heap::Error> {
        Ok(Self)
    }
}

impl<'d> state::State<'d> for Disabled {
    fn transition(
        _: &mut Self,
        _: requests::Id,
        _: timeline::Transition,
        _: clock::Moment<'d>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn deadline(_: &Self) -> Option<clock::Deadline<'d>> {
        None
    }

    fn poll_due(_: &mut Self, _: clock::Moment<'d>) -> deadline::Poll<Due> {
        deadline::Poll::Idle
    }
}

impl<'d> Policy<'d> for config::Config {
    type State = Controller<'d>;

    fn try_state(&self, cap: capacity::Count) -> Result<Self::State, heap::Error> {
        Ok(Controller {
            config: *self,
            book: deadline::Agenda::try_new(cap)?,
        })
    }
}

impl<'d> state::State<'d> for Controller<'d> {
    fn transition(
        state: &mut Self,
        id: requests::Id,
        change: timeline::Transition,
        now: clock::Moment<'d>,
    ) -> Result<(), Error> {
        state
            .book
            .transition(
                id.entry(),
                state.config,
                change,
                now,
                clock::Moment::clamp_add,
            )
            .map_err(Error::Schedule)
    }

    fn deadline(state: &Self) -> Option<clock::Deadline<'d>> {
        deadline::Book::peek(&state.book).map(|entry| entry.1.at)
    }

    fn poll_due(state: &mut Self, now: clock::Moment<'d>) -> deadline::Poll<Due> {
        let expired = match deadline::Book::poll_due(&mut state.book, now.into()) {
            deadline::Poll::Idle => return deadline::Poll::Idle,
            deadline::Poll::Renewed => return deadline::Poll::Renewed,
            deadline::Poll::Expired(expired) => expired,
        };
        match expired.scheduled.action {
            deadline::Action::Expire(phase) => {
                deadline::Poll::Expired(Due::new(expired.key, phase))
            }
            deadline::Action::Wake(never) => match never {},
        }
    }
}
