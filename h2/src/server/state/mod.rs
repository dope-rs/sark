//! Private connection state shared by H2 execution adapters.

use core::marker;

use dope::core::driver::schedule::clock;
use o3::buffer::resident;
use sark_core::{
    deadline::{self, timeline},
    life::{self, audit, exchange::Id},
};
use sark_http::field::metadata::section::rules;

use crate::{conn::machine, server::task::timers, stream};

pub(crate) mod exchange;
mod root;

pub(crate) use root::try_sync;

pub(crate) trait Transitions<C: Copy>: life::Mode<C, stream::Key> {
    const EXCHANGE_DEADLINES: bool;

    fn progress(&mut self, exchange: Id<C, stream::Key>);
    fn started(&mut self, exchange: Id<C, stream::Key>);
    fn ready(&mut self, exchange: Id<C, stream::Key>);
}

pub(crate) struct Projection<'a, 'd, C: Copy, M, D> {
    mode: &'a mut M,
    timer: Option<&'a mut timers::Controller<'d>>,
    now: clock::Moment<'d>,
    change: timers::Change,
    error: Option<deadline::Error<stream::Key>>,
    marker: marker::PhantomData<fn(&mut C, &mut D)>,
}

impl<'a, 'd, C: Copy, M, D> Projection<'a, 'd, C, M, D>
where
    D: timers::Policy<'d>,
{
    pub(crate) fn new(
        mode: &'a mut M,
        state: &'a mut D::State,
        now: clock::Moment<'d>,
        policy: &D,
    ) -> Self {
        let timer = if D::ENABLED {
            policy.controller(state)
        } else {
            None
        };
        Self {
            mode,
            timer,
            now,
            change: timers::Change::Unchanged,
            error: None,
            marker: marker::PhantomData,
        }
    }

    pub(crate) fn finish(self) -> (timers::Change, Option<deadline::Error<stream::Key>>) {
        (self.change, self.error)
    }

    pub(crate) fn poll_due(&mut self) -> deadline::Poll<timers::Due> {
        if !D::ENABLED {
            return deadline::Poll::Idle;
        }
        let Some(timer) = self.timer.as_mut() else {
            return deadline::Poll::Idle;
        };
        let result = timer.poll_due(self.now);
        match result {
            deadline::Poll::Renewed | deadline::Poll::Expired(_) => {
                self.change = timers::Change::Changed;
            }
            deadline::Poll::Idle => {}
        }
        result
    }

    pub(crate) fn observe_head<F>(
        &mut self,
        conn: &machine::Connection<crate::ServerRole, F, resident::Shared<'d>>,
    ) where
        F: rules::Policy,
    {
        if !D::ENABLED {
            return;
        }
        let Some(timer) = self.timer.as_mut() else {
            return;
        };
        let result = timer.observe_head(conn, self.now);
        self.apply(result);
    }

    fn apply(&mut self, result: Result<timers::Change, deadline::Error<stream::Key>>) {
        match result {
            Ok(timers::Change::Changed) => self.change = timers::Change::Changed,
            Ok(timers::Change::Unchanged) => {}
            Err(error) => self.error = Some(error),
        }
    }

    fn project(&mut self, event: life::Event<C, stream::Key>) {
        if !D::ENABLED {
            return;
        }
        let (exchange, step) = match event {
            life::Event::ExchangeOpened(exchange) => (exchange, timeline::Transition::Open),
            life::Event::HeadAdmitted(exchange) => (exchange, timeline::Transition::HeadAdmitted),
            life::Event::InputFinished(exchange) => (exchange, timeline::Transition::InputFinished),
            life::Event::ResponseCommitted { exchange, .. } => {
                (exchange, timeline::Transition::ResponseCommitted)
            }
            life::Event::ExchangeRetired(exchange) => (exchange, timeline::Transition::Retired),
            life::Event::BuildReady(exchange) => (exchange, timeline::Transition::HandlerStarted),
            _ => return,
        };
        self.transition(exchange, step);
    }

    fn transition(&mut self, exchange: Id<C, stream::Key>, step: timeline::Transition) {
        let Some(timer) = self.timer.as_mut() else {
            return;
        };
        let result = timer.transition(exchange.stream(), step, self.now);
        self.apply(result);
    }
}

impl<'d, C, M, D> life::Mode<C, stream::Key> for Projection<'_, 'd, C, M, D>
where
    C: Copy,
    M: life::Mode<C, stream::Key>,
    D: timers::Policy<'d>,
{
    const ENABLED: bool = M::ENABLED;

    type Retention = M::Retention;
    type Observer = M::Observer;

    type Body<'a>
        = M::Body<'a>
    where
        Self: 'a,
        C: 'a;

    fn observer(&mut self) -> Option<&mut Self::Observer> {
        self.mode.observer()
    }

    fn observe(&mut self, event: life::Event<C, stream::Key>) {
        self.project(event);
        self.mode.observe(event);
    }

    fn audit<F>(&mut self, conn: C, project: F)
    where
        F: FnOnce() -> audit::Snapshot,
    {
        self.mode.audit(conn, project);
    }

    fn body<'a>(&'a mut self, exchange: Id<C, stream::Key>) -> Self::Body<'a>
    where
        C: 'a,
    {
        self.mode.body(exchange)
    }
}

impl<'d, C, M, D> Transitions<C> for Projection<'_, 'd, C, M, D>
where
    C: Copy,
    M: life::Mode<C, stream::Key>,
    D: timers::Policy<'d>,
{
    const EXCHANGE_DEADLINES: bool = D::ENABLED;

    fn progress(&mut self, exchange: Id<C, stream::Key>) {
        if D::ENABLED {
            self.transition(exchange, timeline::Transition::BodyProgress);
        }
    }

    fn started(&mut self, exchange: Id<C, stream::Key>) {
        if D::ENABLED {
            self.transition(exchange, timeline::Transition::HandlerStarted);
        }
    }

    fn ready(&mut self, exchange: Id<C, stream::Key>) {
        if D::ENABLED {
            self.transition(exchange, timeline::Transition::ResponseReady);
        }
    }
}

const _: () = assert!(core::mem::size_of::<Option<stream::Key>>() == core::mem::size_of::<u64>());
