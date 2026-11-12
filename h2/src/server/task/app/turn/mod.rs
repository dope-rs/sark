use crate::server::app as semantic;
use o3::mem::quota::Admission;
mod guard;
pub(super) mod schedule;

use std::marker;

use dope::{
    core::driver::{self, route::Identity, schedule::app},
    manifold::network::listener::conn,
    net::wire,
};
use sark_core::{
    body,
    decode::{self, request},
    manifold::{schedule::Egress, server::drain, task::app::Runtime},
};

use crate::{
    conn::Error,
    server::{
        response::Handoff,
        runtime::{app::Fault, input},
        state::Transitions,
        task::{app::drive, *},
    },
};

pub(super) struct Ready {
    pub(super) build: bool,
    pub(super) task: bool,
    pub(super) input: bool,
    pub(super) output: bool,
}

enum Pump {
    Round,
    Single,
}

pub(super) enum Outcome {
    Idle,
    Wake,
    Account,
}

pub(super) struct Turn<'a, 'state, 'work, 'd, ID: Identity + 'd, R, Q, W, A, P>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    A: service::Select<R>
        + semantic::Application<'d, crate::conn::event::Retirement, conn::Id<'d, ID>>,
{
    drive: drive::Drive<'a, 'state, 'd, ID, R, Q, A::Fields>,
    work: app::Work<'work, 'd>,
    driver: driver::Context<'a, 'd>,
    ready: Ready,
    service: &'a mut A,
    service_state: &'a mut service::State<'d, ID, A>,
    fault: &'a mut P,
    wire: marker::PhantomData<fn() -> W>,
}

impl<'a, 'state, 'work, 'd, ID: Identity + 'd, R, Q, W, A, P>
    Turn<'a, 'state, 'work, 'd, ID, R, Q, W, A, P>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    A: service::Select<R>
        + semantic::Application<'d, crate::conn::event::Retirement, conn::Id<'d, ID>>,
    W: wire::Spec,
    Q: Transitions<conn::Id<'d, ID>>,
    Q::Retention: Handoff,
    P: Fault<'d, W, A::Failure, ID, Build<A::Build>>,
{
    pub(super) fn new(
        drive: drive::Drive<'a, 'state, 'd, ID, R, Q, A::Fields>,
        work: app::Work<'work, 'd>,
        driver: driver::Context<'a, 'd>,
        ready: Ready,
        service: &'a mut A,
        service_state: &'a mut service::State<'d, ID, A>,
        fault: &'a mut P,
    ) -> Self {
        Self {
            drive,
            work,
            driver,
            ready,
            service,
            service_state,
            fault,
            wire: marker::PhantomData,
        }
    }

    fn build(&mut self) -> Admission<Result<Outcome, semantic::Error<A::Failure>>> {
        if !self.ready.build {
            return Admission::Empty;
        }
        if self.work.permit().is_none() {
            self.drive.parent.wake();
            return Admission::Exhausted;
        }
        let built = self
            .drive
            .state
            .data()
            .build()
            .drive(body::DEFAULT_BUILD_STEP_BUDGET, |stream| {
                self.drive.mode.body(identify(self.drive.conn, stream))
            });
        self.drive.build(built);
        Admission::Item(Ok(Outcome::Wake))
    }

    fn task(&mut self) -> Admission<Result<Outcome, semantic::Error<A::Failure>>> {
        if !self.ready.task {
            return Admission::Empty;
        }
        if !self.has_budget() {
            return Admission::Exhausted;
        }
        let progress =
            self.drive
                .reborrow()
                .batch::<A>(self.work, &mut self.driver, self.fault.response());
        if progress.is_exhausted() || self.drive.state.connection.ingress().is_blocked() {
            self.drive.parent.wake();
        }
        Admission::Item(Ok(Outcome::Wake))
    }

    fn ingress(&mut self) -> Admission<Result<Outcome, semantic::Error<A::Failure>>>
    where
        <R::Head as request::Head>::Start: schedule::Schedule<'d, R>,
    {
        if !self.ready.input {
            return Admission::Empty;
        }
        if !self.has_budget() {
            return Admission::Exhausted;
        }
        let id = self.drive.conn;
        let service = &mut *self.service;
        let state = &mut *self.service_state;
        let fault = &mut *self.fault;
        let mut turn = semantic::Turn::new(id, self.work, self.drive.parent, &mut self.driver);
        let result = input::Read::new(
            &mut self.drive,
            drive::Drive::connection,
            |run: &mut drive::Drive<'_, '_, 'd, ID, R, Q, A::Fields>, retry| {
                run.drain::<A, W, P>(service, state, &mut turn, Egress::Unchanged, retry, fault)
            },
        )
        .resume();
        let outcome = match drain::Progress::error(result, self.drive.parent) {
            None => Ok(Outcome::Account),
            Some(semantic::Error::Connection(error))
                if error.is_pressure()
                    && self.drive.state.connection.ingress().is_blocked()
                    && self.drive.can_resume() =>
            {
                Ok(Outcome::Wake)
            }
            Some(error) => Err(error),
        };
        Admission::Item(outcome)
    }

    fn egress(&mut self, pump: Pump) -> Admission<Result<Outcome, semantic::Error<A::Failure>>> {
        if !self.ready.output {
            return Admission::Empty;
        }
        if !self.has_budget() {
            return Admission::Exhausted;
        }
        let mut response = self.fault.response();
        let report = match pump {
            Pump::Round => self.drive.state.pump::<true, _, _, _, A>(
                self.drive.router.as_mut(),
                self.drive.tasks,
                self.drive.index,
                self.drive.conn,
                self.drive.limits,
                Egress::Unchanged,
                self.work,
                self.drive.mode,
                &mut response,
            ),
            Pump::Single => self.drive.state.pump::<false, _, _, _, A>(
                self.drive.router.as_mut(),
                self.drive.tasks,
                self.drive.index,
                self.drive.conn,
                self.drive.limits,
                Egress::Unchanged,
                self.work,
                self.drive.mode,
                &mut response,
            ),
        };
        if report.turns == 0 {
            return Admission::Empty;
        }
        Admission::Item(Ok(if self.ready.task || report.completed != 0 {
            Outcome::Wake
        } else {
            Outcome::Idle
        }))
    }

    fn admission(&mut self) -> Admission<Result<Outcome, semantic::Error<A::Failure>>>
    where
        R: decode::Guarded,
    {
        if !self.drive.state.can_admit() {
            return Admission::Empty;
        }
        if !self.has_budget() {
            return Admission::Exhausted;
        }
        let result = self.drive.reborrow().admit_next(self.fault.response());
        Admission::Item(
            result
                .map(|()| Outcome::Wake)
                .map_err(semantic::Error::Connection),
        )
    }

    fn pump(&self) -> Pump {
        if self.ready.build || self.ready.task || self.ready.input {
            Pump::Round
        } else {
            Pump::Single
        }
    }

    fn guarded_pump(&mut self) -> Pump {
        if self.drive.state.can_admit() {
            Pump::Round
        } else {
            self.pump()
        }
    }

    fn has_budget(&self) -> bool {
        if self.work.remaining() != 0 {
            return true;
        }
        self.drive.parent.wake();
        false
    }
}

const _: () = {
    assert!(core::mem::size_of::<Outcome>() == 1);
    assert!(core::mem::size_of::<Ready>() == 4 * core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Ready>() == core::mem::align_of::<bool>());
    assert!(
        core::mem::size_of::<Admission<Result<Outcome, Error>>>() == core::mem::size_of::<Error>()
    );
};
