use o3::mem::quota::Admission;
mod guard;
pub(super) mod schedule;

use std::pin;

use dope::{
    core::driver::{
        retained,
        route::Identity,
        schedule::{app, ready},
    },
    manifold::network::listener::conn,
};
use sark_core::{
    body,
    decode::{self, request},
    manifold::{schedule::Egress, task::app::Runtime},
};

use crate::{
    conn::{Error, event},
    server,
    server::{
        response::Handoff,
        runtime::input,
        state::Transitions,
        task::{
            app::{drive, header::admit},
            registry, *,
        },
    },
};

pub(super) struct Scope<'a, 'd, ID: Identity + 'd, R>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
{
    pub(super) router: pin::Pin<&'a mut R>,
    pub(super) tasks: &'a mut registry::Table<'d, ID, R::Task>,
    pub(super) conn: conn::Id<'d, ID>,
    pub(super) parent: ready::Target<'d>,
    pub(super) index: &'a mut registry::Index,
}

#[derive(Clone, Copy)]
pub(super) enum Availability {
    Idle,
    Ready,
}

pub(super) struct Ready {
    pub(super) build: Availability,
    pub(super) task: Availability,
    pub(super) input: Availability,
    pub(super) output: Availability,
}

enum Budget {
    Open,
    Spent,
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

pub(super) struct Turn<'a, 'state, 'work, 'owner, 'd: 'owner, ID: Identity + 'd, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
{
    scope: Scope<'a, 'd, ID, R>,
    state: &'a mut Borrowed<'state, 'd, R>,
    limits: server::Config,
    mode: &'a mut Q,
    work: app::Work<'work, 'd>,
    driver: retained::Context<'a, 'owner, 'd>,
    ready: Ready,
}

impl<'a, 'state, 'work, 'owner, 'd: 'owner, ID: Identity + 'd, R, Q>
    Turn<'a, 'state, 'work, 'owner, 'd, ID, R, Q>
where
    R: Runtime<'d, Target = Target<'d, ID>>,
    Q: Transitions<conn::Id<'d, ID>>,
    Q::Retention: Handoff,
{
    pub(super) fn new(
        scope: Scope<'a, 'd, ID, R>,
        state: &'a mut Borrowed<'state, 'd, R>,
        limits: server::Config,
        mode: &'a mut Q,
        work: app::Work<'work, 'd>,
        driver: retained::Context<'a, 'owner, 'd>,
        ready: Ready,
    ) -> Self {
        Self {
            scope,
            state,
            limits,
            mode,
            work,
            driver,
            ready,
        }
    }

    fn build(&mut self) -> Admission<Result<Outcome, Error>> {
        if matches!(self.ready.build, Availability::Idle) {
            return Admission::Empty;
        }
        let Some(permit) = self.work.permit() else {
            self.scope.parent.wake();
            return Admission::Exhausted;
        };
        Admission::Item(permit.run(|| {
            let built = self
                .state
                .data()
                .build()
                .drive(body::DEFAULT_BUILD_STEP_BUDGET, |stream| {
                    self.mode.body(identify(self.scope.conn, stream))
                });
            drive::Drive {
                router: self.scope.router.as_mut(),
                tasks: self.scope.tasks,
                conn: self.scope.conn,
                parent: self.scope.parent,
                index: self.scope.index,
                state: self.state,
                limits: self.limits,
                mode: self.mode,
            }
            .build(built);
            Ok(Outcome::Wake)
        }))
    }

    fn task(&mut self) -> Admission<Result<Outcome, Error>> {
        if matches!(self.ready.task, Availability::Idle) {
            return Admission::Empty;
        }
        if matches!(self.budget(), Budget::Spent) {
            return Admission::Exhausted;
        }
        drive::Drive {
            router: self.scope.router.as_mut(),
            tasks: self.scope.tasks,
            conn: self.scope.conn,
            parent: self.scope.parent,
            index: self.scope.index,
            state: self.state,
            limits: self.limits,
            mode: self.mode,
        }
        .batch(self.work, &mut self.driver);
        Admission::Item(Ok(Outcome::Wake))
    }

    fn ingress(&mut self) -> Admission<Result<Outcome, Error>>
    where
        <R::Head as request::Head>::Start: admit::Begin<'d, R>,
    {
        if matches!(self.ready.input, Availability::Idle) {
            return Admission::Empty;
        }
        if matches!(self.budget(), Budget::Spent) {
            return Admission::Exhausted;
        }
        let mut run = drive::Drive {
            router: self.scope.router.as_mut(),
            tasks: self.scope.tasks,
            conn: self.scope.conn,
            parent: self.scope.parent,
            index: self.scope.index,
            state: self.state,
            limits: self.limits,
            mode: self.mode,
        };
        let result = input::Read::new(
            &mut run,
            drive::Drive::connection,
            |run: &mut drive::Drive<'_, '_, 'd, ID, R, Q>| run.drain(self.work),
        )
        .resume::<R::Plan, event::Retirement>();
        let outcome = match result {
            Ok(report) => {
                if report.is_exhausted() {
                    self.scope.parent.wake();
                }
                Ok(Outcome::Account)
            }
            Err(error)
                if error.is_pressure()
                    && self.state.connection.ingress().is_blocked()
                    && (!self.state.connection.output().is_empty()
                        || !self.state.build_ready.is_empty()) =>
            {
                Ok(Outcome::Wake)
            }
            Err(error) => Err(error),
        };
        Admission::Item(outcome)
    }

    fn egress(&mut self, pump: Pump) -> Admission<Result<Outcome, Error>> {
        if matches!(self.ready.output, Availability::Idle) {
            return Admission::Empty;
        }
        if matches!(self.budget(), Budget::Spent) {
            return Admission::Exhausted;
        }
        let report = match pump {
            Pump::Round => self.state.pump::<true, _, _, _>(
                self.scope.router.as_mut(),
                self.scope.conn,
                self.limits,
                Egress::Unchanged,
                self.work,
                self.mode,
            ),
            Pump::Single => self.state.pump::<false, _, _, _>(
                self.scope.router.as_mut(),
                self.scope.conn,
                self.limits,
                Egress::Unchanged,
                self.work,
                self.mode,
            ),
        };
        if report.turns == 0 {
            return Admission::Empty;
        }
        Admission::Item(Ok(match self.ready.task {
            Availability::Ready => Outcome::Wake,
            Availability::Idle if report.completed != 0 => Outcome::Wake,
            Availability::Idle => Outcome::Idle,
        }))
    }

    fn admission(&mut self) -> Admission<Result<Outcome, Error>>
    where
        R: decode::Guarded,
    {
        if !self.state.can_admit() {
            return Admission::Empty;
        }
        if matches!(self.budget(), Budget::Spent) {
            return Admission::Exhausted;
        }
        let result = admit::Admit::new(drive::Drive {
            router: self.scope.router.as_mut(),
            tasks: self.scope.tasks,
            conn: self.scope.conn,
            parent: self.scope.parent,
            index: self.scope.index,
            state: self.state,
            limits: self.limits,
            mode: self.mode,
        })
        .next();
        Admission::Item(result.map(|()| Outcome::Wake))
    }

    fn pump(&self) -> Pump {
        match (self.ready.build, self.ready.task, self.ready.input) {
            (Availability::Idle, Availability::Idle, Availability::Idle) => Pump::Single,
            _ => Pump::Round,
        }
    }

    fn guarded_pump(&mut self) -> Pump {
        if self.state.can_admit() {
            Pump::Round
        } else {
            self.pump()
        }
    }

    fn budget(&self) -> Budget {
        if self.work.remaining() != 0 {
            return Budget::Open;
        }
        self.scope.parent.wake();
        Budget::Spent
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
