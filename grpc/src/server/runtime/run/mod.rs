//! gRPC server runtime over H2 stream identities.
use std::convert;

use dope::core::driver::schedule::clock;
use o3::{
    buffer::{resident, storage},
    depot::queue::pool,
};
use resident::bound;
use sark_core::manifold::{
    schedule,
    server::{Connection, app, drain, timing},
};
use sark_h2::stream::Key;

use crate::{
    details,
    protocol::section,
    server::{
        Bank, call, egress,
        runtime::{
            policy,
            state::{self, active},
        },
    },
};

mod drive;
mod read;
mod write;

/// An exact local response failure.
#[derive(Debug)]
pub enum Fault {
    Lane(pool::Missing),
    Accounting,
    Credit(details::Credit),
    Protocol,
    Response(crate::Status),
}

/// An exact local failure observed by the server host.
#[derive(Debug)]
pub enum Failure {
    Readiness(active::Error),
    Output { stream: Key, cause: Fault },
}

/// One statically dispatched gRPC semantic runtime.
pub struct Service<'a, X> {
    exec: X,
    bank: &'a Bank,
}

impl<'a, X> Service<'a, X> {
    pub(in crate::server) const fn new(exec: X, bank: &'a Bank) -> Self {
        Self { exec, bank }
    }
}

impl<'a, X> app::Spec for Service<'a, X> {
    type Failure = Failure;
    type Fields = section::Plan;
    type Egress<'d> = bound::Unbounded;

    fn layout(&self) -> app::Layout {
        self.bank.limits.layout()
    }
}

impl<'a, 'd, C, O, X> app::Application<'d, C, O> for Service<'a, X>
where
    C: Connection<
            Key = Key,
            Owner = resident::Shared<'d>,
            Fields = section::Owner,
            Output = storage::Shared,
            Retired = convert::Infallible,
        >,
    O: Copy + Eq + 'd,
    X: policy::Dispatch<'d, O, Target = active::Entry<'a>>,
    X::Timer: drive::Mode<'d>,
{
    type State = state::Store<'a, 'd, call::Handler<X::Call, X::State>, X::Timer, C>;
    type Timing = timing::On;
    type Build = state::Error;

    fn state(&self, cap: app::Capacity) -> Result<Self::State, Self::Build> {
        state::Store::try_new(self.bank, cap)
    }

    fn drive(
        &mut self,
        state: &mut Self::State,
        conn: &mut C,
        turn: &mut app::Turn<'_, '_, '_, '_, 'd, O>,
        egress: schedule::Egress,
        mut report: impl FnMut(app::Error<C::Error, Self::Failure>),
    ) -> Result<drain::Progress, app::Error<C::Error, Self::Failure>> {
        let mut sink = |stream, error| report(Failure::from_output(stream, error));
        let mut run = drive::Driver::new(&mut self.exec, &self.bank.limits, &mut sink);
        run.drain(state, conn, turn, egress)
            .map_err(|error| app::Error::Application(Failure::Readiness(error)))
    }

    /// Cancels every task before releasing any retained call payload.
    fn close(&mut self, state: &mut Self::State, conn: &mut C, owner: O) {
        for index in 0..state.state.calls.records.capacity() {
            let Some(stream) = state.state.calls.records.key_at(index) else {
                continue;
            };
            let Some(record) = state.state.calls.records.get_mut(stream) else {
                continue;
            };
            let entry = record.entry;
            if let Some(call) = record.handler_mut() {
                self.exec.cancel(owner, entry, call);
            }
            if let Some(times) =
                <X::Timer as state::Timing<'d>>::timers(&mut state.state.output_deadlines)
            {
                times.cancel(entry);
            }
        }
        state.active.get().clear();
        state.state.retire(conn);
        match <X::Timer as state::Timing<'d>>::activity(state) {
            schedule::Quiescence::Busy => conn.fail(),
            schedule::Quiescence::Quiet => {}
        }
    }

    fn activity(state: &Self::State) -> schedule::Quiescence {
        <X::Timer as state::Timing<'d>>::activity(state)
    }
}

impl<'a, 'd, C, O, X> app::Timed<'d, C, O> for Service<'a, X>
where
    C: Connection<
            Key = Key,
            Owner = resident::Shared<'d>,
            Fields = section::Owner,
            Output = storage::Shared,
            Retired = convert::Infallible,
        >,
    O: Copy + Eq + 'd,
    X: policy::Dispatch<'d, O, Target = active::Entry<'a>>,
    X::Timer: drive::Mode<'d>,
{
    fn deadline(&self, state: &Self::State) -> Option<clock::Deadline<'d>> {
        let call = state.state.deadline();
        let output = <X::Timer as state::Timing<'d>>::deadline(&state.state.output_deadlines);
        match (call, output) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        }
    }
}

impl Failure {
    fn from_output<E>(stream: Key, error: egress::Error<E>) -> app::Error<E, Self> {
        match error {
            egress::Error::Write(error) => app::Error::Connection(error),
            egress::Error::Fault(cause) => app::Error::Application(Self::Output { stream, cause }),
        }
    }
}
