//! gRPC server runtime over H2 stream identities.
use std::ops::ControlFlow;

use dope::core::driver::schedule::clock;
use o3::{buffer::resident, depot::queue::pool};
use resident::bound;
use sark_core::{
    decode,
    manifold::{
        schedule,
        server::{self, Connection as _, drain, timing},
        stream::{Message as _, Section},
    },
};
use sark_h2::{
    conn::{self, event},
    server::{app, task::service::Select},
    stream::Key,
};
use sark_http::field::metadata::section::state::{RetainedHead, Selected};

use crate::{
    details,
    protocol::section,
    server::{
        Bank,
        call::Handler,
        egress,
        runtime::{
            Connection, policy,
            state::{self, active, storage::Storage as _},
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
    Build(state::Error),
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

impl<R: decode::Full, X> Select<R> for Service<'_, X> {
    type Retention = o3::buffer::hold::limits::Bound;

    const ENABLED: bool = true;

    fn try_retention(&self) -> Result<Self::Retention, o3::buffer::hold::limits::Error> {
        sark_h2::INGRESS_RETENTION.try_limit(self.bank.limits.layout().bytes())
    }

    fn retention(admitted: &Self::Retention) -> o3::buffer::hold::limits::Bound {
        *admitted
    }

    fn try_head(
        router: &R,
        head: RetainedHead<section::Owner, Selected<section::Plan>>,
    ) -> Result<R::Head, decode::HeadError<R::Cause>> {
        decode::Full::try_prefixed(router, head)
    }
}

impl<'a, 'd, E: event::Evidence, O, X> app::Application<'d, E, O> for Service<'a, X>
where
    O: Copy + Eq + 'd,
    X: policy::Dispatch<'d, O, Target = active::Entry<'a>>,
    X::Timer: drive::Mode<'d>,
    X::Storage<state::Store<'a, 'd, Handler<X::Call, X::State>, X::Timer>>:
        state::storage::Storage<state::Store<'a, 'd, Handler<X::Call, X::State>, X::Timer>>,
{
    type State = state::State<'a, X::Storage<Self::Selection>>;
    type Selection = state::Store<'a, 'd, Handler<X::Call, X::State>, X::Timer>;
    type Timing = timing::On;
    type Build = state::Error;

    fn state(&self, cap: app::Capacity) -> Result<Self::State, Self::Build> {
        let active = self.bank.try_take(cap)?;
        let storage = X::Storage::try_new(cap, |cap| {
            state::Store::try_new(&self.bank.limits, cap, X::BUFFERED)
        })?;
        Ok(state::State { storage, active })
    }

    fn drive<H>(
        &mut self,
        state: &mut Self::State,
        conn: &mut Connection<'d, E>,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        egress: schedule::Egress,
        context: &mut H,
        protocol: impl FnMut(
            &mut H,
            &mut Connection<'d, E>,
            &mut drain::Events<'_, 'd>,
            Option<&Self::Selection>,
        ) -> Result<Option<server::Event<Connection<'d, E>>>, conn::Error>,
        notify: impl FnMut(&mut H, schedule::Wake<Key>),
        finish: impl FnOnce(&mut H, &mut Connection<'d, E>),
        mut report: impl FnMut(&mut H, app::Error<Self::Failure>),
    ) -> Result<drain::Progress, app::Error<Self::Failure>> {
        if matches!(egress, schedule::Egress::Released) {
            <Self as app::Application<'d, E, O>>::released(state);
        }
        drive::Driver {
            exec: &mut self.exec,
            owner: turn.owner(),
            context,
            sink: move |context: &mut H, stream, error| {
                report(context, Failure::from_output(stream, error))
            },
            limits: &self.bank.limits,
        }
        .drain(state, conn, turn, protocol, notify, finish)
    }

    /// Cancels every task before releasing any retained call payload.
    fn close(&mut self, state: &mut Self::State, conn: &mut Connection<'d, E>, owner: O) {
        let active = state.active.get();
        let ControlFlow::Continue(state) = state.storage.entry() else {
            return;
        };
        for index in 0..state.calls.capacity() {
            let Some(stream) = state.calls.key_at(index) else {
                continue;
            };
            let Some(record) = state.calls.get_mut(stream) else {
                continue;
            };
            let entry = record.entry;
            if let Some(call) = record.active() {
                self.exec.cancel(owner, entry, &mut call.handler);
            }
            if let Some(times) =
                <X::Timer as state::Timing<'d>>::timers(&mut state.output_deadlines)
            {
                times.cancel(entry);
            }
        }
        active.clear();
        state.retire(conn);
        match <X::Timer as state::Timing<'d>>::activity(state, state.is_quiescent(active)) {
            schedule::Quiescence::Busy => conn.fail(),
            schedule::Quiescence::Quiet => {}
        }
    }

    fn selected(state: Option<&Self::Selection>, event: &server::Event<Connection<'d, E>>) -> bool {
        match event {
            server::Event::Head {
                head,
                section: Section::Initial,
                ..
            } => head.fields().iter().any(|field| {
                field.name == b"content-type"
                    && (field.value == b"application/grpc"
                        || field.value.starts_with(b"application/grpc+"))
            }),
            server::Event::Head { message, .. } | server::Event::Data { message, .. } => {
                state.is_some_and(|state| state.calls.entry(message.key()).is_some())
            }
            server::Event::Reset { stream, .. } => {
                state.is_some_and(|state| state.calls.entry(*stream).is_some())
            }
            server::Event::Control | server::Event::Established | server::Event::Retired(_) => {
                false
            }
        }
    }

    fn wake(state: &mut Self::State, wake: schedule::Wake<Key>) -> bool {
        match state.storage.entry() {
            ControlFlow::Continue(state) => state.calls.wake(wake) != 0,
            ControlFlow::Break(_) => false,
        }
    }

    fn released(state: &mut Self::State) {
        if let ControlFlow::Continue(state) = state.storage.entry() {
            state.egress_released = true;
        }
    }

    fn activity(state: &Self::State) -> schedule::Quiescence {
        match state.storage.get() {
            Some(store) => <X::Timer as state::Timing<'d>>::activity(
                store,
                store.is_quiescent(state.active.get()),
            ),
            None => schedule::Quiescence::Quiet,
        }
    }
}

impl<'a, 'd, E: event::Evidence, O, X> app::Timed<'d, E, O> for Service<'a, X>
where
    O: Copy + Eq + 'd,
    X: policy::Dispatch<'d, O, Target = active::Entry<'a>>,
    X::Timer: drive::Mode<'d>,
    X::Storage<state::Store<'a, 'd, Handler<X::Call, X::State>, X::Timer>>:
        state::storage::Storage<state::Store<'a, 'd, Handler<X::Call, X::State>, X::Timer>>,
{
    fn deadline(&self, state: &Self::State) -> Option<clock::Deadline<'d>> {
        let state = state.storage.get()?;
        let call = state.deadline();
        let output = <X::Timer as state::Timing<'d>>::deadline(&state.output_deadlines);
        match (call, output) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        }
    }
}

impl Failure {
    fn from_output(stream: Key, error: egress::Error<conn::Error>) -> app::Error<Self> {
        match error {
            egress::Error::Write(error) => app::Error::Connection(error),
            egress::Error::Fault(cause) => app::Error::Application(Self::Output { stream, cause }),
        }
    }
}
