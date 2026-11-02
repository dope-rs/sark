use std::{convert, mem};

use dope::core::driver::schedule::clock;
use o3::buffer::{resident, storage};
use sark_core::manifold::{
    schedule,
    server::{Connection, app, drain},
};
use sark_h2::stream::Key;
use state::active;

use crate::{
    protocol::section,
    server::{
        call,
        config::Limits,
        egress,
        runtime::{
            policy,
            run::{read, write},
            state::{self, lanes},
        },
        service::{handler::Production, timers},
    },
};

pub(super) struct Driver<'a, X, F> {
    pub(super) exec: &'a mut X,
    pub(super) sink: &'a mut F,
    pub(super) limits: &'a Limits,
}

pub(super) trait Mode<'d>: state::Timing<'d> {
    fn has_output<'r>(
        now: clock::Moment<'d>,
        times: &Self::State<'r, 'd>,
        active: &active::Signals,
        cursor: usize,
        retries: usize,
    ) -> bool {
        active.has_output()
            || active.blocked(cursor, retries).is_some()
            || <Self as state::Timing<'d>>::deadline(times).is_some_and(|at| at <= now)
    }

    fn poll<'r, T, C>(
        now: clock::Moment<'d>,
        state: &mut state::Session<'r, 'd, T, Self, C>,
        active: &'r active::Signals,
    ) -> Option<active::Entry<'r>>
    where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>;

    fn output<'a, 'r: 'a>(
        now: clock::Moment<'d>,
        entry: active::Entry<'r>,
        times: &'a mut Self::State<'r, 'd>,
    ) -> timers::Output<'a, 'r, 'd, Self>
    where
        'd: 'a;
}

impl<'d> Mode<'d> for timers::Off {
    fn poll<'r, T, C>(
        _: clock::Moment<'d>,
        state: &mut state::Session<'r, 'd, T, Self, C>,
        active: &'r active::Signals,
    ) -> Option<active::Entry<'r>>
    where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
    {
        state.poll_output(active)
    }

    fn output<'a, 'r: 'a>(
        _: clock::Moment<'d>,
        _: active::Entry<'r>,
        _: &'a mut Self::State<'r, 'd>,
    ) -> timers::Output<'a, 'r, 'd, Self>
    where
        'd: 'a,
    {
        timers::Output::<Self>::new()
    }
}

impl<'d> Mode<'d> for timers::On {
    fn poll<'r, T, C>(
        now: clock::Moment<'d>,
        state: &mut state::Session<'r, 'd, T, Self, C>,
        active: &'r active::Signals,
    ) -> Option<active::Entry<'r>>
    where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
    {
        if let Some(entry) = state.output_deadlines.pop(now)
            && matches!(
                state.calls.records.production(entry.key()),
                Some(Production::Ready)
            )
        {
            entry.output();
        }
        state.poll_output(active)
    }

    fn output<'a, 'r: 'a>(
        now: clock::Moment<'d>,
        entry: active::Entry<'r>,
        times: &'a mut Self::State<'r, 'd>,
    ) -> timers::Output<'a, 'r, 'd, Self>
    where
        'd: 'a,
    {
        timers::Output::<Self>::new(entry, now, times)
    }
}

impl<'a, X, F> Driver<'a, X, F> {
    pub(super) fn new(exec: &'a mut X, limits: &'a Limits, sink: &'a mut F) -> Self {
        Self { exec, limits, sink }
    }

    pub(super) fn egress<'r, 'd, C, O>(
        &mut self,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        owner: O,
        active: &active::Signals,
        egress: schedule::Egress,
    ) -> (schedule::Report, active::Edge)
    where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        O: Copy + Eq + 'd,
        F: FnMut(Key, egress::Error<C::Error>),
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    {
        let exec = &mut self.exec;
        state.drive(conn, egress, active, self.sink, |entry, call| {
            exec.cancel(owner, entry, call);
        })
    }

    /// The turn time is fixed, and new call deadlines use positive durations,
    /// so only the initial due set needs draining. Output callbacks can add
    /// due output timers, which remain polled by the lane loop.
    pub(super) fn drain<'r, 'd, C, O>(
        &mut self,
        state: &mut state::Store<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        turn: &mut app::Turn<'_, '_, '_, '_, 'd, O>,
        egress: schedule::Egress,
    ) -> Result<drain::Progress, active::Error>
    where
        C: Connection<
                Key = Key,
                Owner = resident::Shared<'d>,
                Fields = section::Owner,
                Output = storage::Shared,
                Retired = convert::Infallible,
            >,
        O: Copy + Eq + 'd,
        F: FnMut(Key, egress::Error<C::Error>),
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
        X::Timer: Mode<'d>,
    {
        let now = turn.driver().driver_ref().clock().now();
        let owner = turn.owner();
        let work = turn.work();
        let wake = turn.wake();
        let state::Store { state, active } = state;
        state.egress_released |= matches!(egress, schedule::Egress::Released);
        let active = active.get();
        drain::Events::fair(work, |mut drain| {
            let mut again = false;
            'work: {
                while state.deadline().is_some_and(|at| at <= now) {
                    let key = match drain.next_ready(|| state.pop_deadline(now)) {
                        Ok(Some(key)) => key,
                        Ok(None) => break 'work,
                        Err(drain::Empty) => {
                            conn.fail();
                            break 'work;
                        }
                    };
                    if let Some(entry) = state.calls.records.entry(key) {
                        self.fail(
                            state,
                            conn,
                            owner,
                            crate::Status::from_code(crate::Code::DeadlineExceeded),
                            entry,
                        );
                    }
                }
                loop {
                    if mem::take(&mut state.egress_released) {
                        self.egress(state, conn, owner, active, schedule::Egress::Released);
                    }
                    let cursor = state.work.waiting;
                    let retries = state.work.retries;
                    let selected = state.work.select(X::TASKS, |lane| match lane {
                        lanes::Kind::Protocol => conn.has_event(),
                        lanes::Kind::Semantic => active.has_semantic(),
                        lanes::Kind::Output => X::Timer::has_output(
                            now,
                            &state.output_deadlines,
                            active,
                            cursor,
                            retries,
                        ),
                        lanes::Kind::Completion => X::TASKS && self.exec.has_done(owner),
                        lanes::Kind::Task => X::TASKS && self.exec.has_tasks(owner),
                    });
                    match selected {
                        Some(lanes::Kind::Protocol) => {
                            let Some(event) = drain.next(conn) else {
                                break;
                            };
                            read::Reader::new(self).event(now, state, conn, turn, active, event)?;
                        }
                        Some(lanes::Kind::Semantic) => {
                            let entry = match drain.next_ready(|| state.poll_app(active)) {
                                Ok(Some(stream)) => stream,
                                Ok(None) => break,
                                Err(drain::Empty) => {
                                    conn.fail();
                                    break;
                                }
                            };
                            read::Reader::new(self).semantic(state, conn, turn, entry);
                        }
                        Some(lanes::Kind::Output) => {
                            let entry =
                                match drain.next_ready(|| X::Timer::poll(now, state, active)) {
                                    Ok(Some(stream)) => stream,
                                    Ok(None) => break,
                                    Err(drain::Empty) => {
                                        conn.fail();
                                        break;
                                    }
                                };
                            write::Writer::new(self).output(now, state, conn, owner, entry);
                        }
                        Some(lanes::Kind::Completion) => {
                            let (entry, done) = match drain.next_ready(|| self.exec.pop_done(owner))
                            {
                                Ok(Some(done)) => done,
                                Ok(None) => break,
                                Err(drain::Empty) => {
                                    conn.fail();
                                    break;
                                }
                            };
                            write::Writer::new(self).completion(state, conn, owner, done, entry);
                            again |= self.exec.wake_tasks(owner) == policy::TaskState::Ready;
                        }
                        Some(lanes::Kind::Task) => {
                            again |= self.exec.drive_tasks(owner, work, turn.driver())
                                == policy::TaskState::Ready;
                            self.egress(state, conn, owner, active, schedule::Egress::Unchanged);
                            break;
                        }
                        None => {
                            let (_, edge) = self.egress(
                                state,
                                conn,
                                owner,
                                active,
                                schedule::Egress::Unchanged,
                            );
                            match edge {
                                active::Edge::None => break,
                                active::Edge::Wake => {}
                            }
                        }
                    }
                }
            }
            let result = drain.finish();
            let done = X::TASKS && self.exec.has_done(owner);
            let task = X::TASKS && active.has_work();
            let output = X::Timer::has_output(
                now,
                &state.output_deadlines,
                active,
                state.work.waiting,
                state.work.retries,
            );
            let rearm = conn.has_event()
                || state.deadline().is_some_and(|at| at <= now)
                || state.egress_released
                || task
                || output
                || done
                || again
                || state.calls.records.has_runnable();
            if rearm && !result.is_exhausted() {
                wake.wake();
            }
            Ok(result)
        })
    }

    pub(super) fn fail<'r, 'd, C, O>(
        &mut self,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        owner: O,
        status: crate::Status,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        O: Copy + Eq + 'd,
        F: FnMut(Key, egress::Error<C::Error>),
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    {
        self.cancel(state, owner, entry);
        state.send_error(conn, status, entry, self.sink)
    }

    pub(super) fn complete<'r, 'd, C, O>(
        &mut self,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        owner: O,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        O: Copy + Eq + 'd,
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    {
        <X::Timer as state::Timing<'d>>::cancel(&mut state.output_deadlines, entry);
        self.cancel(state, owner, entry);
        state.complete(conn, entry);
    }

    pub(super) fn cancel<'r, 'd, C, O>(
        &mut self,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        owner: O,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        O: Copy + Eq + 'd,
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    {
        let Some(record) = state.calls.records.get_mut(entry.key()) else {
            return;
        };
        let Some(call) = record.handler_mut() else {
            return;
        };
        self.exec.cancel(owner, entry, call)
    }
}
