use std::mem;

use dope::core::driver::schedule::clock;
use o3::buffer::{resident, storage};
use sark_core::manifold::{
    schedule,
    server::{self, Connection, app, drain},
};
use sark_h2::stream::Key;
use state::active;

use crate::{
    protocol::section,
    server::{
        call::Handler,
        config::Limits,
        egress,
        runtime::{
            policy,
            run::Failure,
            state::{self, Store, lanes},
        },
        service::{handler::Production, timers},
    },
};

pub(super) struct Driver<'a, X, O, H, F> {
    pub(super) exec: &'a mut X,
    pub(super) owner: O,
    pub(super) context: &'a mut H,
    pub(super) sink: F,
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
        state: &mut Store<'r, 'd, T, Self, C>,
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
        state: &mut Store<'r, 'd, T, Self, C>,
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
        state: &mut Store<'r, 'd, T, Self, C>,
        active: &'r active::Signals,
    ) -> Option<active::Entry<'r>>
    where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
    {
        if let Some(entry) = state.output_deadlines.pop(now)
            && matches!(state.calls.production(entry.key()), Some(Production::Ready))
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

impl<'a, 'r, 'd, X, O, H, F> Driver<'a, X, O, H, F>
where
    O: Copy + Eq + 'd,
    X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    X::Timer: Mode<'d>,
{
    pub(super) fn egress<C>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        active: &active::Signals,
        egress: schedule::Egress,
        notify: impl FnMut(&mut H, schedule::Wake<Key>),
    ) -> (schedule::Report, active::Edge)
    where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
    {
        let owner = self.owner;
        let exec = &mut self.exec;
        state.drive(
            conn,
            egress,
            self.context,
            notify,
            active,
            &mut self.sink,
            |entry, call| exec.cancel(owner, entry, call),
        )
    }

    /// The turn time is fixed, and new call deadlines use positive durations,
    /// so only the initial due set needs draining. Output callbacks can add
    /// due output timers, which remain polled by the lane loop.
    pub(super) fn drain<C>(
        &mut self,
        store: &mut state::Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        mut protocol: impl FnMut(
            &mut H,
            &mut C,
            &mut drain::Events<'_, 'd>,
            &state::Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        ) -> Result<Option<server::Event<C>>, C::Error>,
        mut notify: impl FnMut(&mut H, schedule::Wake<Key>),
        finish: impl FnOnce(&mut H, &mut C),
    ) -> Result<drain::Progress, app::Error<C::Error, Failure>>
    where
        C: Connection<
                Key = Key,
                Owner = resident::Shared<'d>,
                Fields = section::Owner,
                Output = storage::Shared,
            >,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
    {
        let now = turn.driver().driver_ref().clock().now();
        let owner = self.owner;
        let work = turn.work();
        let wake = turn.wake();
        let active = store.active.get();
        drain::Events::fair(work, |mut drain| {
            let mut tasks = drain::Progress::default();
            'work: {
                let state = &mut *store;
                while state.deadline().is_some_and(|at| at <= now) {
                    let key = match drain.next_ready(|| state.pop_deadline(now)) {
                        Ok(Some(key)) => key,
                        Ok(None) => break 'work,
                        Err(drain::Empty) => {
                            conn.fail();
                            break 'work;
                        }
                    };
                    if let Some(entry) = state.calls.entry(key) {
                        self.fail(
                            state,
                            conn,
                            crate::Status::from_code(crate::Code::DeadlineExceeded),
                            entry,
                        );
                    }
                }
                loop {
                    let state = &mut *store;
                    if mem::take(&mut state.egress_released) {
                        self.egress(state, conn, active, schedule::Egress::Released, &mut notify);
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
                            let Some(event) = protocol(self.context, conn, &mut drain, store)
                                .map_err(app::Error::Connection)?
                            else {
                                break;
                            };
                            self.event(now, store, conn, turn, active, event)
                                .map_err(Failure::Readiness)
                                .map_err(app::Error::Application)?;
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
                            self.semantic(state, conn, turn, entry);
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
                            self.output(now, state, conn, entry);
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
                            self.completion(state, conn, done, entry);
                            self.exec.wake_tasks();
                        }
                        Some(lanes::Kind::Task) => {
                            tasks = self.exec.drive_tasks(owner, work, turn.driver());
                            self.egress(
                                state,
                                conn,
                                active,
                                schedule::Egress::Unchanged,
                                &mut notify,
                            );
                            break;
                        }
                        None => {
                            let (_, edge) = self.egress(
                                state,
                                conn,
                                active,
                                schedule::Egress::Unchanged,
                                &mut notify,
                            );
                            match edge {
                                active::Edge::None => break,
                                active::Edge::Wake => {}
                            }
                        }
                    }
                }
            }
            let state = &*store;
            let mut result = drain.finish();
            result.merge(tasks);
            if X::TASKS && self.exec.has_tasks(owner) {
                result.exhaust();
            }
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
                || state.calls.has_runnable();
            if rearm && !result.is_exhausted() {
                wake.wake();
            }
            finish(self.context, conn);
            Ok(result)
        })
    }

    pub(super) fn fail<C>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        status: crate::Status,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
    {
        self.cancel(state, entry);
        state.send_error(conn, status, entry, &mut |stream, error| {
            (self.sink)(self.context, stream, error)
        })
    }

    pub(super) fn complete<C>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
    {
        <X::Timer as state::Timing<'d>>::cancel(&mut state.output_deadlines, entry);
        self.cancel(state, entry);
        state.complete(conn, entry);
    }

    pub(super) fn cancel<C>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
    {
        let Some(call) = state.calls.handler_mut(entry.key()) else {
            return;
        };
        self.exec.cancel(self.owner, entry, call)
    }
}
