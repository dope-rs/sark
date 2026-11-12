use std::mem;

use dope::core::driver::schedule::clock;
use o3::buffer::resident;
use sark_core::{
    deadline::Book,
    manifold::{
        schedule,
        server::{self, Connection as _, drain},
    },
};
use sark_h2::{
    conn::{self, event},
    server::app,
    stream::Key,
};
use state::active;

use crate::server::{
    call::{self, Handler},
    calls,
    config::Limits,
    egress,
    runtime::{
        Connection, policy,
        run::Failure,
        state::{self, Store, lanes},
    },
    service::{handler::Production, timers},
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

    fn poll<'r, T>(
        now: clock::Moment<'d>,
        state: &mut Store<'r, 'd, T, Self>,
        active: &'r active::Signals,
    ) -> Option<active::Entry<'r>>;

    fn output<'a, 'r: 'a>(
        now: clock::Moment<'d>,
        entry: active::Entry<'r>,
        times: &'a mut Self::State<'r, 'd>,
    ) -> timers::Output<'a, 'r, 'd, Self>
    where
        'd: 'a;
}

impl<'d> Mode<'d> for timers::Off {
    fn poll<'r, T>(
        _: clock::Moment<'d>,
        state: &mut Store<'r, 'd, T, Self>,
        active: &'r active::Signals,
    ) -> Option<active::Entry<'r>> {
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
    fn poll<'r, T>(
        now: clock::Moment<'d>,
        state: &mut Store<'r, 'd, T, Self>,
        active: &'r active::Signals,
    ) -> Option<active::Entry<'r>> {
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
    pub(super) fn egress<E: event::Evidence>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        conn: &mut Connection<'d, E>,
        active: &active::Signals,
        egress: schedule::Egress,
        mut notify: impl FnMut(&mut H, schedule::Wake<Key>),
    ) -> (schedule::Report, active::Edge)
    where
        F: FnMut(&mut H, Key, egress::Error<conn::Error>),
    {
        let owner = self.owner;
        let exec = &mut *self.exec;
        let context = &mut *self.context;
        let sink = &mut self.sink;
        let (report, released) = {
            let Store {
                calls,
                egress: output,
                output_deadlines: times,
                call_deadlines: book,
                ..
            } = state;
            let calls::Store {
                records,
                data,
                messages,
            } = calls;
            records.resume(
                || conn.poll_wake().inspect(|wake| notify(context, *wake)),
                egress,
            );
            let mut retire =
                |record: &mut call::State<'r, Handler<X::Call, X::State>, resident::Shared<'d>>| {
                    let entry = record.entry;
                    if let Some(book) = book {
                        book.cancel(entry.key());
                    }
                    <X::Timer as state::Timing<'d>>::cancel(times, entry);
                    if let Some(call) = record.active() {
                        exec.cancel(owner, entry, &mut call.handler);
                    }
                    calls::Store::<_, _, _, conn::DataBlock>::release(record, data, messages)
                };
            let mut sink = |stream, error| sink(context, stream, error);
            let mut driver = egress::driver::Driver::new(conn, output, &mut sink, &mut retire);
            let report = records.drive(&mut driver);
            (report, driver.released)
        };
        state.release(released);
        let edge = if active
            .blocked(state.work.waiting, state.work.retries)
            .is_some()
        {
            active::Edge::Wake
        } else {
            active::Edge::None
        };
        (report, edge)
    }

    /// The turn time is fixed, and new call deadlines use positive durations,
    /// so only the initial due set needs draining. Output callbacks can add
    /// due output timers, which remain polled by the lane loop.
    pub(super) fn drain<E: event::Evidence>(
        &mut self,
        store: &mut state::Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        conn: &mut Connection<'d, E>,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        mut protocol: impl FnMut(
            &mut H,
            &mut Connection<'d, E>,
            &mut drain::Events<'_, 'd>,
            &state::Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        )
            -> Result<Option<server::Event<Connection<'d, E>>>, conn::Error>,
        mut notify: impl FnMut(&mut H, schedule::Wake<Key>),
        finish: impl FnOnce(&mut H, &mut Connection<'d, E>),
    ) -> Result<drain::Progress, app::Error<Failure>>
    where
        F: FnMut(&mut H, Key, egress::Error<conn::Error>),
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

    pub(super) fn fail<E: event::Evidence>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        conn: &mut Connection<'d, E>,
        status: crate::Status,
        entry: active::Entry<'r>,
    ) where
        F: FnMut(&mut H, Key, egress::Error<conn::Error>),
    {
        self.cancel(state, entry);
        state.send_error(conn, status, entry, &mut |stream, error| {
            (self.sink)(self.context, stream, error)
        })
    }

    pub(super) fn complete<E: event::Evidence>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        conn: &mut Connection<'d, E>,
        entry: active::Entry<'r>,
    ) {
        <X::Timer as state::Timing<'d>>::cancel(&mut state.output_deadlines, entry);
        self.cancel(state, entry);
        state.complete(conn, entry);
    }

    pub(super) fn cancel(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        entry: active::Entry<'r>,
    ) {
        let Some(call) = state.calls.handler_mut(entry.key()) else {
            return;
        };
        self.exec.cancel(self.owner, entry, call)
    }
}
