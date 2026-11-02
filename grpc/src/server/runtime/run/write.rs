use dope::core::driver::schedule::clock;
use o3::buffer::{resident, storage};
use sark_core::manifold::{
    self,
    server::{Connection, app},
};
use sark_h2::stream::Key;

use crate::{
    Reason,
    server::{
        call, calls,
        egress::{self, draft},
        runtime::{
            policy,
            run::drive,
            state::{self, active},
        },
        service::{self, handler, reply::Reply},
    },
};

pub(super) struct Writer<'a, 'run, X, F> {
    run: &'a mut drive::Driver<'run, X, F>,
}

enum Output<'a, K: manifold::raw::Key> {
    Live(Reply<'a, draft::Draft<'a, K>>, handler::RequestState),
    Buffered(
        service::Response<'a, draft::Draft<'a, K>>,
        policy::RequestState,
    ),
}

impl<'a, 'run, X, F> Writer<'a, 'run, X, F> {
    pub(super) fn new(run: &'a mut drive::Driver<'run, X, F>) -> Self {
        Self { run }
    }

    pub(super) fn finish<'r, 'd, C, O>(
        &mut self,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        turn: &mut app::Turn<'_, '_, '_, '_, 'd, O>,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        O: Copy + Eq + 'd,
        F: FnMut(Key, egress::Error<C::Error>),
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    {
        let owner = turn.owner();
        let key = entry.key();
        let result = (|| {
            let egress = &mut state.egress;
            let exec = &mut self.run.exec;
            let result = state.calls.try_finish(key, move |call, request| {
                let draft = draft::Draft::try_new(egress, key)?;
                Ok(match request {
                    calls::Request::Live { output, trailers } => {
                        let mut reply = Reply::with_writer(draft);
                        let mut cx = service::Stream::new(key.id());
                        exec.trailers(owner, entry, call, &mut cx, trailers, &mut reply);
                        let next = match (output, reply.is_finished()) {
                            (_, true) => handler::RequestState::Complete,
                            (handler::Production::Ready, false) => handler::RequestState::Produce,
                            (handler::Production::Idle, false) => {
                                reply.finish_with(crate::Status::ok());
                                handler::RequestState::Complete
                            }
                        };
                        Output::Live(reply, next)
                    }
                    calls::Request::Buffered(input) => {
                        let mut output = service::Response::with_writer(draft);
                        let next = exec.request(turn, entry, call, input, &mut output);
                        Output::Buffered(output, next)
                    }
                })
            });
            let output = match result {
                Ok(Some(Ok(output))) => output,
                Ok(Some(Err(status))) => return Err(status),
                Ok(None) => return Ok(None),
                Err(error) => return Err(error.status()),
            };
            let next = match output {
                Output::Live(reply, next) => {
                    state.calls.commit().reply(reply)?;
                    next
                }
                Output::Buffered(output, policy::RequestState::Complete) => {
                    state.calls.commit().response(output)?;
                    handler::RequestState::Complete
                }
                Output::Buffered(output, policy::RequestState::Produce) => {
                    state.calls.commit().open(output)?;
                    handler::RequestState::Produce
                }
                Output::Buffered(_, policy::RequestState::Pending) => return Ok(None),
            };
            Ok(Some(next))
        })();
        let next = match result {
            Ok(Some(next)) => next,
            Ok(None) => return,
            Err(status) => {
                self.run.fail(state, conn, owner, status, entry);
                return;
            }
        };
        match next {
            handler::RequestState::Complete => {
                self.run.complete(state, conn, owner, entry);
            }
            handler::RequestState::Produce => {
                entry.output();
            }
        }
    }

    pub(super) fn completion<'r, 'd, C, O>(
        &mut self,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        owner: O,
        done: X::Completion,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        O: Copy + Eq + 'd,
        F: FnMut(Key, egress::Error<C::Error>),
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    {
        let key = entry.key();
        let result = (|| {
            let draft = draft::Draft::try_new(&mut state.egress, key)?;
            let call = state
                .calls
                .handler_mut(key)
                .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::CallState))?;
            let mut output = service::Response::with_writer(draft);
            let phase = self.run.exec.publish(owner, entry, call, done, &mut output);
            match phase {
                handler::RequestState::Complete => state.calls.commit().response(output)?,
                handler::RequestState::Produce => state.calls.commit().open(output)?,
            }
            Ok(phase)
        })();
        match result {
            Ok(handler::RequestState::Complete) => self.run.complete(state, conn, owner, entry),
            Ok(handler::RequestState::Produce) => entry.output(),
            Err(status) => self.run.fail(state, conn, owner, status, entry),
        }
    }

    pub(super) fn output<'r, 'd, C, O>(
        &mut self,
        now: clock::Moment<'d>,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        owner: O,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        O: Copy + Eq + 'd,
        F: FnMut(Key, egress::Error<C::Error>),
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
        X::Timer: drive::Mode<'d>,
    {
        let key = entry.key();
        let result = (|| {
            let draft = draft::Draft::try_new(&mut state.egress, key)?;
            let call = state
                .calls
                .handler_mut(key)
                .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::CallState))?;
            let mut reply = Reply::with_writer(draft);
            let mut output =
                <X::Timer as drive::Mode<'d>>::output(now, entry, &mut state.output_deadlines);
            let outcome = self
                .run
                .exec
                .output(owner, entry, call, &mut output, &mut reply);
            let phase = outcome.resolve(&mut reply)?;
            state.calls.commit().reply(reply)?;
            Ok(phase)
        })();
        let phase = match result {
            Ok(result) => result,
            Err(status) => {
                self.run.fail(state, conn, owner, status, entry);
                return;
            }
        };
        if matches!(
            phase,
            handler::OutputState::Produced | handler::OutputState::Blocked
        ) && let Some(times) =
            <X::Timer as state::Timing<'d>>::timers(&mut state.output_deadlines)
        {
            times.cancel(entry);
        }
        match phase {
            handler::OutputState::Produced => {
                entry.output();
            }
            handler::OutputState::Blocked => {
                entry.block();
            }
            handler::OutputState::Pending => {}
            handler::OutputState::Complete => self.run.complete(state, conn, owner, entry),
        }
    }
}
