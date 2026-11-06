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
        call::Handler,
        calls,
        egress::{self, draft},
        runtime::{
            policy,
            run::drive,
            state::{self, Session, active},
        },
        service::{self, handler, reply::Reply},
    },
};

enum Output<'a, K: manifold::raw::Key> {
    Live(Reply<'a, draft::Draft<'a, K>>, handler::RequestState),
    Buffered(
        service::Response<'a, draft::Draft<'a, K>>,
        policy::RequestState,
    ),
}

impl<'run, 'r, 'd, X, O, H, F> drive::Driver<'run, X, O, H, F>
where
    O: Copy + Eq + 'd,
    X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    X::Timer: drive::Mode<'d>,
{
    pub(super) fn finish<C>(
        &mut self,
        state: &mut Session<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
    {
        let owner = self.owner;
        let key = entry.key();
        let result = (|| {
            let egress = &mut state.egress;
            let exec = &mut self.exec;
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
                    state.calls.reply(reply)?;
                    next
                }
                Output::Buffered(output, policy::RequestState::Complete) => {
                    state.calls.response(output)?;
                    handler::RequestState::Complete
                }
                Output::Buffered(output, policy::RequestState::Produce) => {
                    state.calls.open(output)?;
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
                self.fail(state, conn, status, entry);
                return;
            }
        };
        match next {
            handler::RequestState::Complete => {
                self.complete(state, conn, entry);
            }
            handler::RequestState::Produce => {
                entry.output();
            }
        }
    }

    pub(super) fn completion<C>(
        &mut self,
        state: &mut Session<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        done: X::Completion,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
    {
        let key = entry.key();
        let result = (|| {
            let draft = draft::Draft::try_new(&mut state.egress, key)?;
            let call = state
                .calls
                .handler_mut(key)
                .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::CallState))?;
            let mut output = service::Response::with_writer(draft);
            let phase = self
                .exec
                .publish(self.owner, entry, call, done, &mut output);
            match phase {
                handler::RequestState::Complete => state.calls.response(output)?,
                handler::RequestState::Produce => state.calls.open(output)?,
            }
            Ok(phase)
        })();
        match result {
            Ok(handler::RequestState::Complete) => self.complete(state, conn, entry),
            Ok(handler::RequestState::Produce) => entry.output(),
            Err(status) => self.fail(state, conn, status, entry),
        }
    }

    pub(super) fn output<C>(
        &mut self,
        now: clock::Moment<'d>,
        state: &mut Session<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
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
                .exec
                .output(self.owner, entry, call, &mut output, &mut reply);
            let phase = outcome.resolve(&mut reply)?;
            state.calls.reply(reply)?;
            Ok(phase)
        })();
        let phase = match result {
            Ok(result) => result,
            Err(status) => {
                self.fail(state, conn, status, entry);
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
            handler::OutputState::Complete => self.complete(state, conn, entry),
        }
    }
}
