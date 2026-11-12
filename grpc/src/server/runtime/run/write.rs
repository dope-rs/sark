use std::mem;

use dope::core::driver::schedule::clock;
use sark_core::manifold;
use sark_h2::{
    conn::{self, event},
    server::app,
    stream::Key,
};

use crate::{
    Reason,
    server::{
        call::Handler,
        egress::{self, draft},
        runtime::{
            Connection, policy,
            run::drive,
            state::{self, Store, active},
        },
        service::{self, handler, messages::Messages, reply::Reply},
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
    pub(super) fn finish<E: event::Evidence>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        conn: &mut Connection<'d, E>,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        entry: active::Entry<'r>,
    ) where
        F: FnMut(&mut H, Key, egress::Error<conn::Error>),
    {
        let owner = self.owner;
        let key = entry.key();
        let result = (|| {
            let Some(record) = state.calls.records.get_mut(key) else {
                return Ok(None);
            };
            let Some(call) = record.active() else {
                return Ok(None);
            };
            let Some(input) = call.input.as_mut() else {
                return Ok(None);
            };
            let index = manifold::raw::Key::slot(key);
            state.calls.data.try_bind(index)?.try_release(input)?;
            let output: Result<_, crate::Status> = match input.head.take() {
                None => {
                    let trailers = mem::take(&mut input.trailers);
                    (|| {
                        let draft = draft::Draft::try_new(&mut state.egress, key)?;
                        let mut reply = Reply::with_writer(draft);
                        let mut cx = service::Stream::new(key.id());
                        self.exec.trailers(
                            owner,
                            entry,
                            &mut call.handler,
                            &mut cx,
                            trailers,
                            &mut reply,
                        );
                        let next = match (call.output, reply.is_finished()) {
                            (_, true) => handler::RequestState::Complete,
                            (handler::Production::Ready, false) => handler::RequestState::Produce,
                            (handler::Production::Idle, false) => {
                                reply.finish_with(crate::Status::ok());
                                handler::RequestState::Complete
                            }
                        };
                        Ok(Output::Live(reply, next))
                    })()
                }
                Some(head) => {
                    let mut messages = match state.calls.messages.try_bind_mut(index) {
                        Ok(messages) => messages,
                        Err(error) => {
                            input.head = Some(head);
                            return Err(error.into());
                        }
                    };
                    let input = service::Request {
                        stream_id: key.id(),
                        head,
                        messages: Messages::new(messages.borrow_mut()),
                        trailers: mem::take(&mut input.trailers),
                    };
                    let output = (|| {
                        let draft = draft::Draft::try_new(&mut state.egress, key)?;
                        let mut output = service::Response::with_writer(draft);
                        let next =
                            self.exec
                                .request(turn, entry, &mut call.handler, input, &mut output);
                        Ok(Output::Buffered(output, next))
                    })();
                    messages.clear();
                    output
                }
            };
            call.input = None;
            let next = match output? {
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

    pub(super) fn completion<E: event::Evidence>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        conn: &mut Connection<'d, E>,
        done: X::Completion,
        entry: active::Entry<'r>,
    ) where
        F: FnMut(&mut H, Key, egress::Error<conn::Error>),
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

    pub(super) fn output<E: event::Evidence>(
        &mut self,
        now: clock::Moment<'d>,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        conn: &mut Connection<'d, E>,
        entry: active::Entry<'r>,
    ) where
        F: FnMut(&mut H, Key, egress::Error<conn::Error>),
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
