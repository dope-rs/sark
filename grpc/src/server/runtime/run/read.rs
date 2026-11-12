use dope::core::driver::schedule::clock;
use o3::{base::time::Positive, buffer::resident, depot::capacity::Count};
use sark_core::{
    deadline,
    manifold::{
        server,
        stream::{Boundary, Message, Section},
    },
};
use sark_h2::{
    conn::{self, event},
    server::app,
    stream::Key,
};

use crate::{
    Reason, message,
    protocol::request,
    server::{
        call::{self, Handler, input},
        calls,
        egress::{self, draft},
        runtime::{
            Connection, policy,
            run::drive,
            state::{self, Store, active},
        },
        service::{self, handler, reply::Reply},
    },
};

impl<'run, 'r, 'd, X, O, H, F> drive::Driver<'run, X, O, H, F>
where
    O: Copy + Eq + 'd,
    X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    X::Timer: drive::Mode<'d>,
{
    pub(super) fn event<E: event::Evidence>(
        &mut self,
        now: clock::Moment<'d>,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        conn: &mut Connection<'d, E>,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        active: &'r active::Signals,
        event: server::Event<Connection<'d, E>>,
    ) -> Result<(), active::Error>
    where
        F: FnMut(&mut H, Key, egress::Error<conn::Error>),
    {
        use server::Event;

        match event {
            Event::Head {
                message: frame,
                head,
                section,
            } => {
                let stream = frame.key();
                let entry = match section {
                    Section::Initial => {
                        let entry = active.try_entry(stream)?;
                        'head: {
                            let head = match request::Head::from_owner(head.into_parts().0) {
                                Ok(head) => head,
                                Err(status) => {
                                    self.fail(state, conn, status, entry);
                                    break 'head;
                                }
                            };
                            let vacant = match state.calls.try_reserve(entry) {
                                Ok(vacant) => vacant,
                                Err(error) => {
                                    self.fail(state, conn, crate::Status::from(error), entry);
                                    break 'head;
                                }
                            };
                            let start = (|| match self.try_start(
                                now,
                                &mut state.egress,
                                &mut state.call_deadlines,
                                state.deadline_capacity,
                                turn,
                                entry,
                                head,
                            ) {
                                Ok((opened, reply)) => {
                                    let done = reply.is_finished();
                                    let (app, output, input) =
                                        opened.into_input(self.limits.bytes());
                                    let output = if done {
                                        handler::Production::Idle
                                    } else {
                                        output
                                    };
                                    let (metadata, trailers, status, draft) = reply.into_parts();
                                    let (ready, result) = match draft.prepare(
                                        metadata,
                                        trailers,
                                        status,
                                        draft::Lifecycle::Keep,
                                    ) {
                                        Ok(prepared) => {
                                            let ready = prepared.work();
                                            prepared.apply();
                                            (ready, Ok((done, output)))
                                        }
                                        Err(status) => (None, Err(status)),
                                    };
                                    vacant
                                        .store(call::State::new(entry, input, app, output), ready);
                                    result
                                }
                                Err(status) => {
                                    vacant.store(call::State::reserved(entry), None);
                                    Err(status)
                                }
                            })();
                            let (done, output) = match start {
                                Ok(outcome) => outcome,
                                Err(status) => {
                                    self.fail(state, conn, status, entry);
                                    break 'head;
                                }
                            };
                            match (done, output) {
                                (true, _) => {
                                    self.complete(state, conn, entry);
                                }
                                (false, handler::Production::Ready) => entry.output(),
                                (false, handler::Production::Idle) => {}
                            }
                        }
                        entry
                    }
                    Section::Trailing => {
                        let Some(entry) = state.calls.entry(stream) else {
                            return Ok(());
                        };
                        match request::Head::trailers(head.into_parts().0) {
                            Ok(meta) => {
                                if let Some(input) = state.calls.stream_mut(stream) {
                                    input.trailers = meta;
                                }
                            }
                            Err(status) => self.fail(state, conn, status, entry),
                        }
                        entry
                    }
                };
                if matches!(frame.boundary(), Boundary::End)
                    && let Some(input) = state.calls.stream_mut(stream)
                {
                    input.data.end();
                    entry.app();
                }
            }
            Event::Data {
                message: frame,
                data,
            } => {
                let stream = frame.key();
                let Some(entry) = state.calls.entry(stream) else {
                    return Ok(());
                };
                self.data(state, conn, data, frame.boundary(), entry);
            }
            Event::Reset { stream, .. } => {
                let Some(entry) = state.calls.entry(stream) else {
                    return Ok(());
                };
                self.cancel(state, entry);
                state.discard(entry, conn);
            }
            Event::Control | Event::Established | Event::Retired(_) => {}
        }
        Ok(())
    }

    #[inline(always)]
    fn try_start<'e>(
        &mut self,
        now: clock::Moment<'d>,
        egress: &'e mut egress::Store,
        call_deadlines: &mut Option<deadline::Exact<Key, clock::Deadline<'d>>>,
        deadline_capacity: Count,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        entry: active::Entry<'r>,
        head: request::Head,
    ) -> Result<
        (
            call::Opened<Handler<X::Call, X::State>>,
            Reply<'e, draft::Draft<'e, Key>>,
        ),
        crate::Status,
    > {
        let stream = entry.key();
        if let Some(timeout) = head.timeout() {
            let timeout = Positive::try_new(timeout)
                .map_err(|_| crate::Status::from_code(crate::Code::DeadlineExceeded))?;
            state::try_deadline(call_deadlines, deadline_capacity, stream, now, timeout)?;
        }
        let draft = draft::Draft::try_new(egress, stream)?;
        let mut reply = Reply::with_writer(draft);
        let mut cx = service::Stream::new(stream.id());
        let opened = self.exec.start(turn, entry, &mut cx, head, &mut reply);
        Ok((opened, reply))
    }

    fn data<E: event::Evidence>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        conn: &mut Connection<'d, E>,
        data: event::DataPayload<resident::Shared<'d>>,
        boundary: Boundary,
        entry: active::Entry<'r>,
    ) where
        F: FnMut(&mut H, Key, egress::Error<conn::Error>),
    {
        let stream = entry.key();
        let data = message::DataChunk::new(data);
        let input = self.limits.input();
        match state
            .calls
            .stage_data(stream, data, boundary, input.stream_wire(), input.wire())
        {
            Ok(()) => {}
            Err(calls::Stage::Unknown) => return,
            Err(calls::Stage::Failed(status)) => {
                self.fail(state, conn, status, entry);
                return;
            }
        }
        Self::rearm(state, entry);
    }

    fn rearm(
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        entry: active::Entry<'r>,
    ) {
        let stream = entry.key();
        if state.calls.has_input(stream) {
            entry.app();
        }
    }

    pub(super) fn semantic<E: event::Evidence>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        conn: &mut Connection<'d, E>,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        entry: active::Entry<'r>,
    ) where
        F: FnMut(&mut H, Key, egress::Error<conn::Error>),
    {
        let stream = entry.key();
        let input = self.limits.input();
        let step = match state
            .calls
            .deframe_turn(stream, &state.segments, input.messages())
        {
            Ok(turn) => turn,
            Err(error) => {
                self.fail(state, conn, error, entry);
                return;
            }
        };
        match step {
            input::Turn::Frame(frame) => {
                self.dispatch(state, conn, frame, entry);
            }
            input::Turn::Pending => entry.app(),
            input::Turn::Complete => {
                self.finish(state, conn, turn, entry);
            }
            input::Turn::Incomplete => {
                self.fail(
                    state,
                    conn,
                    crate::Status::from(message::Error::Incomplete),
                    entry,
                );
            }
            input::Turn::Idle => {}
        }
    }

    fn dispatch<E: event::Evidence>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer>,
        conn: &mut Connection<'d, E>,
        frame: message::Frame<resident::Shared<'d>>,
        entry: active::Entry<'r>,
    ) where
        F: FnMut(&mut H, Key, egress::Error<conn::Error>),
    {
        let stream = entry.key();
        let bytes = frame.len() + message::HEADER_LEN;
        let result = (|| {
            let draft = draft::Draft::try_new(&mut state.egress, stream)?;
            let (call, output) = state
                .calls
                .output(stream)
                .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::CallState))?;
            let mut reply = Reply::with_writer(draft);
            let mut cx = service::Stream::new(stream.id());
            let rearm = self
                .exec
                .message(self.owner, entry, call, &mut cx, frame, &mut reply);
            let done = reply.is_finished();
            state.calls.reply(reply)?;
            Ok((done, rearm, output))
        })();
        let (done, rearm, output) = match result {
            Ok(outcome) => outcome,
            Err(status) => {
                self.fail(state, conn, status, entry);
                return;
            }
        };
        if let Err(error) = state.calls.release_live(stream, bytes) {
            self.fail(state, conn, error.status(), entry);
            return;
        }
        match (done, output) {
            (true, _) => {
                self.complete(state, conn, entry);
            }
            (false, handler::Production::Ready) => entry.output(),
            (false, handler::Production::Idle) => {}
        }
        if matches!(rearm, policy::Rearm::Read) && !done {
            Self::rearm(state, entry);
        }
    }
}
