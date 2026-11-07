use dope::core::driver::schedule::clock;
use o3::{
    base::time::Positive,
    buffer::{resident, storage},
};
use sark_core::manifold::{
    server::{self, Connection, app},
    stream::{Boundary, Message, Section},
};
use sark_h2::stream::Key;

use crate::{
    Reason, message,
    protocol::{request, section},
    server::{
        call::{Handler, input},
        calls,
        egress::{self, draft},
        runtime::{
            policy,
            run::drive,
            state::{Store, active},
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
    pub(super) fn event<C>(
        &mut self,
        now: clock::Moment<'d>,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        active: &'r active::Signals,
        event: server::Event<C>,
    ) -> Result<(), active::Error>
    where
        C: Connection<
                Key = Key,
                Owner = resident::Shared<'d>,
                Fields = section::Owner,
                Output = storage::Shared,
            >,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
    {
        use server::Event;

        match event {
            Event::Head {
                message: frame,
                head,
                section: Section::Trailing,
                ..
            } => {
                let stream = frame.key();
                let Some(entry) = state.calls.entry(stream) else {
                    return Ok(());
                };
                self.trailers(state, conn, head.into_parts().0, entry);
                if matches!(frame.boundary(), Boundary::End) {
                    Self::end(state, entry);
                }
            }
            Event::Head {
                message: frame,
                head,
                ..
            } => {
                let stream = frame.key();
                let entry = active.try_entry(stream)?;
                let boundary = frame.boundary();
                self.headers(now, state, conn, turn, head.into_parts().0, entry);
                if matches!(boundary, Boundary::End) {
                    Self::end(state, entry);
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

    fn headers<C>(
        &mut self,
        now: clock::Moment<'d>,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        fields: section::Owner,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
    {
        let stream = entry.key();
        let head = match request::Head::from_owner(fields) {
            Ok(head) => head,
            Err(status) => {
                self.fail(state, conn, status, entry);
                return;
            }
        };
        match state.calls.try_reserve(entry) {
            Ok(()) => {}
            Err(error) => {
                self.fail(state, conn, crate::Status::from(error), entry);
                return;
            }
        }
        let result = (|| {
            if let Some(timeout) = head.timeout() {
                let timeout = Positive::try_new(timeout)
                    .map_err(|_| crate::Status::from_code(crate::Code::DeadlineExceeded))?;
                state.try_deadline(stream, now, timeout)?;
            }
            let draft = draft::Draft::try_new(&mut state.egress, stream)?;
            let mut reply = Reply::with_writer(draft);
            let mut cx = service::Stream::new(stream.id());
            let opened = self.exec.start(turn, entry, &mut cx, head, &mut reply);
            let done = reply.is_finished();
            let (app, output, input) = opened.into_input(self.limits.bytes());
            let output = match (output, done) {
                (handler::Production::Ready, false) => handler::Production::Ready,
                (handler::Production::Idle | handler::Production::Ready, true)
                | (handler::Production::Idle, false) => handler::Production::Idle,
            };
            state
                .calls
                .initialize(stream, input, app, output)
                .map_err(calls::Error::status)?;
            state.calls.reply(reply)?;
            Ok((done, output))
        })();
        let (done, output) = match result {
            Ok(outcome) => outcome,
            Err(status) => {
                self.fail(state, conn, status, entry);
                return;
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

    fn trailers<C>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        fields: section::Owner,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
    {
        let stream = entry.key();
        match request::Head::trailers(fields) {
            Ok(meta) => {
                if let Some(input) = state.calls.stream_mut(stream) {
                    input.trailers = meta;
                }
            }
            Err(status) => self.fail(state, conn, status, entry),
        }
    }

    fn data<C>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        data: C::Data,
        boundary: Boundary,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
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

    fn end<C>(
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
    {
        let stream = entry.key();
        let Some(call) = state.calls.stream_mut(stream) else {
            return;
        };
        call.data.end();
        entry.app();
    }

    fn rearm<C>(
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
    {
        let stream = entry.key();
        if state.calls.has_input(stream) {
            entry.app();
        }
    }

    pub(super) fn semantic<C>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        turn: &mut app::Turn<'_, '_, '_, 'd, O>,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
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

    fn dispatch<C>(
        &mut self,
        state: &mut Store<'r, 'd, Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        frame: message::Frame<resident::Shared<'d>>,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        F: FnMut(&mut H, Key, egress::Error<C::Error>),
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
