use std::convert;

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
        call::{self, input},
        calls,
        egress::{self, draft},
        runtime::{
            policy,
            run::{drive, write},
            state::{self, active},
        },
        service::{self, handler, reply::Reply, timers},
    },
};

pub(super) struct Reader<'a, 'run, X, F> {
    run: &'a mut drive::Driver<'run, X, F>,
}

impl<'a, 'run, X, F> Reader<'a, 'run, X, F> {
    pub(super) fn new(run: &'a mut drive::Driver<'run, X, F>) -> Self {
        Self { run }
    }

    pub(super) fn event<'r, 'd, C, O>(
        &mut self,
        now: clock::Moment<'d>,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        turn: &mut app::Turn<'_, '_, '_, '_, 'd, O>,
        active: &'r active::Signals,
        event: server::Event<C>,
    ) -> Result<(), active::Error>
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
                let Some(entry) = state.calls.records.entry(stream) else {
                    return Ok(());
                };
                self.trailers(state, conn, turn.owner(), head.into_parts().0, entry);
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
                let Some(entry) = state.calls.records.entry(stream) else {
                    return Ok(());
                };
                self.data(state, conn, turn.owner(), data, frame.boundary(), entry);
            }
            Event::Reset {
                stream,
                retired: None,
                ..
            } => {
                let Some(entry) = state.calls.records.entry(stream) else {
                    return Ok(());
                };
                self.run.cancel(state, turn.owner(), entry);
                state.discard(entry, conn);
            }
            Event::Reset {
                retired: Some(retired),
                ..
            }
            | Event::Retired(retired) => match retired {},
            Event::Control | Event::Established => {}
        }
        Ok(())
    }

    fn headers<'r, 'd, C, O>(
        &mut self,
        now: clock::Moment<'d>,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        turn: &mut app::Turn<'_, '_, '_, '_, 'd, O>,
        fields: section::Owner,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        O: Copy + Eq + 'd,
        F: FnMut(Key, egress::Error<C::Error>),
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    {
        let owner = turn.owner();
        let stream = entry.key();
        let head = match request::Head::from_owner(fields) {
            Ok(head) => head,
            Err(status) => {
                self.run.fail(state, conn, owner, status, entry);
                return;
            }
        };
        match state.calls.records.try_reserve(entry) {
            Ok(()) => {}
            Err(error) => {
                self.run
                    .fail(state, conn, owner, crate::Status::from(error), entry);
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
            let opened = self.run.exec.start(turn, entry, &mut cx, head, &mut reply);
            let done = reply.is_finished();
            let (app, output, input) = opened.into_input(self.run.limits.bytes());
            let output = match (output, done) {
                (handler::Production::Ready, false) => handler::Production::Ready,
                (handler::Production::Idle | handler::Production::Ready, true)
                | (handler::Production::Idle, false) => handler::Production::Idle,
            };
            state
                .calls
                .records
                .initialize(stream, input, app, output)
                .map_err(calls::Error::status)?;
            state.calls.commit().reply(reply)?;
            Ok((done, output))
        })();
        let (done, output) = match result {
            Ok(outcome) => outcome,
            Err(status) => {
                self.run.fail(state, conn, owner, status, entry);
                return;
            }
        };
        match (done, output) {
            (true, _) => {
                self.run.complete(state, conn, owner, entry);
            }
            (false, handler::Production::Ready) => entry.output(),
            (false, handler::Production::Idle) => {}
        }
    }

    fn trailers<'r, 'd, C, O>(
        &mut self,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        owner: O,
        fields: section::Owner,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        O: Copy + Eq + 'd,
        F: FnMut(Key, egress::Error<C::Error>),
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    {
        let stream = entry.key();
        match request::Head::trailers(fields) {
            Ok(meta) => {
                if let Some(input) = state.calls.stream_mut(stream) {
                    input.trailers = meta;
                }
            }
            Err(status) => self.run.fail(state, conn, owner, status, entry),
        }
    }

    fn data<'r, 'd, C, O>(
        &mut self,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        owner: O,
        data: C::Data,
        boundary: Boundary,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        O: Copy + Eq + 'd,
        F: FnMut(Key, egress::Error<C::Error>),
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
    {
        let stream = entry.key();
        let data = message::DataChunk::new(data);
        let input = self.run.limits.input();
        match state
            .calls
            .stage_data(stream, data, boundary, input.stream_wire(), input.wire())
        {
            Ok(()) => {}
            Err(calls::Stage::Unknown) => return,
            Err(calls::Stage::Failed(status)) => {
                self.run.fail(state, conn, owner, status, entry);
                return;
            }
        }
        Self::rearm(state, entry);
    }

    fn end<'r, 'd, T, P, C>(state: &mut state::Session<'r, 'd, T, P, C>, entry: active::Entry<'r>)
    where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        P: timers::Policy,
    {
        let stream = entry.key();
        let Some(call) = state.calls.stream_mut(stream) else {
            return;
        };
        call.data.end();
        entry.app();
    }

    fn rearm<'r, 'd, T, P, C>(state: &mut state::Session<'r, 'd, T, P, C>, entry: active::Entry<'r>)
    where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        P: timers::Policy,
    {
        let stream = entry.key();
        if state.calls.has_input(stream) {
            entry.app();
        }
    }

    pub(super) fn semantic<'r, 'd, C, O>(
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
        let stream = entry.key();
        let input = self.run.limits.input();
        let step = match state
            .calls
            .deframe_turn(stream, &state.segments, input.messages())
        {
            Ok(turn) => turn,
            Err(error) => {
                self.run.fail(state, conn, owner, error, entry);
                return;
            }
        };
        match step {
            input::Turn::Frame(frame) => {
                self.dispatch(state, conn, owner, frame, entry);
            }
            input::Turn::Pending => entry.app(),
            input::Turn::Complete => {
                write::Writer::new(self.run).finish(state, conn, turn, entry);
            }
            input::Turn::Incomplete => {
                self.run.fail(
                    state,
                    conn,
                    owner,
                    crate::Status::from(message::Error::Incomplete),
                    entry,
                );
            }
            input::Turn::Idle => {}
        }
    }

    fn dispatch<'r, 'd, C, O>(
        &mut self,
        state: &mut state::Session<'r, 'd, call::Handler<X::Call, X::State>, X::Timer, C>,
        conn: &mut C,
        owner: O,
        frame: message::Frame<resident::Shared<'d>>,
        entry: active::Entry<'r>,
    ) where
        C: Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
        O: Copy + Eq + 'd,
        F: FnMut(Key, egress::Error<C::Error>),
        X: policy::Dispatch<'d, O, Target = active::Entry<'r>>,
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
                .run
                .exec
                .message(owner, entry, call, &mut cx, frame, &mut reply);
            let done = reply.is_finished();
            state.calls.commit().reply(reply)?;
            Ok((done, rearm, output))
        })();
        let (done, rearm, output) = match result {
            Ok(outcome) => outcome,
            Err(status) => {
                self.run.fail(state, conn, owner, status, entry);
                return;
            }
        };
        if let Err(error) = state.calls.release_live(stream, bytes) {
            self.run.fail(state, conn, owner, error.status(), entry);
            return;
        }
        match (done, output) {
            (true, _) => {
                self.run.complete(state, conn, owner, entry);
            }
            (false, handler::Production::Ready) => entry.output(),
            (false, handler::Production::Idle) => {}
        }
        if matches!(rearm, policy::Rearm::Read) && !done {
            Self::rearm(state, entry);
        }
    }
}
