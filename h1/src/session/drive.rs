use core::{ops::ControlFlow, pin};

use o3::buffer::{
    Complete as _,
    write::{Sink as _, Slice},
};
use sark_http::{h1::codec::canned, payload::fixed::Body};

use super::{Conn, Consumption, Dispatch, Outcome, ingress::Protocol};
use crate::{
    Input, pump,
    server::deadline,
    session, switch,
    wire::{head, output, parsing, recv},
};

const MAX_PIPELINE: u32 = 32;

enum Stop<'d> {
    Output(output::Complete<'d>),
    Wake,
    Streamed,
    Park,
    Close(Body<'d>),
    Rejected(pump::Reject),
}

pub enum Flow {
    Next,
    Sent,
    End,
    Head,
    Fields,
}

struct State<'d, H> {
    connection: head::Connection,
    handoff: Option<H>,
    stop: Stop<'d>,
}

/// One request's loan of the connection, input cursor and unpublished output.
pub struct Turn<'a, 'input, 'd, R, P, T: switch::Engine, D: Dispatch<'d, R, P, T>> {
    pub conn: &'a mut Conn<'d, R, P, T, D::Ready, D::Head>,
    pub router: pin::Pin<&'a mut D>,
    pub output: Slice<'a, &'a mut usize>,
    rest: &'a mut Input<'input>,
    state: &'a mut State<'d, T::Staged>,
}

impl<'input, 'd, R, P, T: switch::Engine, D: Dispatch<'d, R, P, T>>
    Turn<'_, 'input, 'd, R, P, T, D>
{
    pub fn input(&self) -> Input<'input> {
        *self.rest
    }

    fn stop(&mut self, reason: Stop<'d>) -> Flow {
        self.state.stop = reason;
        Flow::End
    }

    pub fn close(&mut self, reason: Body<'d>) -> Flow {
        self.stop(Stop::Close(reason))
    }

    pub fn failed(&mut self, cause: crate::wire::Failure) -> Flow {
        let reason = self.router.as_mut().failure(&cause);
        self.close(reason)
    }

    fn abort(&mut self, error: crate::wire::Failure) -> Flow {
        let reason = self.router.as_mut().abort_body(self.conn, error);
        self.close(reason)
    }

    fn after_head(&mut self, used: &mut usize, prior: usize) -> Result<(), Flow> {
        match used.checked_sub(prior) {
            Some(rest) => {
                *used = rest;
                Ok(())
            }
            None => {
                let error = o3::buffer::storage::range::Error::new(0..prior, *used);
                self.conn.ingress.protocol = o3::mem::slot::Storage::new(());
                Err(
                    self.failed(crate::wire::Failure::Input(crate::wire::InputError::Range(
                        error,
                    ))),
                )
            }
        }
    }

    fn consume(&mut self, mut frame: Consumption, prior: usize) -> Result<bool, Flow> {
        if prior != 0 {
            let used = match &mut frame {
                Consumption::Buffered(used) => used,
                Consumption::Discard(frame) => &mut frame.head,
            };
            self.after_head(used, prior)?;
        }
        match self.conn.ingress.consume(frame, self.rest) {
            Ok(complete) => Ok(complete),
            Err(cause) => Err(self.abort(cause)),
        }
    }

    pub fn rejected(&mut self, error: parsing::Rejection<'d>) -> Flow {
        match error {
            parsing::Rejection::Close(reason) => self.close(reason),
            parsing::Rejection::Failed(cause) => self.failed(cause),
        }
    }

    pub fn reject_head(&mut self, error: parsing::Rejection<'d>) -> Flow {
        self.router.as_mut().reject_head(&mut self.conn.async_state);
        self.rejected(error)
    }

    pub fn reject(
        &mut self,
        progress: core::ops::ControlFlow<parsing::Rejected<'d>, parsing::Rejection<'d>>,
    ) -> Flow {
        match progress {
            core::ops::ControlFlow::Continue(error) => self.reject_head(error),
            core::ops::ControlFlow::Break(pending) => {
                self.conn.ingress.park(Protocol::Reject(pending));
                Flow::Fields
            }
        }
    }

    pub fn await_head(&mut self, pending: D::Head) -> Flow {
        self.conn.ingress.park(Protocol::PendingHead(pending));
        Flow::Fields
    }

    pub fn await_ready(&mut self, ready: D::Ready, used: usize) -> Flow {
        if let Err(end) = self.consume(Consumption::Buffered(used), 0) {
            return end;
        }
        self.conn.ingress.park(Protocol::Ready(ready));
        self.stop(Stop::Wake)
    }

    pub fn await_line(&mut self) -> Flow {
        match parsing::Line::start(*self.rest) {
            Ok(line) => self.conn.ingress.park(Protocol::Line(line)),
            Err(error) => return self.reject_head(error),
        }
        self.rest.consume_max(usize::MAX);
        Flow::Head
    }

    pub fn await_body(
        &mut self,
        mut state: session::BodyAwait,
        expectation: sark_http::h1::field::expect::Decision,
        pending: P,
        prior: usize,
    ) -> Flow {
        self.conn.ingress.park(if D::has_body_work(&pending) {
            Protocol::Working(pending)
        } else {
            Protocol::Body(pending)
        });
        if let Err(end) = self.after_head(&mut state.consumed, prior) {
            return end;
        }
        if self.rest.advance(state.consumed).is_err() {
            let pending = self
                .conn
                .ingress
                .protocol
                .occupied()
                .map(|slot| slot.take().0);
            self.conn.ingress.finish_frame();
            drop(pending);
            return self.close(canned::BAD_REQUEST);
        }
        if expectation.is_continue() {
            return self.stop(Stop::Output(output::Complete::Static {
                body: crate::CONTINUE_100,
            }));
        }
        if self.conn.ingress.is_ready() {
            return self.stop(Stop::Wake);
        }
        Flow::End
    }

    pub fn complete(
        &mut self,
        used: Consumption,
        response: output::Complete<'d>,
        next: head::Connection,
        prior: usize,
    ) -> Flow {
        let prior = if matches!(used, Consumption::Buffered(0)) && next == head::Connection::Close {
            0
        } else {
            prior
        };
        let complete = match self.consume(used, prior) {
            Ok(complete) => complete,
            Err(end) => return end,
        };
        self.state.connection = self.state.connection.join(next);
        match response {
            output::Complete::Inline | output::Complete::Copy { .. } => {}
            output::Complete::Static { body }
                if self.output.write_slice(body.as_slice()).is_ok() => {}
            body => {
                self.state.stop = Stop::Output(body);
                return Flow::Sent;
            }
        }
        if complete { Flow::Next } else { Flow::Sent }
    }

    pub fn apply(&mut self, action: Outcome<'d, T::Raw>, prior: usize) -> Flow {
        match action {
            Outcome::Switch {
                next,
                consumed,
                response,
            } => {
                if let Err(end) = self.consume(Consumption::Buffered(consumed), prior) {
                    return end;
                }
                self.state.handoff = match self.conn.transitions.try_stage(next) {
                    Ok(staged) => Some(staged),
                    Err(error) => {
                        return self.stop(Stop::Rejected(pump::Reject::Switch {
                            actual: error.mode(),
                            attempted: error.attempted(),
                        }));
                    }
                };
                let egress = match response {
                    output::Switch::Static(body)
                        if self.output.write_slice(body.as_slice()).is_err() =>
                    {
                        output::Complete::Static { body }
                    }
                    _ => output::Complete::Inline,
                };
                self.state.stop = Stop::Output(egress);
                Flow::Sent
            }
            Outcome::Streamed { consumed, close } | Outcome::Park { consumed, close } => {
                if let Err(end) = self.consume(Consumption::Buffered(consumed), prior) {
                    return end;
                }
                self.state.connection = self.state.connection.join(close);
                if matches!(action, Outcome::Streamed { .. }) {
                    self.state.stop = Stop::Streamed;
                    Flow::Sent
                } else {
                    self.stop(Stop::Park)
                }
            }
            Outcome::Failed(cause) => self.failed(cause),
            Outcome::Close(reason) => self.close(reason),
            Outcome::Complete {
                consumption,
                response,
                connection,
            } => self.complete(consumption, response, connection, prior),
        }
    }
}

impl<'d, Runtime: pump::Tasks, Pending, Transitions: switch::Engine, Ready, Heads>
    Conn<'d, Runtime, Pending, Transitions, Ready, Heads>
{
    /// Drives one bounded turn on the connection's input and reserved output.
    pub fn drive<D>(
        &mut self,
        mut disp: pin::Pin<&mut D>,
        input: &[u8],
        mut write: Slice<'_, &mut usize>,
        tail: &mut pump::Tail<Transitions::Staged>,
    ) -> Result<
        (
            ControlFlow<(), output::Complete<'d>>,
            head::Connection,
            deadline::Progress,
        ),
        pump::Reject,
    >
    where
        D: Dispatch<'d, Runtime, Pending, Transitions, Ready = Ready, Head = Heads>,
    {
        let snapshot = self.ingress.recv.prepare(input)?;
        let mut rest = match &snapshot {
            Some(buffer) => Input::Buffered(buffer.view()),
            None => Input::Borrowed(input),
        };
        let extent = rest.as_slice().len();
        let mut transaction = write.stage();
        let mut output = transaction.body();
        let minimum = output.remaining().min(1024);
        let mut state = State {
            connection: self.deferred,
            handoff: None,
            stop: Stop::Output(output::Complete::Inline),
        };
        let mut head_progress = deadline::Head::Unchanged;
        let mut body_progress = deadline::Body::None;

        let stop = 'turn: {
            'requests: for _ in 0..MAX_PIPELINE {
                if output.remaining() < minimum
                    || matches!(state.connection, head::Connection::Close)
                    || (rest.as_slice().is_empty() && !self.ingress.is_ready())
                {
                    break;
                }
                head_progress = deadline::Head::Ready;
                let mut staged = output.stage();
                let result = 'frame: loop {
                    let mut turn = Turn {
                        conn: &mut *self,
                        router: disp.as_mut(),
                        output: staged.body(),
                        rest: &mut rest,
                        state: &mut state,
                    };
                    let Some(pending) = turn.conn.ingress.protocol.occupied() else {
                        let input = turn.input();
                        break D::try_consume(input, &mut turn, 0);
                    };
                    let line = match pending.take().0 {
                        Protocol::PendingHead(pending) => {
                            break 'frame D::resume_head(pending, &mut turn);
                        }
                        Protocol::Reject(pending) => {
                            let input = turn.input();
                            break 'frame turn.reject(pending.resume(input));
                        }
                        Protocol::Line(line) => line.advance(turn.input()),
                        Protocol::Prefix(prefix) => parsing::Line::prefixed(prefix, turn.input()),
                        Protocol::Body(task) => {
                            let done = match turn.conn.ingress.resume_frame(turn.rest.as_slice()) {
                                Ok(done) => done,
                                Err(error) => {
                                    turn.conn.ingress.park(Protocol::Body(task));
                                    break 'turn Stop::Close(
                                        turn.router.as_mut().abort_body(turn.conn, error),
                                    );
                                }
                            };
                            body_progress = deadline::Body::Progress;
                            let body = turn.conn.ingress.payload.take();
                            let task = match turn.router.as_mut().body_data(
                                task,
                                body,
                                &mut turn.conn.async_state,
                            ) {
                                Ok(task) => task,
                                Err(reason) => {
                                    turn.conn.ingress.finish_frame();
                                    break 'turn Stop::Close(reason);
                                }
                            };
                            let Some(done) = done else {
                                turn.rest.consume_max(usize::MAX);
                                if D::has_body_work(&task) {
                                    turn.conn.ingress.park(Protocol::Working(task));
                                    break 'turn Stop::Wake;
                                }
                                turn.conn.ingress.park(Protocol::Body(task));
                                break 'requests;
                            };
                            break 'frame D::finish_body(task, done, &mut turn);
                        }
                        Protocol::Ready(task) => {
                            break 'frame D::resume_body(task, &mut turn);
                        }
                        Protocol::Working(task) => {
                            let task = match turn
                                .router
                                .as_mut()
                                .resume_pending(task, &mut turn.conn.async_state)
                            {
                                Ok(task) => task,
                                Err(reason) => {
                                    turn.conn.ingress.finish_frame();
                                    break 'turn Stop::Close(reason);
                                }
                            };
                            if D::has_body_work(&task) {
                                turn.conn.ingress.park(Protocol::Working(task));
                                break 'turn Stop::Wake;
                            }
                            turn.conn.ingress.park(Protocol::Body(task));
                            continue 'requests;
                        }
                    };
                    match line {
                        Ok(parsing::LineProgress::Pending(line)) => {
                            turn.conn.ingress.park(Protocol::Line(line));
                            turn.rest.consume_max(usize::MAX);
                            head_progress = deadline::Head::Pending;
                            break 'requests;
                        }
                        Ok(parsing::LineProgress::Complete {
                            source,
                            prior,
                            used,
                        }) => {
                            let result = D::consume_line(source, &mut turn, prior);
                            if matches!(result, Flow::Fields) && used < rest.as_slice().len() {
                                rest.consume_max(used);
                                continue 'frame;
                            }
                            break 'frame result;
                        }
                        Err(error) => break 'frame turn.reject_head(error),
                    }
                };
                match result {
                    Flow::Next => staged.commit(),
                    Flow::Sent => {
                        staged.commit();
                        break;
                    }
                    Flow::Head | Flow::Fields => {
                        if matches!(result, Flow::Fields) {
                            rest.consume_max(usize::MAX);
                        }
                        head_progress = deadline::Head::Pending;
                        break;
                    }
                    Flow::End => break,
                }
            }
            state.stop
        };
        let State {
            connection,
            handoff,
            ..
        } = state;

        if self.ingress.is_discard() || matches!(self.ingress.window(), recv::Window::Body) {
            head_progress = deadline::Head::Body;
        }
        let handoff = match (rest, handoff) {
            (_, None)
                if matches!(connection, head::Connection::Close)
                    || matches!(stop, Stop::Close(_) | Stop::Rejected(_)) =>
            {
                drop(snapshot);
                self.ingress.recv = recv::Buffer::default();
                pump::Tail::Empty
            }
            (Input::Buffered(view), _) => {
                let used = extent - view.len();
                drop(snapshot);
                self.ingress.recv.consume(used)?;
                pump::Tail::Empty
            }
            (Input::Borrowed(bytes), Some(staged)) if !bytes.is_empty() => pump::Tail::Handoff {
                staged,
                range: input.len() - bytes.len()..input.len(),
            },
            (Input::Borrowed(_), Some(_)) => pump::Tail::Empty,
            (Input::Borrowed(bytes), None) => {
                self.ingress.absorb(bytes)?;
                pump::Tail::Empty
            }
        };
        self.deferred = connection;
        let written = !output.is_empty();
        let (state, intent) = match (self.async_state.activity(), stop) {
            (pump::Activity::Task | pump::Activity::Stream, Stop::Streamed) => (
                ControlFlow::Continue(output::Complete::Inline),
                head::Connection::KeepAlive,
            ),
            (pump::Activity::Task | pump::Activity::Stream, Stop::Park) => {
                (ControlFlow::Break(()), head::Connection::KeepAlive)
            }
            (_, Stop::Wake) if !written => (ControlFlow::Break(()), connection),
            (_, Stop::Streamed | Stop::Park | Stop::Wake) => {
                (ControlFlow::Continue(output::Complete::Inline), connection)
            }
            (_, Stop::Output(reply)) => (ControlFlow::Continue(reply), connection),
            (_, Stop::Close(reason)) if written => {
                self.deferred_action = Some(reason);
                (ControlFlow::Continue(output::Complete::Inline), connection)
            }
            (_, Stop::Close(reason)) => {
                self.deferred = head::Connection::Close;
                (
                    ControlFlow::Continue(output::Complete::fixed(reason)),
                    self.deferred,
                )
            }
            (_, Stop::Rejected(error)) => return Err(error),
        };
        transaction.commit();
        *tail = handoff;
        Ok((
            state,
            intent,
            deadline::Progress::new(head_progress, body_progress),
        ))
    }
}
