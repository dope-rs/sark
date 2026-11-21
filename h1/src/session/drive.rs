use core::{mem, pin};

use o3::buffer::{
    Complete as _,
    write::{Sink as _, Slice},
};
use sark_http::{h1::codec::canned, payload::fixed::Body};

use super::{Buffered, Conn, Consumption, Dispatch, Outcome, RouteOutcome, ingress::Protocol};
use crate::{
    Input,
    pump::{self, Egress},
    server::deadline,
    session, switch,
    wire::{head, output, parsing, recv},
};

const MAX_PIPELINE: u32 = 32;

enum Stop<'d> {
    Output(Egress<'d>),
    Streamed,
    Park,
    Close(Body<'d>),
    Rejected(pump::Reject),
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
    ) -> pump::Cycle<'d, Transitions::Staged>
    where
        D: Dispatch<'d, Runtime, Pending, Transitions, Ready = Ready, Head = Heads>,
    {
        let snapshot = match self.ingress.recv.prepare(input) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return pump::Cycle::rejected(
                    error,
                    deadline::Progress::new(deadline::Head::Ready, deadline::Body::None),
                );
            }
        };
        let mut rest = match &snapshot {
            Some(buffer) => Input::Buffered(buffer.view()),
            None => Input::Borrowed(input),
        };
        let extent = rest.as_slice().len();
        let mut transaction = write.stage();
        let mut output = transaction.body();
        let minimum = output.remaining().min(1024);
        let mut connection = self.deferred;
        let mut head_progress = deadline::Head::Unchanged;
        let mut body_progress = deadline::Body::None;
        let mut handoff = None;
        let mut permit = session::Permit::new();

        let stop = 'turn: {
            'requests: for _ in 0..MAX_PIPELINE {
                if output.remaining() < minimum
                    || matches!(connection, head::Connection::Close)
                    || (rest.as_slice().is_empty() && !self.ingress.is_ready())
                {
                    break;
                }
                let mut staged = output.stage();
                let mut target = staged.body();
                head_progress = deadline::Head::Ready;
                let (result, consumed) = 'frame: {
                    let route = 'head: {
                        let line = match mem::take(&mut self.ingress.protocol) {
                            Protocol::Head => {
                                break 'head disp.as_mut().try_consume(
                                    permit,
                                    rest,
                                    target.reborrow(),
                                    self,
                                );
                            }
                            Protocol::PendingHead(pending) => {
                                break 'head disp.as_mut().resume_head(
                                    pending,
                                    permit,
                                    rest,
                                    target.reborrow(),
                                    self,
                                );
                            }
                            Protocol::Reject(pending) => {
                                break 'head disp.as_mut().resume_rejection(
                                    pending,
                                    permit,
                                    rest,
                                    &mut self.async_state,
                                );
                            }
                            Protocol::Line(line) => line.advance(rest),
                            Protocol::Prefix(prefix) => parsing::Line::prefixed(prefix, rest),
                            Protocol::Body(task) => {
                                let done = match self.ingress.resume_frame(rest.as_slice()) {
                                    Ok(done) => done,
                                    Err(error) => {
                                        self.ingress.protocol = Protocol::Body(task);
                                        break 'turn Stop::Close(
                                            disp.as_mut().abort_body(self, error),
                                        );
                                    }
                                };
                                body_progress = deadline::Body::Progress;
                                let body = self.ingress.payload.take();
                                let task = match disp.as_mut().body_data(
                                    task,
                                    body,
                                    &mut self.async_state,
                                ) {
                                    Ok(task) => task,
                                    Err(reason) => {
                                        self.ingress.finish_frame();
                                        break 'turn Stop::Close(reason);
                                    }
                                };
                                let Some(done) = done else {
                                    rest.consume_max(usize::MAX);
                                    if D::has_body_work(&task) {
                                        self.ingress.protocol = Protocol::Working(task);
                                        break 'turn Stop::Output(Egress::Wake);
                                    }
                                    self.ingress.protocol = Protocol::Body(task);
                                    break 'requests;
                                };
                                let used = done.consumed;
                                break 'frame (
                                    disp.as_mut().finish_body(
                                        task,
                                        done,
                                        permit,
                                        target.reborrow(),
                                        &mut self.async_state,
                                    ),
                                    used,
                                );
                            }
                            Protocol::Ready(task) => {
                                break 'frame (
                                    disp.as_mut().resume_body(
                                        task,
                                        permit,
                                        target.reborrow(),
                                        &mut self.async_state,
                                    ),
                                    0,
                                );
                            }
                            Protocol::Working(task) => {
                                let task =
                                    match disp.as_mut().resume_pending(task, &mut self.async_state)
                                    {
                                        Ok(task) => task,
                                        Err(reason) => {
                                            self.ingress.finish_frame();
                                            break 'turn Stop::Close(reason);
                                        }
                                    };
                                if D::has_body_work(&task) {
                                    self.ingress.protocol = Protocol::Working(task);
                                    break 'turn Stop::Output(Egress::Wake);
                                }
                                self.ingress.protocol = Protocol::Body(task);
                                continue 'requests;
                            }
                        };
                        match line {
                            Ok(parsing::LineProgress::Pending(line)) => {
                                permit.defer();
                                self.ingress.protocol = Protocol::Line(line);
                                rest.consume_max(usize::MAX);
                                head_progress = deadline::Head::Pending;
                                break 'requests;
                            }
                            Ok(parsing::LineProgress::Complete {
                                source,
                                prior,
                                used,
                            }) => {
                                let route = disp
                                    .as_mut()
                                    .consume_line(permit, source, target.reborrow(), self)
                                    .after_line(prior);
                                match route {
                                    RouteOutcome::AwaitHead { permit, pending }
                                        if used < rest.as_slice().len() =>
                                    {
                                        rest.consume_max(used);
                                        disp.as_mut().resume_head(
                                            pending,
                                            permit,
                                            rest,
                                            target.reborrow(),
                                            self,
                                        )
                                    }
                                    RouteOutcome::AwaitReject { permit, pending }
                                        if used < rest.as_slice().len() =>
                                    {
                                        rest.consume_max(used);
                                        disp.as_mut().resume_rejection(
                                            pending,
                                            permit,
                                            rest,
                                            &mut self.async_state,
                                        )
                                    }
                                    route => route,
                                }
                            }
                            Err(error) => Outcome::Close(disp.as_mut().reject_line(
                                permit,
                                &mut self.async_state,
                                error,
                            ))
                            .into(),
                        }
                    };
                    self.ingress.protocol = match route {
                        RouteOutcome::AwaitHead { permit, pending } => {
                            permit.defer();
                            Protocol::PendingHead(pending)
                        }
                        RouteOutcome::AwaitReject { permit, pending } => {
                            permit.defer();
                            Protocol::Reject(pending)
                        }
                        RouteOutcome::Outcome(result) => {
                            break 'frame (session::BodyOutcome::Complete(result), 0);
                        }
                        RouteOutcome::AwaitBody {
                            state,
                            expectation,
                            pending,
                        } => {
                            if rest.advance(state.consumed()).is_err() {
                                self.ingress.finish_frame();
                                break 'turn Stop::Close(canned::BAD_REQUEST);
                            }
                            let work = D::has_body_work(&pending);
                            self.ingress.protocol = if work {
                                Protocol::Working(pending)
                            } else {
                                Protocol::Body(pending)
                            };
                            if expectation.is_continue() {
                                break 'turn Stop::Output(Egress::Static {
                                    body: crate::CONTINUE_100,
                                });
                            }
                            if work {
                                break 'turn Stop::Output(Egress::Wake);
                            }
                            break 'requests;
                        }
                    };
                    rest.consume_max(usize::MAX);
                    head_progress = deadline::Head::Pending;
                    break 'requests;
                };
                let result = match result {
                    session::BodyOutcome::Complete(result) => result,
                    session::BodyOutcome::Pending(task) => {
                        if let Err(error) = self
                            .ingress
                            .consume(Consumption::Buffered(Buffered::new(consumed)), &mut rest)
                        {
                            break 'turn Stop::Close(disp.as_mut().abort_body(self, error));
                        }
                        self.ingress.protocol = Protocol::Ready(task);
                        break 'turn Stop::Output(Egress::Wake);
                    }
                };
                if let Outcome::Streamed { consumed: used, .. }
                | Outcome::Park { consumed: used, .. } = &result
                    && let Err(error) = self
                        .ingress
                        .consume(Consumption::Buffered(Buffered::new(*used)), &mut rest)
                {
                    break 'turn Stop::Close(disp.as_mut().abort_body(self, error));
                }
                permit = match result {
                    Outcome::NeedMore { permit, state } => {
                        match state {
                            session::NeedMore::Head => self.ingress.protocol = Protocol::Head,
                            session::NeedMore::Line => {
                                let line = match parsing::Line::start(rest) {
                                    Ok(line) => line,
                                    Err(error) => {
                                        break 'turn Stop::Close(disp.as_mut().reject_line(
                                            permit,
                                            &mut self.async_state,
                                            error,
                                        ));
                                    }
                                };
                                self.ingress.protocol = Protocol::Line(line);
                                rest.consume_max(usize::MAX);
                            }
                        }
                        head_progress = deadline::Head::Pending;
                        permit.defer();
                        break 'requests;
                    }
                    Outcome::Complete {
                        permit,
                        consumption,
                        response,
                        connection: next,
                    } => {
                        let complete = match self.ingress.consume(consumption, &mut rest) {
                            Ok(complete) => complete,
                            Err(error) => {
                                break 'turn Stop::Close(disp.as_mut().abort_body(self, error));
                            }
                        };
                        connection = connection.join(next);
                        let body = match response {
                            output::Complete::Inline | output::Complete::Copy { .. } => None,
                            output::Complete::Static { body }
                                if target.write_slice(body.as_slice()).is_ok() =>
                            {
                                None
                            }
                            output::Complete::Static { body } => Some(Egress::Static { body }),
                            output::Complete::Shared { body }
                            | output::Complete::Owned { body, .. } => Some(Egress::Shared { body }),
                            output::Complete::Compressed { body, .. } => {
                                Some(Egress::Frozen { body })
                            }
                            output::Complete::Segments { body }
                            | output::Complete::Linked { body } => Some(Egress::Segmented { body }),
                        };
                        staged.commit();
                        if let Some(body) = body {
                            break 'turn Stop::Output(body);
                        }
                        if !complete {
                            break 'requests;
                        }
                        permit
                    }
                    Outcome::Switch {
                        next,
                        buffered,
                        response,
                    } => {
                        if let Err(error) = self
                            .ingress
                            .consume(Consumption::Buffered(buffered), &mut rest)
                        {
                            break 'turn Stop::Close(disp.as_mut().abort_body(self, error));
                        }
                        handoff = match self.transitions.try_stage(next) {
                            Ok(staged) => Some(staged),
                            Err(error) => {
                                break 'turn Stop::Rejected(pump::Reject::Switch {
                                    actual: error.mode(),
                                    attempted: error.attempted(),
                                });
                            }
                        };
                        let egress = match response {
                            output::Switch::Static(body)
                                if target.write_slice(body.as_slice()).is_err() =>
                            {
                                Egress::Static { body }
                            }
                            _ => Egress::None,
                        };
                        staged.commit();
                        break 'turn Stop::Output(egress);
                    }
                    Outcome::Streamed { close, .. } => {
                        connection = connection.join(close);
                        staged.commit();
                        break 'turn Stop::Streamed;
                    }
                    Outcome::Park { close, .. } => {
                        connection = connection.join(close);
                        break 'turn Stop::Park;
                    }
                    Outcome::Failed(error) => {
                        break 'turn Stop::Close(disp.as_mut().failure(&error));
                    }
                    Outcome::Close(reason) => break 'turn Stop::Close(reason),
                };
            }
            Stop::Output(Egress::None)
        };

        if self.ingress.is_discard()
            || matches!(
                self.ingress.protocol,
                Protocol::Body(_) | Protocol::Working(_)
            )
        {
            head_progress = deadline::Head::Body;
        }
        let progress = deadline::Progress::new(head_progress, body_progress);
        let tail = match (rest, handoff) {
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
                if let Err(error) = self.ingress.recv.consume(used) {
                    return pump::Cycle::rejected(error, progress);
                }
                pump::Tail::Empty
            }
            (Input::Borrowed(bytes), Some(staged)) if !bytes.is_empty() => pump::Tail::Handoff {
                staged,
                range: input.len() - bytes.len()..input.len(),
            },
            (Input::Borrowed(_), Some(_)) => pump::Tail::Empty,
            (Input::Borrowed(bytes), None) => {
                if let Err(error) = self.ingress.absorb(bytes) {
                    return pump::Cycle::rejected(error, progress);
                }
                pump::Tail::Empty
            }
        };
        self.deferred = connection;
        let written = !output.is_empty();
        let (egress, intent) = match (self.async_state.activity(), stop) {
            (pump::Activity::Task | pump::Activity::Stream, Stop::Streamed) => {
                (Egress::Inline, head::Connection::KeepAlive)
            }
            (pump::Activity::Task | pump::Activity::Stream, Stop::Park) => {
                (Egress::Wake, head::Connection::KeepAlive)
            }
            (_, Stop::Streamed) => (Egress::Inline, connection),
            (_, Stop::Park | Stop::Output(Egress::None | Egress::Wake)) if written => {
                (Egress::Inline, connection)
            }
            (_, Stop::Park) => (Egress::None, connection),
            (_, Stop::Output(egress)) => (egress, connection),
            (_, Stop::Close(reason)) if written => {
                self.deferred_action = Some(reason);
                (Egress::Inline, connection)
            }
            (_, Stop::Close(reason)) => {
                self.deferred = head::Connection::Close;
                Egress::close(reason)
            }
            (_, Stop::Rejected(error)) => return pump::Cycle::rejected(error, progress),
        };
        transaction.commit();
        pump::Cycle {
            state: Ok(egress),
            progress,
            connection: intent,
            tail,
        }
    }
}
