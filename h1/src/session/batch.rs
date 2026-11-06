use core::{convert::Infallible, mem, pin};

use o3::buffer::{storage, write::consume};
use sark_http::{
    h1::codec::canned,
    payload::{compress, fixed::Body, owned},
};
use session::{
    drain,
    ingress::{Frame, HeadFrame, Protocol},
};

use crate::{
    pump,
    server::deadline,
    session, switch,
    wire::{head, output, parsing},
};

pub(crate) enum SplitBody<'d> {
    Static(Body<'d>),
    Shared(storage::Shared),
    Frozen(compress::Frozen),
    Segmented(owned::Segments<'d>),
}

pub(crate) enum Output<'d> {
    None,
    Split(SplitBody<'d>),
    Streamed,
    Park,
    Yield,
    Close(Body<'d>),
    Rejected(pump::Reject),
}

pub(crate) enum Tail<H> {
    Absorb,
    Handoff { staged: H, start: usize },
}

const _: () = {
    assert!(mem::size_of::<Tail<()>>() == 2 * mem::size_of::<usize>());
    assert!(mem::align_of::<Tail<()>>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Tail<Infallible>>() <= mem::size_of::<Tail<()>>());
};

pub(crate) struct Batch<'d, H> {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) output: Output<'d>,
    pub(crate) connection: head::Connection,
    pub(crate) progress: deadline::Progress,
    pub(crate) tail: Tail<H>,
}

const MAX_PIPELINE: u32 = 32;

impl<'d, H> Batch<'d, H> {
    pub(crate) fn drive<Runtime, Pending, Transitions, Ready, Heads, D>(
        conn: &mut session::Conn<'d, Runtime, Pending, Transitions, Ready, Heads>,
        mut disp: pin::Pin<&mut D>,
        input: crate::Input<'_>,
        buf: &mut [u8],
        prior: head::Connection,
    ) -> Self
    where
        Transitions: switch::Engine<Staged = H>,
        D: session::Dispatch<'d, Runtime, Pending, Transitions, Ready = Ready, Head = Heads>,
    {
        let mut out = Self {
            consumed: 0,
            written: 0,
            output: Output::None,
            connection: prior,
            progress: deadline::Progress::default(),
            tail: Tail::Absorb,
        };
        let mut permit = session::Permit::new();
        let mut rest = input;
        let capacity = buf.len();
        let minimum = capacity.min(1024);
        let mut write = buf;
        for _ in 0..MAX_PIPELINE {
            if write.len() < minimum || matches!(out.connection, head::Connection::Close) {
                break;
            }
            if rest.as_slice().is_empty() && !conn.ingress.is_ready() {
                break;
            }
            out.progress.merge(deadline::Progress::new(
                deadline::Head::Ready,
                deadline::Body::None,
            ));
            let (result, consumed) = match conn.ingress.protocol.take_frame() {
                Frame::Head(pending) => {
                    let result = match pending {
                        HeadFrame::Pending(pending) => disp
                            .as_mut()
                            .resume_head(pending, permit, rest, write, conn),
                        HeadFrame::New => disp.as_mut().try_consume(permit, rest, write, conn),
                        HeadFrame::Rejected(pending) => disp
                            .as_mut()
                            .resume_error(pending, permit, rest, write, conn),
                        HeadFrame::Line(line) => match line.advance(rest) {
                            Ok(parsing::LineProgress::Pending(line)) => {
                                permit.defer();
                                conn.ingress.protocol = Protocol::Line(line);
                                out.consumed += rest.as_slice().len();
                                out.progress.merge(deadline::Progress::new(
                                    deadline::Head::Pending,
                                    deadline::Body::None,
                                ));
                                break;
                            }
                            Ok(parsing::LineProgress::Complete {
                                source,
                                prior,
                                used,
                            }) => {
                                let result = disp
                                    .as_mut()
                                    .consume_line(permit, source, write, conn)
                                    .after_line(prior);
                                match result {
                                    session::RouteOutcome::AwaitHead { permit, pending }
                                        if used < rest.as_slice().len() =>
                                    {
                                        out.consumed += rest.consume_max(used);
                                        disp.as_mut()
                                            .resume_head(pending, permit, rest, write, conn)
                                    }
                                    session::RouteOutcome::AwaitReject { permit, pending }
                                        if used < rest.as_slice().len() =>
                                    {
                                        out.consumed += rest.consume_max(used);
                                        disp.as_mut()
                                            .resume_error(pending, permit, rest, write, conn)
                                    }
                                    result => result,
                                }
                            }
                            Err(error) => session::Outcome::Close(
                                disp.as_mut().reject_line(permit, conn, error),
                            )
                            .into(),
                        },
                    };
                    match result {
                        session::RouteOutcome::AwaitReject { permit, pending } => {
                            permit.defer();
                            conn.ingress.protocol = Protocol::Reject(pending);
                            out.consumed += rest.as_slice().len();
                            out.progress.merge(deadline::Progress::new(
                                deadline::Head::Pending,
                                deadline::Body::None,
                            ));
                            break;
                        }
                        session::RouteOutcome::AwaitHead { permit, pending } => {
                            permit.defer();
                            conn.ingress.protocol = Protocol::PendingHead(pending);
                            out.consumed += rest.as_slice().len();
                            out.progress.merge(deadline::Progress::new(
                                deadline::Head::Pending,
                                deadline::Body::None,
                            ));
                            break;
                        }
                        session::RouteOutcome::Outcome(result) => {
                            (session::BodyOutcome::Complete(result), 0)
                        }
                        session::RouteOutcome::AwaitBody {
                            state,
                            expectation: expect,
                            pending: task,
                        } => {
                            let used = state.consumed();
                            if used > rest.as_slice().len() {
                                conn.ingress.finish_frame();
                                out.output = Output::Close(canned::BAD_REQUEST);
                                break;
                            }
                            conn.ingress.protocol = if D::has_body_work(&task) {
                                out.output = Output::Yield;
                                Protocol::Working(task)
                            } else {
                                Protocol::Body(task)
                            };
                            out.consumed += used;
                            if expect.is_continue() {
                                out.output = Output::Split(SplitBody::Static(crate::CONTINUE_100));
                            }
                            break;
                        }
                    }
                }
                Frame::Body(task) => {
                    let done = match conn.ingress.resume_frame(rest.as_slice()) {
                        Ok(done) => {
                            out.progress.merge(deadline::Progress::new(
                                deadline::Head::Unchanged,
                                deadline::Body::Progress,
                            ));
                            done
                        }
                        Err(error) => {
                            conn.ingress.protocol = Protocol::Body(task);
                            out.output = Output::Close(disp.as_mut().abort_body(conn, error));
                            break;
                        }
                    };
                    let body = conn.ingress.payload.take();
                    let session::Conn {
                        ingress: input,
                        async_state: app,
                        ..
                    } = conn;
                    let task = match disp.as_mut().body_data(task, body, app) {
                        Ok(task) => task,
                        Err(reason) => {
                            input.finish_frame();
                            out.output = Output::Close(reason);
                            break;
                        }
                    };
                    let Some(done) = done else {
                        input.protocol = if D::has_body_work(&task) {
                            out.output = Output::Yield;
                            Protocol::Working(task)
                        } else {
                            Protocol::Body(task)
                        };
                        out.consumed += rest.as_slice().len();
                        break;
                    };
                    let consumed = done.consumed;
                    let result = disp.as_mut().finish_body(task, done, permit, write, app);
                    (result, consumed)
                }
                Frame::Ready(task) => {
                    let result =
                        disp.as_mut()
                            .resume_body(task, permit, write, &mut conn.async_state);
                    (result, 0)
                }
                Frame::Working(task) => {
                    let task = match disp.as_mut().resume_pending(task, &mut conn.async_state) {
                        Ok(task) => task,
                        Err(reason) => {
                            conn.ingress.finish_frame();
                            out.output = Output::Close(reason);
                            break;
                        }
                    };
                    if D::has_body_work(&task) {
                        conn.ingress.protocol = Protocol::Working(task);
                        out.output = Output::Yield;
                        break;
                    }
                    conn.ingress.protocol = Protocol::Body(task);
                    continue;
                }
            };
            let result = match result {
                session::BodyOutcome::Complete(result) => result,
                session::BodyOutcome::Pending(task) => {
                    if let Err(error) = drain::Drain::buffered(
                        &mut conn.ingress,
                        session::Buffered::new(consumed),
                        &mut rest,
                        &mut out.consumed,
                    ) {
                        out.output = Output::Close(disp.as_mut().abort_body(conn, error));
                        break;
                    }
                    conn.ingress.protocol = Protocol::Ready(task);
                    out.output = Output::Yield;
                    break;
                }
            };
            if let session::Outcome::Streamed { consumed: used, .. }
            | session::Outcome::Park { consumed: used, .. } = &result
                && let Err(error) = drain::Drain::buffered(
                    &mut conn.ingress,
                    session::Buffered::new(*used),
                    &mut rest,
                    &mut out.consumed,
                )
            {
                out.output = Output::Close(disp.as_mut().abort_body(conn, error));
                break;
            }
            permit = match result {
                session::Outcome::NeedMore { permit, state } => {
                    match state {
                        session::NeedMore::Head => conn.ingress.protocol = Protocol::Head,
                        session::NeedMore::Line => {
                            let line = match parsing::Line::start(rest) {
                                Ok(line) => line,
                                Err(error) => {
                                    out.output = Output::Close(
                                        disp.as_mut().reject_line(permit, conn, error),
                                    );
                                    break;
                                }
                            };
                            conn.ingress.protocol = Protocol::Line(line);
                            out.consumed += rest.as_slice().len();
                        }
                    }
                    out.progress.merge(deadline::Progress::new(
                        deadline::Head::Pending,
                        deadline::Body::None,
                    ));
                    permit.defer();
                    break;
                }
                session::Outcome::Complete {
                    permit,
                    consumption: frame,
                    response: reply,
                    connection: next,
                } => {
                    let drain = match drain::Drain::apply(
                        &mut conn.ingress,
                        frame,
                        &mut rest,
                        &mut out.consumed,
                    ) {
                        Ok(drain) => drain,
                        Err(error) => {
                            out.output = Output::Close(disp.as_mut().abort_body(conn, error));
                            break;
                        }
                    };
                    out.connection = out.connection.join(next);
                    let (written, body) = match reply {
                        output::Complete::Inline { written: wrote }
                        | output::Complete::Copy { written: wrote, .. } => (wrote, None),
                        output::Complete::Static {
                            hdr_written: head,
                            body,
                        } => (head, Some(SplitBody::Static(body))),
                        output::Complete::Shared {
                            hdr_written: head,
                            body,
                        }
                        | output::Complete::Owned {
                            hdr_written: head,
                            body,
                            ..
                        } => (head, Some(SplitBody::Shared(body))),
                        output::Complete::Compressed {
                            hdr_written: head,
                            body,
                            ..
                        } => (head, Some(SplitBody::Frozen(body))),
                        output::Complete::Segments {
                            hdr_written: head,
                            body,
                        }
                        | output::Complete::Linked {
                            hdr_written: head,
                            body,
                        } => (head, Some(SplitBody::Segmented(body))),
                    };
                    if let Err(error) = Self::try_advance(&mut write, written) {
                        out.output = Output::Rejected(error);
                        break;
                    }
                    match body {
                        None => {}
                        Some(SplitBody::Static(body)) => {
                            if let Some(target) = write.split_off_mut(..body.len()) {
                                target.copy_from_slice(body.as_slice());
                            } else {
                                out.output = Output::Split(SplitBody::Static(body));
                                break;
                            }
                        }
                        Some(body) => {
                            out.output = Output::Split(body);
                            break;
                        }
                    }
                    if matches!(drain, drain::Drain::Pending) {
                        break;
                    }
                    permit
                }
                session::Outcome::Switch {
                    next,
                    buffered,
                    response: reply,
                } => {
                    if let Err(error) = drain::Drain::buffered(
                        &mut conn.ingress,
                        buffered,
                        &mut rest,
                        &mut out.consumed,
                    ) {
                        out.output = Output::Close(disp.as_mut().abort_body(conn, error));
                        break;
                    }
                    let staged = match conn.transitions.try_stage(next) {
                        Ok(staged) => staged,
                        Err(error) => {
                            out.output = Output::Rejected(pump::Reject::Switch {
                                actual: error.mode(),
                                attempted: error.attempted(),
                            });
                            break;
                        }
                    };
                    out.tail = Tail::Handoff {
                        staged,
                        start: out.consumed,
                    };
                    match reply {
                        output::Switch::Inline { written: wrote } => {
                            if let Err(error) = Self::try_advance(&mut write, wrote) {
                                out.output = Output::Rejected(error);
                            }
                        }
                        output::Switch::Static(reply) => {
                            if let Some(target) = write.split_off_mut(..reply.len()) {
                                target.copy_from_slice(reply.as_slice());
                            } else {
                                out.output = Output::Split(SplitBody::Static(reply));
                            }
                        }
                    }
                    break;
                }
                session::Outcome::Streamed {
                    written: wrote,
                    close,
                    ..
                } => {
                    out.connection = out.connection.join(close);
                    if let Err(error) = Self::try_advance(&mut write, wrote) {
                        out.output = Output::Rejected(error);
                        break;
                    }
                    out.output = Output::Streamed;
                    break;
                }
                session::Outcome::Park { close, .. } => {
                    out.connection = out.connection.join(close);
                    out.output = Output::Park;
                    break;
                }
                session::Outcome::Failed(error) => {
                    out.output = Output::Close(disp.as_mut().failure(&error));
                    break;
                }
                session::Outcome::Close(reason) => {
                    out.output = Output::Close(reason);
                    break;
                }
            };
        }
        if conn.ingress.is_discard()
            || matches!(
                conn.ingress.protocol,
                Protocol::Body(_) | Protocol::Working(_)
            )
        {
            out.progress.merge(deadline::Progress::new(
                deadline::Head::Body,
                deadline::Body::None,
            ));
        }
        out.written = capacity - write.len();
        out
    }

    fn try_advance(write: &mut &mut [u8], amount: usize) -> Result<(), pump::Reject> {
        consume::Consume::try_consume(write, amount).map_err(|error| {
            pump::Reject::new(pump::Operation::Write, pump::Cause::Capacity(error))
        })
    }
}
