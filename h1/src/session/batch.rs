use core::{convert::Infallible, mem, pin};

use o3::buffer::{
    storage,
    write::{Sink as _, Slice},
};
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
        mut write: Slice<'_, &mut usize>,
        prior: head::Connection,
    ) -> Self
    where
        Transitions: switch::Engine<Staged = H>,
        D: session::Dispatch<'d, Runtime, Pending, Transitions, Ready = Ready, Head = Heads>,
    {
        let mut out = Self {
            consumed: 0,
            output: Output::None,
            connection: prior,
            progress: deadline::Progress::default(),
            tail: Tail::Absorb,
        };
        let mut permit = session::Permit::new();
        let mut rest = input;
        let capacity = write.remaining();
        let minimum = capacity.min(1024);
        for _ in 0..MAX_PIPELINE {
            if write.remaining() < minimum || matches!(out.connection, head::Connection::Close) {
                break;
            }
            if rest.as_slice().is_empty() && !conn.ingress.is_ready() {
                break;
            }
            let mut staged = write
                .try_split(0, write.remaining())
                .unwrap_or_else(|_| unreachable!("output"));
            let mut target = staged.body();
            out.progress.merge(deadline::Progress::new(
                deadline::Head::Ready,
                deadline::Body::None,
            ));
            let (result, consumed) = match conn.ingress.protocol.take_frame(rest) {
                Frame::Head(pending) => {
                    let result = match pending {
                        HeadFrame::Pending(pending) => disp.as_mut().resume_head(
                            pending,
                            permit,
                            rest,
                            target.reborrow(),
                            conn,
                        ),
                        HeadFrame::New => {
                            disp.as_mut()
                                .try_consume(permit, rest, target.reborrow(), conn)
                        }
                        HeadFrame::Rejected(pending) => disp.as_mut().resume_error(
                            pending,
                            permit,
                            rest,
                            target.reborrow(),
                            conn,
                        ),
                        HeadFrame::Line(line) => match line {
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
                                    .consume_line(permit, source, target.reborrow(), conn)
                                    .after_line(prior);
                                match result {
                                    session::RouteOutcome::AwaitHead { permit, pending }
                                        if used < rest.as_slice().len() =>
                                    {
                                        out.consumed += rest.consume_max(used);
                                        disp.as_mut().resume_head(
                                            pending,
                                            permit,
                                            rest,
                                            target.reborrow(),
                                            conn,
                                        )
                                    }
                                    session::RouteOutcome::AwaitReject { permit, pending }
                                        if used < rest.as_slice().len() =>
                                    {
                                        out.consumed += rest.consume_max(used);
                                        disp.as_mut().resume_error(
                                            pending,
                                            permit,
                                            rest,
                                            target.reborrow(),
                                            conn,
                                        )
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
                    let result =
                        disp.as_mut()
                            .finish_body(task, done, permit, target.reborrow(), app);
                    (result, consumed)
                }
                Frame::Ready(task) => {
                    let result = disp.as_mut().resume_body(
                        task,
                        permit,
                        target.reborrow(),
                        &mut conn.async_state,
                    );
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
                    let body = match reply {
                        output::Complete::Inline | output::Complete::Copy { .. } => None,
                        output::Complete::Static { body } => Some(SplitBody::Static(body)),
                        output::Complete::Shared { body }
                        | output::Complete::Owned { body, .. } => Some(SplitBody::Shared(body)),
                        output::Complete::Compressed { body, .. } => Some(SplitBody::Frozen(body)),
                        output::Complete::Segments { body } | output::Complete::Linked { body } => {
                            Some(SplitBody::Segmented(body))
                        }
                    };
                    match body {
                        None => {}
                        Some(SplitBody::Static(body)) => {
                            if target.write_slice(body.as_slice()).is_err() {
                                out.output = Output::Split(SplitBody::Static(body));
                                staged
                                    .try_commit()
                                    .unwrap_or_else(|_| unreachable!("output"));
                                break;
                            }
                        }
                        Some(body) => {
                            out.output = Output::Split(body);
                            staged
                                .try_commit()
                                .unwrap_or_else(|_| unreachable!("output"));
                            break;
                        }
                    }
                    staged
                        .try_commit()
                        .unwrap_or_else(|_| unreachable!("output"));
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
                    let transition = match conn.transitions.try_stage(next) {
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
                        staged: transition,
                        start: out.consumed,
                    };
                    match reply {
                        output::Switch::Inline => {}
                        output::Switch::Static(reply) => {
                            if target.write_slice(reply.as_slice()).is_err() {
                                out.output = Output::Split(SplitBody::Static(reply));
                            }
                        }
                    }
                    staged
                        .try_commit()
                        .unwrap_or_else(|_| unreachable!("output"));
                    break;
                }
                session::Outcome::Streamed { close, .. } => {
                    out.connection = out.connection.join(close);
                    staged
                        .try_commit()
                        .unwrap_or_else(|_| unreachable!("output"));
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
        out
    }
}
