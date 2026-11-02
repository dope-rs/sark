//! Ingress transaction boundary.

use core::{mem, pin};

use o3::buffer::Complete as _;
use session::batch;

use crate::{
    pump,
    server::deadline,
    session, switch,
    wire::{head, output, recv},
};

/// Stateless owner of HTTP ingress transitions.
pub enum Ingress {}

enum Source<'a> {
    Borrowed(&'a [u8]),
    Buffered,
}

const _: () = {
    assert!(mem::size_of::<Source<'static>>() == mem::size_of::<&[u8]>());
    assert!(mem::align_of::<Source<'static>>() == mem::align_of::<&[u8]>());
    assert!(!mem::needs_drop::<Source<'static>>());
};

impl Ingress {
    /// Handles states which cannot acquire a transport write buffer.
    pub fn preflight<'d, Runtime, Pending, Transitions: switch::Engine, Ready, Heads>(
        conn: &mut session::Conn<'d, Runtime, Pending, Transitions, Ready, Heads>,
        input: &[u8],
    ) -> pump::Preflight
    where
        Runtime: pump::Tasks,
    {
        match conn.mode() {
            switch::Mode::Switching(kind) => {
                return pump::Preflight::from(
                    conn.ingress
                        .recv
                        .backlog(pump::Operation::Switch(kind), input),
                );
            }
            switch::Mode::Raw(kind) => {
                return pump::Preflight::Rejected(pump::Reject::Raw(kind));
            }
            switch::Mode::Http => {}
        }
        match conn.async_state.activity() {
            pump::Activity::Idle => {}
            pump::Activity::Task | pump::Activity::Stream => {
                return pump::Preflight::from(
                    conn.ingress.recv.backlog(pump::Operation::Task, input),
                );
            }
        }
        if conn.is_draining() || conn.deferred_action.is_some() {
            conn.ingress.discard_remaining -= input.len().min(conn.ingress.discard_remaining);
            return pump::Preflight::Settled;
        }
        if conn.ingress.discard_remaining > 0 {
            let take = input.len().min(conn.ingress.discard_remaining);
            let (_, tail) = input.split_at(take);
            return match conn.ingress.absorb(tail) {
                Ok(()) => {
                    conn.ingress.discard_remaining -= take;
                    if !tail.is_empty() {
                        pump::Preflight::Resume
                    } else if conn.ingress.discard_remaining == 0 {
                        pump::Preflight::Finished
                    } else {
                        pump::Preflight::Settled
                    }
                }
                Err(error) => pump::Preflight::Rejected(error),
            };
        }
        pump::Preflight::Drive
    }

    /// Retains ingress while transport write capacity is unavailable.
    pub fn retain<'d, Runtime, Pending, Transitions: switch::Engine, Ready, Heads>(
        conn: &mut session::Conn<'d, Runtime, Pending, Transitions, Ready, Heads>,
        input: &[u8],
    ) -> pump::Retention {
        let window = conn.ingress.recv_window();
        let progress = match (input.is_empty(), window) {
            (false, recv::Window::Head) => {
                deadline::Progress::new(deadline::Head::Pending, deadline::Body::None)
            }
            (false, recv::Window::Body) => {
                deadline::Progress::new(deadline::Head::Body, deadline::Body::Progress)
            }
            (true, recv::Window::Head) if conn.ingress.recv.is_ready() => {
                deadline::Progress::new(deadline::Head::Pending, deadline::Body::None)
            }
            (true, recv::Window::Head | recv::Window::Body) => deadline::Progress::default(),
        };
        let (state, progress) = match conn.ingress.absorb(input) {
            Ok(()) => (Ok(()), progress),
            Err(error) => (
                Err(error),
                deadline::Progress::new(progress.head(), deadline::Body::None),
            ),
        };
        pump::Retention {
            state,
            progress,
            discard_remaining: conn.ingress.discard_remaining,
        }
    }

    /// Parses, dispatches, and settles one borrowed-input transaction.
    pub fn drive<'d, Runtime, Pending, Transitions, Ready, Heads, D>(
        conn: &mut session::Conn<'d, Runtime, Pending, Transitions, Ready, Heads>,
        disp: pin::Pin<&mut D>,
        input: &[u8],
        write: &mut [u8],
        prior: head::Connection,
    ) -> pump::Cycle<'d, Transitions::Staged>
    where
        Runtime: pump::Tasks,
        Transitions: switch::Engine,
        D: session::Dispatch<'d, Runtime, Pending, Transitions, Ready = Ready, Head = Heads>,
    {
        let prepared = match conn.ingress.recv.prepare(input) {
            Ok(prepared) => prepared,
            Err(error) => {
                return pump::Cycle::rejected(
                    error,
                    deadline::Progress::new(deadline::Head::Ready, deadline::Body::None),
                    conn.ingress.discard_remaining,
                );
            }
        };
        let (batch, source) = match prepared {
            Some(buffer) => {
                let view = crate::Input::Buffered(buffer.view());
                (
                    batch::Batch::drive(conn, disp, view, write, prior),
                    Source::Buffered,
                )
            }
            None => {
                let view = crate::Input::Borrowed(input);
                (
                    batch::Batch::drive(conn, disp, view, write, prior),
                    Source::Borrowed(input),
                )
            }
        };
        Self::settle(conn, batch, source)
    }

    /// Clears bytes accepted by the transport's native discard path.
    pub fn discard<'d, Runtime, Pending, Transitions: switch::Engine, Ready, Heads>(
        conn: &mut session::Conn<'d, Runtime, Pending, Transitions, Ready, Heads>,
    ) {
        conn.ingress.end_discard();
    }

    /// Returns the running task's retained connection intent.
    pub fn deferred<'d, Runtime, Pending, Transitions, Ready, Heads>(
        conn: &session::Conn<'d, Runtime, Pending, Transitions, Ready, Heads>,
    ) -> head::Connection {
        conn.deferred
    }

    fn settle<'d, Runtime: pump::Tasks, Pending, Transitions: switch::Engine, Ready, Heads>(
        conn: &mut session::Conn<'d, Runtime, Pending, Transitions, Ready, Heads>,
        batch: batch::Batch<'d, Transitions::Staged>,
        source: Source<'_>,
    ) -> pump::Cycle<'d, Transitions::Staged> {
        let (tail, accepted) = match (source, batch.tail) {
            (_, batch::Tail::Absorb)
                if matches!(batch.connection, head::Connection::Close)
                    || matches!(
                        batch.output,
                        batch::Output::Close(_) | batch::Output::Rejected(_)
                    ) =>
            {
                conn.ingress.recv = recv::Buffer::default();
                (pump::Tail::Empty, Ok(()))
            }
            (Source::Buffered, batch::Tail::Absorb | batch::Tail::Handoff { .. }) => {
                (pump::Tail::Empty, conn.ingress.recv.consume(batch.consumed))
            }
            (Source::Borrowed(input), batch::Tail::Handoff { staged, start }) => {
                let tail = if start < input.len() {
                    pump::Tail::Handoff {
                        staged,
                        range: start..input.len(),
                    }
                } else {
                    pump::Tail::Empty
                };
                (tail, Ok(()))
            }
            (Source::Borrowed(input), batch::Tail::Absorb) => {
                let accepted = match input.get(batch.consumed..) {
                    Some(tail) => conn.ingress.absorb(tail),
                    None => Err(pump::Reject::new(
                        pump::Operation::Consume,
                        pump::Cause::Range {
                            offset: batch.consumed,
                            capacity: input.len(),
                        },
                    )),
                };
                (pump::Tail::Empty, accepted)
            }
        };
        match accepted {
            Ok(()) => {}
            Err(error) => {
                return pump::Cycle::rejected(
                    error,
                    batch.progress,
                    conn.ingress.discard_remaining,
                );
            }
        }

        conn.deferred = batch.connection;
        let (egress, intent) = match (conn.async_state.activity(), batch.output) {
            (pump::Activity::Task | pump::Activity::Stream, batch::Output::Streamed) => (
                pump::Egress::Write {
                    prefix: batch.written,
                    body: pump::Body::None,
                },
                head::Connection::KeepAlive,
            ),
            (pump::Activity::Task | pump::Activity::Stream, batch::Output::Park) => {
                (pump::Egress::Wake, head::Connection::KeepAlive)
            }
            (_, batch::Output::Split(body)) => (
                pump::Egress::Write {
                    prefix: batch.written,
                    body: pump::Body::from_split(body),
                },
                batch.connection,
            ),
            (_, batch::Output::Close(reason)) if batch.written > 0 => {
                conn.deferred_action = Some(reason);
                (
                    pump::Egress::Write {
                        prefix: batch.written,
                        body: pump::Body::None,
                    },
                    batch.connection,
                )
            }
            (_, batch::Output::None | batch::Output::Park | batch::Output::Yield)
                if batch.written > 0 =>
            {
                (
                    pump::Egress::Write {
                        prefix: batch.written,
                        body: pump::Body::None,
                    },
                    batch.connection,
                )
            }
            (_, batch::Output::Streamed) => (
                pump::Egress::Write {
                    prefix: batch.written,
                    body: pump::Body::None,
                },
                batch.connection,
            ),
            (_, batch::Output::Yield) => (pump::Egress::Wake, batch.connection),
            (_, batch::Output::Close(reason)) => {
                conn.deferred = head::Connection::Close;
                pump::Egress::output(output::Plan::Close(reason))
            }
            (_, batch::Output::None | batch::Output::Park) => {
                (pump::Egress::None, batch.connection)
            }
            (_, batch::Output::Rejected(error)) => {
                return pump::Cycle::rejected(
                    error,
                    batch.progress,
                    conn.ingress.discard_remaining,
                );
            }
        };
        pump::Cycle::ready(
            egress,
            batch.progress,
            intent,
            conn.ingress.discard_remaining,
            tail,
        )
    }
}
