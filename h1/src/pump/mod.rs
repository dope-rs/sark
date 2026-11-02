//! Transport-neutral HTTP/1 connection pump.

use core::convert::Infallible;
use std::{mem, ops};

use o3::{
    alloc,
    buffer::{pool, pool::owners, storage, write::consume},
    mem::rc,
};
use sark_http::{
    h1::switch::Kind,
    payload::{compress::Gzip, fixed, owned},
};
use session::batch;

use crate::{
    server::deadline,
    session,
    wire::{head, output},
};

mod drain;
mod ingress;
mod reject;

pub use drain::Drain;
pub use ingress::Ingress;
pub use reject::Reject;

/// Exact disposition before an HTTP ingress drive acquires transport output.
pub enum Preflight {
    Drive,
    /// Input was retained after a discarded body; drive with no new bytes.
    Resume,
    /// The discarded request body ended without a following request byte.
    Finished,
    Settled,
    Rejected(Reject),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Operation {
    Prepare,
    Consume,
    Append,
    Write,
    Switch(Kind),
    Task,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cause {
    Capacity(consume::Error),
    Consume(consume::Error),
    Range { offset: usize, capacity: usize },
    Exhausted(alloc::Exhausted),
    Retain(rc::Error),
}

impl Preflight {
    fn from(state: Result<(), Reject>) -> Self {
        match state {
            Ok(()) => Self::Settled,
            Err(error) => Self::Rejected(error),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Preflight>() == mem::size_of::<Option<Result<(), Reject>>>());
    assert!(mem::align_of::<Preflight>() == mem::align_of::<Reject>());
    assert!(!mem::needs_drop::<Preflight>());
};

/// Exact scheduling activity carried by a connection.
#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum Activity {
    Idle,
    Task,
    Stream,
}

/// Static scheduling capability carried by a connection.
pub trait Tasks {
    fn activity(&self) -> Activity;
}

impl Tasks for () {
    fn activity(&self) -> Activity {
        Activity::Idle
    }
}

const _: () = assert!(mem::size_of::<Activity>() == 1);

/// Body ownership carried across the transport submission boundary.
pub enum Body<'d> {
    None,
    Static(fixed::Body<'d>),
    Shared(storage::Shared),
    Frozen(
        pool::Span<
            owners::Plain<pool::Fixed<{ Gzip::SLOTS.get() }, { Gzip::CAPACITY.get() }>>,
            pool::Owned,
            pool::Frozen,
        >,
    ),
    Segmented(owned::Segments<'d>),
}

impl<'d> Body<'d> {
    /// Exact owner charge submitted to the transport.
    fn retention(&self) -> output::Owner {
        match self {
            Self::None | Self::Static(_) => output::Owner::default(),
            Self::Shared(body) => output::Owner::shared(body),
            Self::Frozen(body) => output::Owner::shared(body),
            Self::Segmented(body) => output::Owner::segmented(body),
        }
    }

    fn from_split(body: batch::SplitBody<'d>) -> Self {
        match body {
            batch::SplitBody::Static(body) => Self::Static(body),
            batch::SplitBody::Shared(body) => Self::Shared(body),
            batch::SplitBody::Frozen(body) => Self::Frozen(body),
            batch::SplitBody::Segmented(body) => Self::Segmented(body),
        }
    }
}

/// Transport-independent command produced by one pump cycle.
pub enum Egress<'d> {
    None,
    /// The runtime parked work and needs its target scheduled.
    Wake,
    /// Initialized prefix followed by its unchanged body owner.
    Write {
        prefix: usize,
        body: Body<'d>,
    },
}

impl<'d> Egress<'d> {
    pub(crate) fn output(action: output::Plan<'d>) -> (Self, head::Connection) {
        let (prefix, body, connection) = match action {
            output::Plan::Inline {
                written,
                connection,
            } => (written, Body::None, connection),
            output::Plan::Static {
                hdr_written,
                body,
                connection,
            } => (hdr_written, Body::Static(body), connection),
            output::Plan::Shared {
                hdr_written,
                body,
                connection,
            } => (hdr_written, Body::Shared(body), connection),
            output::Plan::Frozen {
                hdr_written,
                body,
                connection,
            } => (hdr_written, Body::Frozen(body), connection),
            output::Plan::Segments {
                hdr_written,
                body,
                connection,
            } => (hdr_written, Body::Segmented(body), connection),
            output::Plan::Close(reason) if reason.is_empty() => {
                return (Self::None, head::Connection::Close);
            }
            output::Plan::Close(reason) => (0, Body::Static(reason), head::Connection::Close),
        };
        (Self::Write { prefix, body }, connection)
    }

    pub(crate) fn retention(&self) -> output::Owner {
        match self {
            Self::Write { body, .. } => body.retention(),
            Self::None | Self::Wake => output::Owner::default(),
        }
    }

    pub(crate) fn commit<S: Sink<'d>>(self, conn: head::Connection, mut sink: S) {
        match conn {
            head::Connection::KeepAlive => {}
            head::Connection::Close => sink.close(),
        }
        match self {
            Self::None => {}
            Self::Wake => sink.wake(),
            Self::Write { prefix, body } => match body {
                Body::None => sink.inline(prefix),
                Body::Static(body) => sink.static_body(prefix, body),
                Body::Shared(body) => sink.shared(prefix, body),
                Body::Frozen(body) => sink.frozen(prefix, body),
                Body::Segmented(body) => sink.segments(prefix, body),
            },
        }
    }
}

/// Consuming, statically selected transport submission capability.
pub trait Sink<'d>: Sized {
    fn close(&mut self);

    fn wake(&mut self);

    fn inline(self, prefix: usize);

    fn static_body(self, prefix: usize, body: fixed::Body<'d>);

    fn shared(self, prefix: usize, body: storage::Shared);

    fn frozen(
        self,
        prefix: usize,
        body: pool::Span<
            owners::Plain<pool::Fixed<{ Gzip::SLOTS.get() }, { Gzip::CAPACITY.get() }>>,
            pool::Owned,
            pool::Frozen,
        >,
    );

    fn segments(self, prefix: usize, body: owned::Segments<'d>);
}

/// Exact bounded-retention result before any transport output is constructed.
pub struct Retention {
    pub(crate) state: Result<(), Reject>,
    pub(crate) progress: deadline::Progress,
    pub(crate) discard_remaining: usize,
}

/// Exact callback-borrowed suffix disposition after an ingress cycle.
pub enum Tail<H = ()> {
    Empty,
    Handoff { staged: H, range: ops::Range<usize> },
}

const _: () = {
    assert!(mem::size_of::<Tail>() == 3 * mem::size_of::<usize>());
    assert!(mem::align_of::<Tail>() == mem::align_of::<usize>());
    assert!(mem::size_of::<Tail<Infallible>>() <= mem::size_of::<Tail<()>>());
};

/// Result of one ingress or buffered-resume transaction.
#[must_use = "the pump cycle contains transport output and connection intents"]
pub struct Cycle<'d, H = Infallible> {
    pub state: Result<Egress<'d>, Reject>,
    pub progress: deadline::Progress,
    pub connection: head::Connection,
    pub discard_remaining: usize,
    pub tail: Tail<H>,
}

impl<'d, H> Cycle<'d, H> {
    pub(crate) fn settled(left: usize) -> Self {
        let head = if left == 0 {
            deadline::Head::Ready
        } else {
            deadline::Head::Body
        };
        Self::ready(
            Egress::None,
            deadline::Progress::new(head, deadline::Body::None),
            head::Connection::KeepAlive,
            left,
            Tail::Empty,
        )
    }

    pub(crate) fn failed(error: Reject, left: usize) -> Self {
        Self::rejected(
            error,
            deadline::Progress::new(deadline::Head::Ready, deadline::Body::None),
            left,
        )
    }

    pub(crate) fn retained(held: Retention) -> Self {
        match held.state {
            Ok(()) => Self::ready(
                Egress::Wake,
                held.progress,
                head::Connection::KeepAlive,
                held.discard_remaining,
                Tail::Empty,
            ),
            Err(error) => Self::rejected(error, held.progress, held.discard_remaining),
        }
    }

    fn ready(
        egress: Egress<'d>,
        progress: deadline::Progress,
        conn: head::Connection,
        left: usize,
        tail: Tail<H>,
    ) -> Self {
        Self {
            state: Ok(egress),
            progress,
            connection: conn,
            discard_remaining: left,
            tail,
        }
    }

    fn rejected(error: Reject, progress: deadline::Progress, left: usize) -> Self {
        Self {
            state: Err(error),
            progress,
            connection: head::Connection::Close,
            discard_remaining: left,
            tail: Tail::Empty,
        }
    }

    /// Projects the owner charge before transport submission.
    pub(crate) fn retention(&self) -> output::Retention {
        match self.state.as_ref() {
            Ok(egress) => output::Retention::Response(egress.retention()),
            Err(_) => output::Retention::Rejected,
        }
    }

    pub(crate) fn commit<S: Sink<'d>>(self, mut sink: S) -> (Result<(), Reject>, Tail<H>) {
        let state = match self.state {
            Ok(egress) => {
                egress.commit(self.connection, sink);
                Ok(())
            }
            Err(error) => {
                match self.connection {
                    head::Connection::KeepAlive => {}
                    head::Connection::Close => sink.close(),
                }
                Err(error)
            }
        };
        (state, self.tail)
    }
}

/// Work after the transport completes a write submission.
#[must_use = "send completion may require a close response or buffered-input resume"]
pub enum Completion<'d> {
    None,
    Resume,
    /// A switching response committed its raw-protocol owner.
    Switch,
    Close(fixed::Body<'d>),
}

/// Work that cannot commit a raw-protocol switch.
#[must_use = "send completion may require a close response or buffered-input resume"]
pub enum Next<'d> {
    None,
    Resume,
    Close(fixed::Body<'d>),
}

impl<'d> From<Next<'d>> for Completion<'d> {
    fn from(done: Next<'d>) -> Self {
        match done {
            Next::None => Self::None,
            Next::Resume => Self::Resume,
            Next::Close(reason) => Self::Close(reason),
        }
    }
}
