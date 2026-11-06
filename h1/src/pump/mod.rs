//! Transport-neutral HTTP/1 connection pump.

use core::convert::Infallible;
use std::{mem, ops};

use o3::{
    alloc,
    buffer::{storage, write::consume},
    mem::rc,
};
use sark_http::{
    h1::switch::Kind,
    payload::{compress, fixed, owned},
};

use crate::{
    server::deadline,
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

/// Transport-independent command produced by one pump cycle.
pub enum Egress<'d> {
    None,
    /// The runtime parked work and needs its target scheduled.
    Wake,
    Inline {
        prefix: usize,
    },
    Static {
        prefix: usize,
        body: fixed::Body<'d>,
    },
    Shared {
        prefix: usize,
        body: storage::Shared,
    },
    Frozen {
        prefix: usize,
        body: compress::Frozen,
    },
    Segmented {
        prefix: usize,
        body: owned::Segments<'d>,
    },
}

impl<'d> Egress<'d> {
    pub(crate) fn output(action: output::Outcome<'d>) -> (Self, head::Connection) {
        match action {
            output::Outcome::Send {
                written: prefix,
                connection,
            }
            | output::Outcome::SendCopied {
                written: prefix,
                connection,
                ..
            } => (Self::Inline { prefix }, connection),
            output::Outcome::SendStatic {
                hdr_written: prefix,
                body,
                connection,
            } => (Self::Static { prefix, body }, connection),
            output::Outcome::SendShared {
                hdr_written: prefix,
                body,
                connection,
            }
            | output::Outcome::SendOwnedCopy {
                hdr_written: prefix,
                body,
                connection,
                ..
            } => (Self::Shared { prefix, body }, connection),
            output::Outcome::SendCompressed {
                hdr_written: prefix,
                body,
                connection,
                ..
            } => (Self::Frozen { prefix, body }, connection),
            output::Outcome::SendSegmented {
                hdr_written: prefix,
                body,
                connection,
            }
            | output::Outcome::SendLinked {
                hdr_written: prefix,
                body,
                connection,
            } => (Self::Segmented { prefix, body }, connection),
            output::Outcome::SendRequestBody(pending) => match pending.body {},
            output::Outcome::Close(reason) => Self::close(reason),
            output::Outcome::Failed(error) => Self::close(error.body()),
        }
    }

    pub(crate) fn close(reason: fixed::Body<'d>) -> (Self, head::Connection) {
        let action = if reason.is_empty() {
            Self::None
        } else {
            Self::Static {
                prefix: 0,
                body: reason,
            }
        };
        (action, head::Connection::Close)
    }
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
