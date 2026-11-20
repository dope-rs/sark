//! Transport-neutral HTTP/1 connection pump.

use core::convert::Infallible;
use std::{mem, ops};

use o3::buffer::storage;
use sark_http::{
    h1::switch::Kind,
    payload::{compress, fixed, owned},
};

use crate::{
    server::deadline,
    wire::{head, output},
};

mod drain;
mod reject;

pub use drain::Drain;
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
    Switch(Kind),
    Task,
}

impl Preflight {
    pub(crate) fn from(state: Result<(), Reject>) -> Self {
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
    Inline,
    Static {
        body: fixed::Body<'d>,
    },
    Shared {
        body: storage::Shared,
    },
    Frozen {
        body: compress::Frozen,
    },
    Segmented {
        body: owned::Segments<'d>,
    },
}

impl<'d> Egress<'d> {
    pub fn output(action: output::Outcome<'d>) -> (Self, head::Connection) {
        match action {
            output::Outcome::Send { connection }
            | output::Outcome::SendCopied { connection, .. } => (Self::Inline, connection),
            output::Outcome::SendStatic { body, connection } => (Self::Static { body }, connection),
            output::Outcome::SendShared { body, connection }
            | output::Outcome::SendOwnedCopy {
                body, connection, ..
            } => (Self::Shared { body }, connection),
            output::Outcome::SendCompressed {
                body, connection, ..
            } => (Self::Frozen { body }, connection),
            output::Outcome::SendSegmented { body, connection }
            | output::Outcome::SendLinked { body, connection } => {
                (Self::Segmented { body }, connection)
            }
            output::Outcome::SendRequestBody(pending) => match pending.body {},
            output::Outcome::Close(reason) => Self::close(reason),
            output::Outcome::Failed(error) => Self::close(error.body()),
        }
    }

    pub(crate) fn close(reason: fixed::Body<'d>) -> (Self, head::Connection) {
        let action = if reason.is_empty() {
            Self::None
        } else {
            Self::Static { body: reason }
        };
        (action, head::Connection::Close)
    }
}

/// Exact bounded-retention result before any transport output is constructed.
pub struct Retention {
    pub(crate) state: Result<(), Reject>,
    pub(crate) progress: deadline::Progress,
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
    pub tail: Tail<H>,
}

impl<'d, H> Cycle<'d, H> {
    pub(crate) fn rejected(error: Reject, progress: deadline::Progress) -> Self {
        Self {
            state: Err(error),
            progress,
            connection: head::Connection::Close,
            tail: Tail::Empty,
        }
    }
}

/// Work after the transport completes a write submission.
#[must_use = "final drain may require another turn or a protocol switch"]
pub enum Completion {
    None,
    Resume,
    /// A switching response committed its raw-protocol owner.
    Switch,
}

/// Work that cannot commit a raw-protocol switch.
#[must_use = "send completion may require a close response or buffered-input resume"]
pub enum Next<'d> {
    None,
    Resume,
    Close(fixed::Body<'d>),
}
