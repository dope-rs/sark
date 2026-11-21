//! Transport-neutral HTTP/1 connection pump.

use core::convert::Infallible;
use std::{mem, ops};

use sark_http::{h1::switch::Kind, payload::fixed};

use crate::server::deadline;

mod reject;

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

/// Work that cannot commit a raw-protocol switch.
#[must_use = "send completion may require a close response or buffered-input resume"]
pub enum Next<'d> {
    None,
    Resume,
    Close(fixed::Body<'d>),
}
