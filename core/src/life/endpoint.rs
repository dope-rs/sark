//! Endpoint-owned transport evidence.

use std::{convert::Infallible, num::NonZero};

use dope_core::{
    Errno,
    driver::result::SubmitError,
    io::{datagram, datagram::ReceiveError, transfer},
};

/// Terminal result for one endpoint-owned payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Sent { bytes: transfer::Len },
    Failed { errno: Errno },
    Submit(SubmitError),
    Shutdown,
}

/// Physical receive backing at the final endpoint sample point.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub receive_capacity_bytes: Option<datagram::Capacity>,
    pub receive_resident_bytes: Option<datagram::Resident>,
}

/// One endpoint mutation committed by its production owner.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Event<E, O> {
    Opened(E),
    Receive {
        endpoint: E,
        error: ReceiveError,
    },
    Accepted {
        owner: O,
        bytes: usize,
        datagrams: NonZero<usize>,
        segmented: bool,
    },
    Transferred {
        from: O,
        to: O,
    },
    Settled {
        owner: O,
        bytes: usize,
        outcome: Outcome,
    },
    Audit {
        endpoint: E,
        snapshot: Snapshot,
    },
    Closed(E),
}

/// Statically dispatched endpoint evidence receiver.
pub trait Observer<E, O> {
    fn observe(&mut self, event: Event<E, O>);
}

impl<E, O> Observer<E, O> for Infallible {
    fn observe(&mut self, _: Event<E, O>) {
        match *self {}
    }
}

/// Compile-time endpoint evidence mode with explicit disabled absence.
pub trait Mode<E, O> {
    const ENABLED: bool;

    type Observer: Observer<E, O>;

    fn observer(&mut self) -> Option<&mut Self::Observer>;

    fn observe(&mut self, event: Event<E, O>) {
        if let Some(out) = self.observer() {
            out.observe(event);
        }
    }
}
