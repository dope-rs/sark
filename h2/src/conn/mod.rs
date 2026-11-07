use core::{mem, num::NonZero};

use dope::core::Errno;
use o3::{alloc, buffer::resident, depot::capacity};
use sark_core::manifold::{client::io, schedule, stream};

use crate::{
    Endpoint,
    stream::{Id, Key, State},
};

pub mod build;
pub mod config;
mod error;
pub mod event;
pub(crate) mod ingress;
pub mod machine;
pub(crate) use sark_http::preface;
pub(crate) mod outputs;
pub(crate) mod registry;

pub use error::Error;
use outputs::payloads;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Build {
    Entropy(Errno),
    Streams(alloc::Error),
    Ingress(build::Ingress),
    Egress(build::Egress),
}

/// Exact reason a stream could not enter the fixed connection registry.
#[derive(PartialEq, Eq, Debug)]
pub enum Admission {
    Duplicate(Id),
    IdentityExhausted(Id),
    Full {
        stream: Id,
        used: capacity::Count,
        limit: usize,
    },
}

/// Exact violation observed while sequencing a CONTINUATION chain.
#[derive(PartialEq, Eq, Debug)]
pub enum Continuation {
    Type(u8),
    Stream { expected: Id, actual: Id },
    Empty(Id),
    Unexpected(Id),
    Limit { actual: u32, limit: u32 },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HeaderPhase {
    Idle,
    Active,
    Failed,
    Store,
    Discard,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct HeaderState {
    pub expected: HeaderPhase,
    pub actual: HeaderPhase,
}

/// Exact HTTP/2 rule violated by a connection operation.
#[derive(PartialEq, Eq, Debug)]
pub enum Protocol {
    Idle(Id),
    WindowUpdate {
        stream: Id,
        state: State,
    },
    Peer {
        actual: Id,
        last: Id,
        endpoint: Endpoint,
    },
    Push(Push),
    Association(Association),
}

/// Exact reason an inbound PUSH_PROMISE is forbidden.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Push {
    Server,
    Disabled,
}

/// Exact reason a stream cannot associate a pushed response.
#[derive(PartialEq, Eq, Debug)]
pub enum Association {
    Role { stream: Id, endpoint: Endpoint },
    Peer(Id),
    State { stream: Id, state: State },
    Idle(Id),
    Tunnel(Id),
}

/// Packed H2 stream identity and END_STREAM marker.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Message(crate::stream::raw::Marked);

impl Message {
    pub(crate) fn new(key: Key, boundary: stream::Boundary) -> Self {
        Self(crate::stream::raw::Marked::new(key, boundary))
    }
}

impl stream::Message for Message {
    type Key = Key;

    fn key(self) -> Self::Key {
        self.0.key()
    }

    fn boundary(self) -> stream::Boundary {
        self.0.boundary()
    }
}

/// The resource that prevented an outbound DATA transaction from committing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataBlock {
    ConnectionFlow,
    StreamFlow,
    Egress,
}

impl schedule::Block for DataBlock {
    fn wait(self) -> schedule::Wait {
        match self {
            Self::ConnectionFlow => schedule::Wait::Connection,
            Self::StreamFlow => schedule::Wait::Stream,
            Self::Egress => schedule::Wait::Egress,
        }
    }
}

/// Result of one atomic outbound DATA attempt.
/// A blocked result changes no credit, state, or queue ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum DataOutcome {
    Sent(NonZero<usize>),
    Ended,
    Blocked(DataBlock),
}

impl DataOutcome {
    pub(crate) fn sent(written: usize) -> Self {
        match NonZero::<usize>::new(written) {
            Some(written) => Self::Sent(written),
            None => Self::Ended,
        }
    }

    pub(in crate::conn) fn write(self) -> io::Outcome<DataBlock> {
        match self {
            Self::Sent(written) => io::Outcome::Written(written.get()),
            Self::Ended => io::Outcome::Written(0),
            Self::Blocked(block) => io::Outcome::Blocked(block),
        }
    }
}

/// Response-wide upper bound for body bytes materialized into the fixed H2
/// ring instead of retaining one final-link owner record.
#[doc(hidden)]
pub const MAX_MATERIALIZED_RESPONSE_TAIL: usize =
    payloads::Ledger::<resident::Shared<'_>>::MAX_MATERIALIZED_TAIL;

pub use sark_http::preface::CLIENT_PREFACE;
/// Connection security bound for peer-initiated stream resets.
/// Cancellation-heavy callers must rotate before crossing it.
pub const MAX_PEER_RESETS_PER_CONNECTION: u32 = 100;

const _: () = {
    assert!(mem::size_of::<Message>() == mem::size_of::<Key>());
    assert!(mem::size_of::<Option<Message>>() == mem::size_of::<Message>());
    assert!(mem::size_of::<Admission>() <= 4 * mem::size_of::<usize>());
    assert!(mem::size_of::<Continuation>() <= 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<HeaderPhase>() == 1);
    assert!(mem::size_of::<HeaderState>() == 2);
    assert!(mem::size_of::<Protocol>() <= 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<Association>() <= 2 * mem::size_of::<usize>());
    assert!(mem::size_of::<DataOutcome>() == 2 * mem::size_of::<usize>());
};
