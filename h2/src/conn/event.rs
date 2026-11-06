use std::convert;

use o3::{
    buffer::{self, hold, pool, pool::owners, segments, storage},
    mem,
};
use sark_core::{
    life::session,
    manifold::stream::{Boundary, Payload, Section},
};
use sark_http::field::metadata::{
    block, packed,
    section::{
        rules,
        state::{RetainedHead, Selected},
    },
};

use crate::{conn::outputs::payloads, flow, frame, frame::numbers, hpack::decode::limit, stream};

/// Smallest contiguous prefix owner that can always join one retained DATA
/// body without first consuming protocol state.
#[doc(hidden)]
pub const MIN_RETAINED_WRITE_PREFIX_CAPACITY: usize = frame::HEADER_LEN + payloads::INLINE_PREFIX;

pub use crate::conn::machine::ready::Incoming as Ready;

/// Exact header admission and semantic validation that rejected a stream.
#[derive(Clone, Copy, Debug)]
pub struct Rejected {
    pub admit: limit::Admission,
    pub sealed: block::Validation,
    pub finished: block::Validation,
}

const _: () = assert!(size_of::<Option<Rejected>>() == 3);

#[derive(Debug)]
pub enum Event<
    S = Selected<rules::RetainRaw>,
    B = packed::Decoded,
    C: session::Storage = storage::Shared,
    E: Evidence = Retirement,
> {
    PrefaceComplete,
    SettingsApplied,
    SettingsAck,
    Ping {
        ack: frame::Ack,
        opaque: [u8; 8],
    },
    GoAway {
        last_stream_id: stream::Id,
        error: numbers::ErrorCode,
        debug: DataPayload<C>,
    },
    Headers {
        stream_id: stream::Key,
        head: RetainedHead<B, S, Option<block::Plan>>,
        boundary: Boundary,
        section: Section,
    },
    Data {
        stream_id: stream::Key,
        data: DataPayload<C>,
        boundary: Boundary,
    },
    StreamReset {
        stream_id: stream::Key,
        error: numbers::ErrorCode,
        /// Retained DATA canceled atomically by this peer reset.
        #[doc(hidden)]
        retired: Option<E::Retired>,
        validation: Option<Rejected>,
    },
    /// A late peer reset retired retained DATA for a stream whose semantic
    /// state was already terminal. The owner has already been dropped by the
    /// H2 egress ledger; this is exact lifecycle evidence for that release.
    EgressRetired {
        write: E::Retired,
    },
    PushPromise {
        stream_id: stream::Id,
        promised_stream_id: stream::Key,
        head: RetainedHead<B, S, Option<block::Plan>>,
    },
}

#[must_use = "the returned write owns bytes removed from the connection"]
pub enum Write<E = Retirement, C: session::Storage = storage::Shared> {
    Buffered(usize),
    Owned {
        stream_id: stream::Key,
        prefix_len: usize,
        body: C,
    },
    Split {
        /// Canonical ownership charge minted by the H2 ledger at the exact
        /// staging-to-transport handoff.
        #[doc(hidden)]
        write: E,
        prefix_len: usize,
        body: C,
    },
}

#[doc(hidden)]
/// Copyable lifecycle metadata emitted after the retained owner is released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retirement {
    /// Exact generational stream owner of the retained DATA range.
    pub stream_id: stream::Key,
    /// Bytes projected from a retained owner. DATA headers and copied inline
    /// protocol prefixes carry no allocation owner and are excluded.
    pub charge: hold::Charge,
}

#[doc(hidden)]
pub trait Evidence: Sized {
    type Retired: PartialEq + Eq;

    #[doc(hidden)]
    const OBSERVED: bool;

    #[doc(hidden)]
    fn project(write: Retirement) -> Option<Self::Retired>;
}

impl Evidence for Retirement {
    type Retired = Self;
    const OBSERVED: bool = true;

    fn project(write: Retirement) -> Option<Self::Retired> {
        Some(write)
    }
}

#[doc(hidden)]
pub enum Ignore {}

impl Evidence for Ignore {
    type Retired = convert::Infallible;
    const OBSERVED: bool = false;

    fn project(_: Retirement) -> Option<Self::Retired> {
        None
    }
}

#[must_use = "staged DATA owns flow credit until the machine accepts or cancels it"]
pub(crate) struct Staging {
    pub(crate) charge: hold::Charge,
    pub(crate) credit: flow::Reserved,
}

impl Staging {
    pub(crate) fn into_parts(self) -> (hold::Charge, flow::Reserved) {
        (self.charge, self.credit)
    }
}

const _: () = assert!(
    core::mem::size_of::<Staging>() == core::mem::size_of::<(hold::Charge, flow::Reserved)>()
);

impl Retirement {
    pub(crate) fn from_body<C: session::Storage>(stream: stream::Key, body: &C) -> Self {
        Self {
            stream_id: stream,
            charge: hold::Charge::from_owner(
                body.as_ref().len(),
                buffer::Residency::resident(body),
            ),
        }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Ignore>() == 0);
    assert!(core::mem::size_of::<Option<convert::Infallible>>() == 0);
};

#[derive(Debug)]
pub struct DataPayload<C: session::Storage = storage::Shared> {
    segments: segments::Chain<C>,
    permit: pool::Span<owners::Plain<pool::Width<1>>, pool::Owned, pool::Frozen>,
}

impl<C: session::Storage> DataPayload<C> {
    #[doc(hidden)]
    pub fn new(
        segments: segments::Chain<C>,
        permit: pool::Span<owners::Plain<pool::Width<1>>, pool::Owned, pool::Frozen>,
    ) -> Self {
        Self { segments, permit }
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn count(&self) -> segments::Count {
        self.segments.count()
    }

    pub fn contiguous(&self) -> Option<&[u8]> {
        self.segments.contiguous()
    }

    pub fn segments(&self) -> segments::Iter<'_, C> {
        self.segments.segments()
    }

    /// Visits every retained segment as bytes, in order.
    pub fn for_each(&self, mut visit: impl FnMut(&[u8])) {
        for part in self.segments.segments() {
            visit(part.as_ref());
        }
    }

    /// Moves DATA into its body, preserving both owners and the permit on rejection.
    pub(crate) fn try_append(
        mut self,
        target: &mut Option<segments::Chain<C>>,
    ) -> Result<(), mem::Rejected<Self, segments::Error>> {
        if self.is_empty() {
            self.discard();
            return Ok(());
        }
        match target {
            Some(target) => {
                if let Err(error) = target.try_append(&mut self.segments) {
                    return Err(mem::Rejected::new(self, error));
                }
                self.discard();
            }
            None => {
                let Self { segments, permit } = self;
                *target = Some(segments);
                drop(permit);
            }
        }
        Ok(())
    }

    /// Releases the retained segments before returning the pool permit.
    pub fn discard(self) {
        drop(self);
    }
}

impl<C: session::Storage> Payload<C> for DataPayload<C> {
    type Permit = pool::Span<owners::Plain<pool::Width<1>>, pool::Owned, pool::Frozen>;

    fn into_parts(self) -> (segments::Chain<C>, Self::Permit) {
        (self.segments, self.permit)
    }
}
