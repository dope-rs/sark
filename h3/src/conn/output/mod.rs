//! Queued connection output and its wire prefixes.

use core::{mem, ops};

use dope_quic::{
    conn::stream::resident::Tag,
    wire::{Mark, stream::Id},
};
use o3::buffer::{Residency, hold, resident, segments, storage, storage::inline, write::Reserved};
use sark_core::manifold::stream::Boundary;
use sark_http::{self, payload::owned};

use crate::{conn, frame, frame::Writer, qpack::encoder, stream};

mod error;
mod sealed;

pub use error::Error;
pub(crate) use sealed::Sealed;

pub enum WritePayload<'d> {
    Owned(Vec<u8>),
    Shared(storage::Shared),
    Retained(resident::Shared<'d>),
    Segmented(segments::Many<resident::Shared<'d>>),
}

impl WritePayload<'_> {
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(bytes) => bytes.len(),
            Self::Shared(bytes) => bytes.len(),
            Self::Retained(bytes) => bytes.len(),
            Self::Segmented(body) => body.as_chain().len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn resident_len(&self) -> usize {
        match self {
            Self::Owned(bytes) => bytes.capacity(),
            Self::Shared(bytes) => bytes.resident(),
            Self::Retained(bytes) => bytes.resident(),
            Self::Segmented(body) => body.as_chain().resident(),
        }
    }
}

/// Immutable owners whose measured charges survive conversion into queued output.
pub trait Payload<'d>: Sealed + Sized {
    fn measure(&self) -> WriteCharge;
    fn retention(&self) -> hold::Charge;
    fn into_payload(self) -> WritePayload<'d>;
}

impl<'d> Payload<'d> for WritePayload<'d> {
    fn measure(&self) -> WriteCharge {
        WriteCharge::new(self.len(), self.resident_len())
    }

    fn retention(&self) -> hold::Charge {
        match self {
            Self::Owned(bytes) => hold::Charge::from_owner(bytes.len(), bytes.capacity()),
            Self::Shared(bytes) => hold::Charge::from_owner(bytes.len(), bytes.resident()),
            Self::Retained(bytes) => hold::Charge::from_owner(bytes.len(), bytes.resident()),
            Self::Segmented(body) => hold::Charge::from(body.as_chain()),
        }
    }

    fn into_payload(self) -> WritePayload<'d> {
        self
    }
}

impl<'d> Payload<'d> for Vec<u8> {
    fn measure(&self) -> WriteCharge {
        WriteCharge::new(self.len(), self.capacity())
    }

    fn retention(&self) -> hold::Charge {
        hold::Charge::from_owner(self.len(), self.capacity())
    }

    fn into_payload(self) -> WritePayload<'d> {
        WritePayload::Owned(self)
    }
}

impl<'d> Payload<'d> for owned::Segments<'d> {
    fn measure(&self) -> WriteCharge {
        match &**self {
            segments::Shape::Empty => WriteCharge::default(),
            segments::Shape::One(owner) => {
                let owner = owner.as_ref();
                WriteCharge::new(owner.len(), owner.resident())
            }
            segments::Shape::Many(owners) => {
                let chain = owners.as_chain();
                WriteCharge::new(chain.len(), chain.resident())
            }
        }
    }

    fn retention(&self) -> hold::Charge {
        self.charge()
    }

    fn into_payload(self) -> WritePayload<'d> {
        WritePayload::from(self)
    }
}

impl<'d> From<owned::Segments<'d>> for WritePayload<'d> {
    fn from(body: owned::Segments<'d>) -> Self {
        match body.into_shape() {
            segments::Shape::Empty => Self::Retained(resident::Shared::new()),
            segments::Shape::One(owner) => Self::Retained(owner.into_inner()),
            segments::Shape::Many(chain) => Self::Segmented(chain),
        }
    }
}

impl<'d> Payload<'d> for segments::Nonempty<resident::Shared<'d>> {
    fn measure(&self) -> WriteCharge {
        match self {
            Self::One(owner) => {
                let owner = owner.as_ref();
                WriteCharge::new(owner.len(), owner.resident())
            }
            Self::Many(owners) => {
                let chain = owners.as_chain();
                WriteCharge::new(chain.len(), chain.resident())
            }
        }
    }

    fn retention(&self) -> hold::Charge {
        match self {
            Self::One(owner) => {
                let owner = owner.as_ref();
                hold::Charge::from_owner(owner.len(), owner.resident())
            }
            Self::Many(owners) => hold::Charge::from(owners.as_chain()),
        }
    }

    fn into_payload(self) -> WritePayload<'d> {
        match self {
            Self::One(owner) => WritePayload::Retained(owner.into_inner()),
            Self::Many(owners) => WritePayload::Segmented(owners),
        }
    }
}

#[derive(PartialEq, Eq)]
pub enum WritePrefix {
    Inline(inline::Bytes),
    Owned(Vec<u8>),
}

impl From<&encoder::Capacity<&mut encoder::State>> for WritePrefix {
    fn from(capacity: &encoder::Capacity<&mut encoder::State>) -> Self {
        Self::Inline(capacity.pack())
    }
}

impl WritePrefix {
    pub(in crate::conn) fn try_encode(wire: &impl frame::Wire) -> Result<Self, conn::Error> {
        Self::try_write(wire.encoded_len(), |writer| {
            if let Err(error) = wire.encode(writer) {
                match error {}
            }
        })
    }

    pub(in crate::conn) fn try_write(
        expected: usize,
        encode: impl FnOnce(&mut Writer<'_>),
    ) -> Result<Self, conn::Error> {
        let mut prefix = if expected <= conn::INLINE_PREFIX_CAPACITY {
            Self::Inline(inline::Bytes::new())
        } else {
            Self::Owned(Vec::new())
        };
        let output = match &mut prefix {
            Self::Inline(bytes) => frame::Output::Inline(bytes.fit(expected)),
            Self::Owned(bytes) => {
                frame::Output::Owned(Reserved::try_exact(bytes, expected).map_err(|error| {
                    conn::Error::Prefix {
                        required: expected,
                        error,
                    }
                })?)
            }
        };
        output.try_write(encode).map_err(|error| match error {
            frame::WriteError::Write {
                expected,
                written,
                additional: added,
            } => conn::Error::PrefixWrite {
                expected,
                written,
                additional: added,
            },
            frame::WriteError::Length { expected, actual } => {
                conn::Error::PrefixLength { expected, actual }
            }
        })?;
        Ok(prefix)
    }

    fn resident_len(&self) -> usize {
        match self {
            Self::Inline(_) => 0,
            Self::Owned(bytes) => bytes.capacity(),
        }
    }

    pub(crate) fn charge(&self) -> WriteCharge {
        WriteCharge::new(self.len(), self.resident_len())
    }

    pub(in crate::conn) fn retention(&self) -> hold::Charge {
        match self {
            Self::Inline(_) => hold::Charge::default(),
            Self::Owned(bytes) => hold::Charge::from_owner(bytes.len(), bytes.capacity()),
        }
    }
}

impl ops::Deref for WritePrefix {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Inline(prefix) => prefix.as_ref(),
            Self::Owned(bytes) => bytes,
        }
    }
}

impl From<Vec<u8>> for WritePrefix {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Owned(bytes)
    }
}

pub struct Write<'d> {
    stream_id: Id,
    pub prefix: WritePrefix,
    pub payload: Option<WritePayload<'d>>,
    state: Tag,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteCharge {
    pub(crate) wire_bytes: usize,
    pub(crate) resident_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Overflow {
    Wire { left: usize, right: usize },
    Resident { left: usize, right: usize },
}

impl WriteCharge {
    pub(crate) const fn new(wire: usize, stored: usize) -> Self {
        Self {
            wire_bytes: wire,
            resident_bytes: stored,
        }
    }

    pub(crate) fn try_add(self, right: Self) -> Result<Self, Overflow> {
        let wire = self
            .wire_bytes
            .checked_add(right.wire_bytes)
            .ok_or(Overflow::Wire {
                left: self.wire_bytes,
                right: right.wire_bytes,
            })?;
        let resident = self
            .resident_bytes
            .checked_add(right.resident_bytes)
            .ok_or(Overflow::Resident {
                left: self.resident_bytes,
                right: right.resident_bytes,
            })?;
        Ok(Self::new(wire, resident))
    }
}

impl<'d> Write<'d> {
    pub(in crate::conn) fn single(
        target: stream::Target,
        prefix: impl Into<WritePrefix>,
        boundary: Boundary,
    ) -> Self {
        Self {
            stream_id: target.stream_id(),
            prefix: prefix.into(),
            payload: None,
            state: Self::state(target, boundary),
        }
    }

    pub(in crate::conn) fn segmented(
        target: stream::Target,
        prefix: WritePrefix,
        body: WritePayload<'d>,
        boundary: Boundary,
    ) -> Self {
        Self {
            stream_id: target.stream_id(),
            prefix,
            payload: Some(body),
            state: Self::state(target, boundary),
        }
    }

    pub(in crate::conn) fn finish(mut self) -> Self {
        self.state = Tag::new(self.state.key(), Mark::Set);
        self
    }

    const fn state(target: stream::Target, boundary: Boundary) -> Tag {
        let mark = match boundary {
            Boundary::More => Mark::Clear,
            Boundary::End => Mark::Set,
        };
        Tag::new(target.key(), mark)
    }

    pub(crate) fn target(&self) -> stream::Target {
        match self.state.key() {
            Some(key) => stream::Target::resident(self.stream_id, key),
            None => stream::Target::new(self.stream_id),
        }
    }

    pub const fn stream_id(&self) -> Id {
        self.stream_id
    }

    pub const fn boundary(&self) -> Boundary {
        match self.state.mark() {
            Mark::Clear => Boundary::More,
            Mark::Set => Boundary::End,
        }
    }
}

const _: () = assert!(mem::size_of::<Write<'_>>() == 10 * mem::size_of::<u64>());
