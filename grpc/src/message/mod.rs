use std::mem;

use o3::{
    buffer,
    buffer::{pool, pool::owners, resident, segments, storage},
    mem::rc,
};
use prost::bytes::Buf;
use sark_core::{life::session, manifold::stream};

use crate::Reason;

mod deframer;
pub mod input;
mod view;

pub use deframer::Deframer;
pub use view::View;

/// Number of wire bytes preceding every gRPC message payload.
pub const HEADER_LEN: usize = 5;

#[repr(transparent)]
pub struct Payload<C: buffer::raw::Segment = storage::Shared> {
    segments: segments::Chain<C>,
}

impl<C: buffer::raw::Segment> Payload<C> {
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

    pub fn try_flatten(&self) -> Result<storage::Shared, storage::AllocError> {
        self.segments.try_flatten()
    }
}

impl<C: buffer::raw::Segment> From<segments::Chain<C>> for Payload<C> {
    fn from(segments: segments::Chain<C>) -> Self {
        Self { segments }
    }
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub struct Plain;

#[derive(Clone, Copy, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum Compression {
    #[default]
    Plain,
    Compressed,
}

const _: () = assert!(mem::size_of::<Compression>() == mem::size_of::<bool>());
const _: () = assert!(mem::align_of::<Compression>() == mem::align_of::<bool>());

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Wire {
    compression: Compression,
}

pub struct Frame<C: buffer::raw::Segment = storage::Shared, S = Plain> {
    pub payload: Payload<C>,
    state: S,
}

impl<C: buffer::raw::Segment> PartialEq for Frame<C, Plain> {
    fn eq(&self, other: &Self) -> bool {
        self.cursor().is_equal(&mut other.cursor())
    }
}

impl<C: buffer::raw::Segment> Eq for Frame<C, Plain> {}

impl<C: buffer::raw::Segment> PartialEq for Frame<C, Wire> {
    fn eq(&self, other: &Self) -> bool {
        self.state == other.state
            && Cursor::new(&self.payload).is_equal(&mut Cursor::new(&other.payload))
    }
}

impl<C: buffer::raw::Segment> Eq for Frame<C, Wire> {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    BadCompressionFlag(u8),
    MessageTooLarge { len: usize, max: usize },
    Incomplete,
    LengthOverflow { len: usize },
    BodyLength { expected: usize, actual: usize },
    Input(input::Error),
    Transfer(segments::Error),
    Retain(rc::Error),
}

impl Error {
    pub(crate) fn code(&self) -> crate::Code {
        match self {
            Self::BadCompressionFlag(_) | Self::Incomplete => crate::Code::Internal,
            Self::MessageTooLarge { .. } | Self::Retain(_) => crate::Code::ResourceExhausted,
            Self::Transfer(error) => match error {
                segments::Error::SegmentCapacity(_)
                | segments::Error::ChainCapacity(_)
                | segments::Error::Retain(_) => crate::Code::ResourceExhausted,
                segments::Error::DifferentPool
                | segments::Error::Count(_)
                | segments::Error::Logical(_)
                | segments::Error::Resident(_)
                | segments::Error::Boundary(_) => crate::Code::Internal,
            },
            Self::LengthOverflow { .. } | Self::BodyLength { .. } | Self::Input(_) => {
                crate::Code::Internal
            }
        }
    }
}

impl<C: buffer::raw::Segment> Frame<C, Wire> {
    pub fn into_plain(self) -> Result<Frame<C>, crate::Status> {
        match self.state.compression {
            Compression::Plain => Ok(Frame {
                payload: self.payload,
                state: Plain,
            }),
            Compression::Compressed => Err(crate::Status::new(
                crate::Code::Internal,
                Reason::Compression,
            )),
        }
    }
}

impl<C: buffer::raw::Segment> Frame<C> {
    pub fn from_payload(payload: Payload<C>) -> Self {
        Self {
            payload,
            state: Plain,
        }
    }

    pub fn cursor(&self) -> Cursor<'_, C> {
        Cursor::new(&self.payload)
    }

    pub fn view(&self) -> View<'_, C> {
        View::new(&self.payload)
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
}

impl Frame<storage::Shared, Plain> {
    pub fn try_header(mode: Compression, len: usize) -> Result<[u8; HEADER_LEN], Error> {
        if len > u32::MAX as usize {
            return Err(Error::LengthOverflow { len });
        }
        Ok(Self::header(mode, len as u32))
    }

    pub(crate) fn header(mode: Compression, len: u32) -> [u8; HEADER_LEN] {
        let bytes = len.to_be_bytes();
        let flag = match mode {
            Compression::Plain => 0,
            Compression::Compressed => 1,
        };
        [flag, bytes[0], bytes[1], bytes[2], bytes[3]]
    }
}

pub struct Cursor<'a, C: buffer::raw::Segment = storage::Shared> {
    segments: segments::Iter<'a, C>,
    current: &'a [u8],
    remaining: usize,
}

impl<'a, C: buffer::raw::Segment> Cursor<'a, C> {
    pub fn new(payload: &'a Payload<C>) -> Self {
        let mut segments = payload.segments();
        let current = segments.next().map_or(&[][..], AsRef::as_ref);
        Self {
            segments,
            current,
            remaining: payload.len(),
        }
    }

    fn next_segment(&mut self) {
        self.current = self.segments.next().map_or(&[][..], AsRef::as_ref);
    }

    fn is_equal(&mut self, other: &mut Self) -> bool {
        if self.remaining() != other.remaining() {
            return false;
        }
        while self.remaining() != 0 {
            let left = self.chunk();
            let right = other.chunk();
            let len = left.len().min(right.len());
            if left[..len] != right[..len] {
                return false;
            }
            self.advance(len);
            other.advance(len);
        }
        true
    }
}

impl<C: buffer::raw::Segment> Buf for Cursor<'_, C> {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        self.current
    }

    fn advance(&mut self, mut cnt: usize) {
        cnt = cnt.min(self.remaining);
        self.remaining -= cnt;
        while cnt != 0 {
            let span = self.current.len();
            if cnt < span {
                self.current = &self.current[cnt..];
                return;
            }
            cnt -= span;
            self.next_segment();
        }
    }
}

pub(crate) struct DataChunk<
    C: buffer::raw::Segment,
    P = pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
> {
    data: Payload<C>,
    permit: Option<P>,
}

impl<C: buffer::raw::Segment, P> DataChunk<C, P> {
    pub(crate) fn new<D>(data: D) -> Self
    where
        C: session::Storage,
        D: stream::Payload<C, Permit = P>,
    {
        let (segments, permit) = data.into_parts();
        let mut chunk = Self {
            data: Payload::from(segments),
            permit: Some(permit),
        };
        chunk.release();
        chunk
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn release(&mut self) {
        if !self.data.is_empty() {
            return;
        }
        self.permit = None;
    }
}

const _: () = assert!(mem::size_of::<Error>() == mem::size_of::<segments::Error>());
const _: () = {
    assert!(
        mem::size_of::<Option<storage::Shared>>() == mem::size_of::<storage::Shared>()
            && mem::size_of::<Option<resident::Shared<'static>>>()
                == mem::size_of::<resident::Shared<'static>>()
            && mem::size_of::<Payload<storage::Shared>>()
                == mem::size_of::<segments::Chain<storage::Shared>>()
            && mem::align_of::<Payload<storage::Shared>>()
                == mem::align_of::<segments::Chain<storage::Shared>>()
    );
};
const _: () = assert!(
    mem::size_of::<Frame<storage::Shared>>() == mem::size_of::<Payload<storage::Shared>>()
        && mem::align_of::<Frame<storage::Shared>>() == mem::align_of::<Payload<storage::Shared>>()
        && mem::size_of::<Frame<resident::Shared<'static>>>()
            == mem::size_of::<Frame<storage::Shared>>()
        && mem::size_of::<Frame<storage::Shared, Wire>>()
            <= mem::size_of::<Payload<storage::Shared>>()
                + mem::align_of::<Payload<storage::Shared>>()
);
const _: () = assert!(
    mem::size_of::<
        DataChunk<
            storage::Shared,
            pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
        >,
    >() == mem::size_of::<(
        Payload<storage::Shared>,
        pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>
    )>() && mem::align_of::<
        DataChunk<
            storage::Shared,
            pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
        >,
    >() == mem::align_of::<(
        Payload<storage::Shared>,
        pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>
    )>()
);
const _: () = assert!(
    mem::size_of::<
        DataChunk<
            resident::Shared<'static>,
            pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
        >,
    >() == mem::size_of::<
        DataChunk<
            storage::Shared,
            pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
        >,
    >() && mem::align_of::<
        DataChunk<
            resident::Shared<'static>,
            pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
        >,
    >() == mem::align_of::<
        DataChunk<
            storage::Shared,
            pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
        >,
    >()
);
