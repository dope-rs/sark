//! Frame header layout and header-bearing payload fields.

use o3::{
    buffer,
    buffer::{segments::queue, write},
};
use sark_core::manifold::stream::Boundary;

use crate::{
    frame::{self, HEADER_LEN, numbers},
    stream,
};

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Header {
    pub length: numbers::Length,
    pub kind: numbers::Type,
    pub flags: numbers::Flags,
    pub stream_id: stream::Id,
}

/// The decoded length and unsupported type byte of a complete frame header.
pub(crate) struct Unknown {
    pub(crate) length: numbers::Length,
    pub(crate) actual: u8,
}

impl Header {
    pub fn parse(buf: &[u8]) -> Result<Self, numbers::ParseError> {
        let Some(buf) = buf.first_chunk() else {
            return Err(numbers::ParseError::Need {
                actual: buf.len(),
                required: HEADER_LEN,
            });
        };
        Self::read(buf).map_err(|error| numbers::ParseError::BadType(error.actual))
    }

    pub(crate) fn assemble<C: buffer::raw::Segment>(
        mut source: queue::Array<'_, C, HEADER_LEN>,
    ) -> Result<Self, Unknown> {
        if let Some(bytes) = source.contiguous() {
            return Self::read(bytes);
        }
        let mut bytes = [0; HEADER_LEN];
        source.copy(&mut bytes);
        Self::read(&bytes)
    }

    /// Decodes a header whose complete wire length is already proved.
    pub(crate) fn read(buf: &[u8; HEADER_LEN]) -> Result<Self, Unknown> {
        let length = numbers::Length::from_wire(buf[0], buf[1], buf[2]);
        let kind = numbers::Type::from_u8(buf[3]).map_err(|actual| Unknown { length, actual })?;
        let flags = numbers::Flags::new(buf[4]);
        let stream = stream::Id::from_wire(u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]));
        Ok(Self {
            length,
            kind,
            flags,
            stream_id: stream,
        })
    }

    pub fn wire_bytes(&self) -> [u8; HEADER_LEN] {
        let len = self.length.wire_bytes();
        let stream = self.stream_id.wire_bytes();
        [
            len[0],
            len[1],
            len[2],
            self.kind as u8,
            self.flags.0,
            stream[0],
            stream[1],
            stream[2],
            stream[3],
        ]
    }

    pub(crate) fn try_stream(&self) -> Result<stream::Nonzero, numbers::ParseError> {
        self.stream_id.nonzero().ok_or(numbers::ParseError::Stream {
            kind: self.kind,
            actual: self.stream_id,
        })
    }

    pub(crate) fn check_conn(&self) -> Result<(), numbers::ParseError> {
        if self.stream_id.is_zero() {
            Ok(())
        } else {
            Err(numbers::ParseError::Connection {
                kind: self.kind,
                actual: self.stream_id,
            })
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum Sharing {
    Shared,
    Exclusive,
}

const _: () = {
    assert!(core::mem::size_of::<Sharing>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Sharing>() == core::mem::align_of::<bool>());
};

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Priority {
    pub sharing: Sharing,
    pub dependency: stream::Id,
    pub weight: u8,
}

impl Priority {
    pub(crate) const fn check(self, stream: stream::Nonzero) -> Result<Self, numbers::ParseError> {
        if self.dependency.into_u32() == stream.get().into_u32() {
            Err(numbers::ParseError::Dependency {
                stream: stream.get(),
                dependency: self.dependency,
            })
        } else {
            Ok(self)
        }
    }

    pub fn parse(buf: &[u8]) -> Result<Self, numbers::ParseError> {
        let Some(buf) = buf.first_chunk() else {
            return Err(numbers::ParseError::FrameMinimum {
                kind: numbers::Type::Priority,
                actual: buf.len(),
                minimum: 5,
            });
        };
        Ok(Self::read(buf))
    }

    pub(crate) fn read(buf: &[u8; 5]) -> Self {
        let raw = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        Self {
            sharing: match raw & 0x8000_0000 {
                0 => Sharing::Shared,
                _ => Sharing::Exclusive,
            },
            dependency: stream::Id::from_wire(raw),
            weight: buf[4],
        }
    }

    pub(crate) fn wire_bytes(self) -> [u8; 5] {
        let mut raw = self.dependency.into_u32();
        match self.sharing {
            Sharing::Shared => {}
            Sharing::Exclusive => raw |= 0x8000_0000,
        }
        let bytes = raw.to_be_bytes();
        [bytes[0], bytes[1], bytes[2], bytes[3], self.weight]
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) struct Parts<'a> {
    pub(crate) first: &'a [u8],
    pub(crate) second: &'a [u8],
}

impl<'a> Parts<'a> {
    pub(crate) const fn new(first: &'a [u8], second: &'a [u8]) -> Self {
        Self { first, second }
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Data<'a> {
    stream_id: stream::Nonzero,
    boundary: Boundary,
    payload: &'a [u8],
    length: numbers::Length,
}

impl<'a> Data<'a> {
    pub fn new(stream: stream::Id, boundary: Boundary, payload: &'a [u8]) -> Option<Self> {
        Self::from(stream.nonzero()?, boundary, payload)
    }

    fn from(id: stream::Nonzero, boundary: Boundary, payload: &'a [u8]) -> Option<Self> {
        Some(Self {
            stream_id: id,
            boundary,
            payload,
            length: numbers::Length::from_usize(payload.len())?,
        })
    }

    pub const fn stream_id(&self) -> stream::Id {
        self.stream_id.get()
    }

    pub const fn boundary(&self) -> Boundary {
        self.boundary
    }

    pub const fn payload(&self) -> &'a [u8] {
        self.payload
    }

    pub fn parse(header: Header, payload: &'a [u8]) -> Result<Self, numbers::ParseError> {
        let id = header.try_stream()?;
        let body = header.flags.strip(payload)?;
        let boundary = if header.flags.contains(numbers::Flags::END_STREAM) {
            Boundary::End
        } else {
            Boundary::More
        };
        Self::from(id, boundary, body).ok_or(numbers::ParseError::FrameLimit {
            kind: numbers::Type::Data,
            actual: body.len(),
            limit: numbers::Length::MAX as usize,
        })
    }

    pub fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        let header = Self::wire_header(self.stream_id.get(), self.boundary, self.length);
        out.write_slices([&header, self.payload])
    }

    pub(crate) fn wire_header(
        stream: stream::Id,
        boundary: Boundary,
        length: numbers::Length,
    ) -> [u8; HEADER_LEN] {
        let flags = match boundary {
            Boundary::More => 0,
            Boundary::End => numbers::Flags::END_STREAM,
        };
        Header {
            length,
            kind: numbers::Type::Data,
            flags: numbers::Flags::new(flags),
            stream_id: stream,
        }
        .wire_bytes()
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Headers<'a> {
    stream_id: stream::Nonzero,
    boundary: Boundary,
    chain: frame::Block,
    priority: Option<Priority>,
    block: &'a [u8],
    length: numbers::Length,
}

impl<'a> Headers<'a> {
    pub fn new(
        stream: stream::Id,
        boundary: Boundary,
        chain: frame::Block,
        priority: Option<Priority>,
        block: &'a [u8],
    ) -> Result<Self, numbers::ParseError> {
        let id = stream.nonzero().ok_or(numbers::ParseError::Stream {
            kind: numbers::Type::Headers,
            actual: stream,
        })?;
        let priority = priority.map(|priority| priority.check(id)).transpose()?;
        Self::from(id, boundary, chain, priority, block)
    }

    fn from(
        id: stream::Nonzero,
        boundary: Boundary,
        chain: frame::Block,
        priority: Option<Priority>,
        block: &'a [u8],
    ) -> Result<Self, numbers::ParseError> {
        let overhead: usize = if priority.is_some() { 5 } else { 0 };
        let actual = overhead.saturating_add(block.len());
        let length =
            numbers::Length::from_usize(actual).ok_or(numbers::ParseError::FrameLimit {
                kind: numbers::Type::Headers,
                actual,
                limit: numbers::Length::MAX as usize,
            })?;
        Ok(Self {
            stream_id: id,
            boundary,
            chain,
            priority,
            block,
            length,
        })
    }

    pub const fn stream_id(&self) -> stream::Id {
        self.stream_id.get()
    }

    pub const fn priority(&self) -> Option<Priority> {
        self.priority
    }

    pub const fn block(&self) -> &'a [u8] {
        self.block
    }

    pub fn parse(header: Header, payload: &'a [u8]) -> Result<Self, numbers::ParseError> {
        let id = header.try_stream()?;
        let unpadded = header.flags.strip(payload)?;
        let (priority, rest) = if header.flags.contains(numbers::Flags::PRIORITY) {
            let Some((bytes, rest)) = unpadded.split_first_chunk() else {
                return Err(numbers::ParseError::FrameMinimum {
                    kind: numbers::Type::Headers,
                    actual: unpadded.len(),
                    minimum: 5,
                });
            };
            let pri = Priority::read(bytes).check(id)?;
            (Some(pri), rest)
        } else {
            (None, unpadded)
        };
        let boundary = if header.flags.contains(numbers::Flags::END_STREAM) {
            Boundary::End
        } else {
            Boundary::More
        };
        let chain = if header.flags.contains(numbers::Flags::END_HEADERS) {
            frame::Block::End
        } else {
            frame::Block::More
        };
        Self::from(id, boundary, chain, priority, rest)
    }

    pub fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        let mut flags = match self.boundary {
            Boundary::More => 0,
            Boundary::End => numbers::Flags::END_STREAM,
        };
        flags |= match self.chain {
            frame::Block::More => 0,
            frame::Block::End => numbers::Flags::END_HEADERS,
        };
        if self.priority.is_some() {
            flags |= numbers::Flags::PRIORITY;
        }
        let header = Header {
            length: self.length,
            kind: numbers::Type::Headers,
            flags: numbers::Flags::new(flags),
            stream_id: self.stream_id.get(),
        }
        .wire_bytes();
        let priority = self.priority.map(Priority::wire_bytes);
        let priority = match priority.as_ref() {
            Some(fields) => fields.as_slice(),
            None => &[],
        };
        out.write_slices([&header, priority, self.block])
    }
}
