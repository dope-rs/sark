use std::num::NonZero;

use o3::depot::capacity;

use crate::{
    message,
    server::config::error::{Error, Field, Operation},
};

const DEFAULT_STREAM_BYTES: usize = 16 << 20;
const DEFAULT_BYTES: usize = 64 << 20;

/// Valid retained-input limits for one gRPC connection.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Input {
    messages: capacity::Count,
    segments: NonZero<u32>,
    stream_bytes: usize,
    bytes: usize,
    stream_wire: usize,
    wire: usize,
}

impl Input {
    pub(super) const DEFAULT: Self = Self {
        messages: capacity::Count::new(8192),
        segments: NonZero::<u32>::MIN.saturating_add(16_383),
        stream_bytes: DEFAULT_STREAM_BYTES,
        bytes: DEFAULT_BYTES,
        stream_wire: DEFAULT_STREAM_BYTES + 8192 * message::HEADER_LEN,
        wire: DEFAULT_BYTES + 8192 * message::HEADER_LEN,
    };

    pub fn try_new(
        messages: usize,
        segments: usize,
        stream: usize,
        bytes: usize,
    ) -> Result<Self, Error> {
        let segments = NonZero::new(segments).ok_or(Error::ZeroCapacity(Field::InputSegments))?;
        let count =
            capacity::Count::try_from(messages).map_err(|error| Error::MaximumCapacity {
                field: Field::InputMessages,
                maximum: u32::MAX as usize,
                error,
            })?;
        let framing = count.get() * message::HEADER_LEN;
        let framed = stream.checked_add(framing).ok_or(Error::Overflow {
            field: Field::StreamWireBytes,
            operation: Operation::Add,
            left: stream,
            right: framing,
        })?;
        let wire = bytes.checked_add(framing).ok_or(Error::Overflow {
            field: Field::ConnectionWireBytes,
            operation: Operation::Add,
            left: bytes,
            right: framing,
        })?;
        let segments =
            capacity::Count::try_nonzero(segments).map_err(|error| Error::MaximumCapacity {
                field: Field::InputSegments,
                maximum: u32::MAX as usize,
                error,
            })?;
        Ok(Self {
            messages: count,
            segments,
            stream_bytes: stream,
            bytes,
            stream_wire: framed,
            wire,
        })
    }

    pub const fn messages(&self) -> usize {
        self.messages.get()
    }

    pub const fn segments(&self) -> usize {
        self.segments.get() as usize
    }

    pub const fn stream_bytes(&self) -> usize {
        self.stream_bytes
    }

    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    pub(in crate::server) const fn count(self) -> capacity::Count {
        self.messages
    }

    pub const fn stream_wire(self) -> usize {
        self.stream_wire
    }

    pub const fn wire(self) -> usize {
        self.wire
    }
}

impl Default for Input {
    fn default() -> Self {
        Self::DEFAULT
    }
}

const _: () = assert!(!core::mem::needs_drop::<Input>());
