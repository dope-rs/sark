use std::num::NonZero;

use o3::depot::capacity;

use crate::{
    message,
    server::config::error::{Error, Field},
};

const DEFAULT_WIRE_BYTES: usize = 64 << 20;

/// Valid pending-output limits for one gRPC connection.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Output {
    pub(in crate::server) messages: NonZero<u32>,
    wire_bytes: usize,
}

impl Output {
    pub(super) const DEFAULT: Self = Self {
        messages: NonZero::<u32>::MIN.saturating_add(8191),
        wire_bytes: DEFAULT_WIRE_BYTES,
    };

    pub fn try_new(messages: usize, bytes: usize) -> Result<Self, Error> {
        let messages = NonZero::new(messages).ok_or(Error::ZeroCapacity(Field::OutputMessages))?;
        if bytes < message::HEADER_LEN {
            return Err(Error::MinimumCapacity {
                field: Field::OutputWireBytes,
                actual: bytes,
                minimum: message::HEADER_LEN,
            });
        }
        let messages =
            capacity::Count::try_nonzero(messages).map_err(|error| Error::MaximumCapacity {
                field: Field::OutputMessages,
                maximum: u32::MAX as usize,
                error,
            })?;
        Ok(Self {
            messages,
            wire_bytes: bytes,
        })
    }

    pub const fn messages(&self) -> usize {
        self.messages.get() as usize
    }

    pub const fn wire_bytes(&self) -> usize {
        self.wire_bytes
    }
}

impl Default for Output {
    fn default() -> Self {
        Self::DEFAULT
    }
}

const _: () = assert!(!core::mem::needs_drop::<Output>());
