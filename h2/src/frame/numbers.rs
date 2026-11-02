//! Bounded wire numbers and frame discriminants.

use o3::base::num::bounded;
use sark_core::manifold::client::event::Reset;

use crate::{frame, stream};

#[repr(transparent)]
#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Length(u32);

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Limit(bounded::NonZeroU32<16_384, 0x00ff_ffff>);

impl Length {
    pub const MAX: u32 = 0x00ff_ffff;
    pub const ZERO: Self = Self(0);
    pub(crate) const FOUR: Self = Self(4);
    pub(crate) const FIVE: Self = Self(5);
    pub(crate) const EIGHT: Self = Self(8);

    pub const fn new(value: u32) -> Option<Self> {
        match bounded::U32::<0, 0x00ff_ffff>::new(value) {
            Some(value) => Some(Self(value.get())),
            None => None,
        }
    }

    pub const fn from_usize(value: usize) -> Option<Self> {
        match bounded::U32::<0, 0x00ff_ffff>::from_usize(value) {
            Some(value) => Some(Self(value.get())),
            None => None,
        }
    }

    pub const fn into_u32(self) -> u32 {
        self.0
    }

    pub const fn into_usize(self) -> usize {
        self.0 as usize
    }

    pub(crate) const fn from_wire(high: u8, middle: u8, low: u8) -> Self {
        Self(u32::from_be_bytes([0, high, middle, low]))
    }

    pub(crate) const fn wire_bytes(self) -> [u8; 3] {
        let bytes = self.0.to_be_bytes();
        [bytes[1], bytes[2], bytes[3]]
    }
}

impl Limit {
    pub const DEFAULT: Self = Self(bounded::NonZeroU32::exact::<16_384>());
    pub const MAX: Self = Self(bounded::NonZeroU32::exact::<0x00ff_ffff>());

    pub const fn new(value: u32) -> Option<Self> {
        match bounded::NonZeroU32::<16_384, 0x00ff_ffff>::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn into_u32(self) -> u32 {
        self.0.get()
    }

    pub const fn into_usize(self) -> usize {
        self.0.into_usize()
    }

    pub(crate) fn take(self, left: usize) -> Length {
        Length(left.min(self.into_usize()) as u32)
    }
}

impl Default for Limit {
    fn default() -> Self {
        Self::DEFAULT
    }
}

const _: () = assert!(core::mem::size_of::<Limit>() == core::mem::size_of::<u32>());

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Increment(bounded::NonZeroU32<1, 0x7fff_ffff>);

impl Increment {
    pub const MAX: u32 = 0x7fff_ffff;

    pub const fn new(value: u32) -> Option<Self> {
        match bounded::NonZeroU32::<1, 0x7fff_ffff>::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }

    pub(crate) const fn into_i32(self) -> i32 {
        self.get() as i32
    }

    pub(crate) const fn from_wire(value: u32) -> Option<Self> {
        Self::new(value & Self::MAX)
    }

    pub(crate) const fn wire_bytes(self) -> [u8; 4] {
        self.get().to_be_bytes()
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Type {
    Data = 0x0,
    Headers = 0x1,
    Priority = 0x2,
    RstStream = 0x3,
    Settings = 0x4,
    PushPromise = 0x5,
    Ping = 0x6,
    GoAway = 0x7,
    WindowUpdate = 0x8,
    Continuation = 0x9,
}

impl Type {
    pub fn from_u8(byte: u8) -> Result<Self, u8> {
        match byte {
            0x0 => Ok(Self::Data),
            0x1 => Ok(Self::Headers),
            0x2 => Ok(Self::Priority),
            0x3 => Ok(Self::RstStream),
            0x4 => Ok(Self::Settings),
            0x5 => Ok(Self::PushPromise),
            0x6 => Ok(Self::Ping),
            0x7 => Ok(Self::GoAway),
            0x8 => Ok(Self::WindowUpdate),
            0x9 => Ok(Self::Continuation),
            other => Err(other),
        }
    }
}

#[derive(Copy, Clone, Default, PartialEq, Eq)]
pub struct Flags(pub u8);

impl Flags {
    pub(crate) const fn new(bits: u8) -> Self {
        Self(bits)
    }

    pub const END_STREAM: u8 = 0x1;
    pub const ACK: u8 = 0x1;
    pub const END_HEADERS: u8 = 0x4;
    pub const PADDED: u8 = 0x8;
    pub const PRIORITY: u8 = 0x20;

    pub fn contains(self, bit: u8) -> bool {
        (self.0 & bit) != 0
    }

    pub fn strip(self, payload: &[u8]) -> Result<&[u8], ParseError> {
        if !self.contains(Self::PADDED) {
            return Ok(payload);
        }
        if payload.is_empty() {
            return Err(ParseError::Padding {
                actual: 0,
                required: 1,
            });
        }
        let pad_len = payload[0] as usize;
        let required = 1 + pad_len;
        if required > payload.len() {
            return Err(ParseError::Padding {
                actual: payload.len(),
                required,
            });
        }
        Ok(&payload[1..payload.len() - pad_len])
    }
}

#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    NoError = 0x0,
    ProtocolError = 0x1,
    InternalError = 0x2,
    FlowControl = 0x3,
    SettingsTimeout = 0x4,
    StreamClosed = 0x5,
    FrameSize = 0x6,
    RefusedStream = 0x7,
    Cancel = 0x8,
    Compression = 0x9,
    Connect = 0xa,
    EnhanceYourCalm = 0xb,
    InadequateSecurity = 0xc,
    Http11Required = 0xd,
}

impl ErrorCode {
    pub(crate) fn reason(self) -> Reset {
        match self {
            Self::Cancel => Reset::Cancelled,
            Self::RefusedStream => Reset::Refused,
            Self::InternalError => Reset::Internal,
            Self::EnhanceYourCalm => Reset::ResourceExhausted,
            Self::InadequateSecurity => Reset::InadequateSecurity,
            _ => Reset::Other,
        }
    }

    pub(crate) fn from_reason(reason: Reset) -> Self {
        match reason {
            Reset::Cancelled => Self::Cancel,
            Reset::Refused => Self::RefusedStream,
            Reset::Internal | Reset::Other => Self::InternalError,
            Reset::ResourceExhausted => Self::EnhanceYourCalm,
            Reset::InadequateSecurity => Self::InadequateSecurity,
        }
    }

    pub fn from_u32(value: u32) -> Self {
        match value {
            0x0 => Self::NoError,
            0x1 => Self::ProtocolError,
            0x2 => Self::InternalError,
            0x3 => Self::FlowControl,
            0x4 => Self::SettingsTimeout,
            0x5 => Self::StreamClosed,
            0x6 => Self::FrameSize,
            0x7 => Self::RefusedStream,
            0x8 => Self::Cancel,
            0x9 => Self::Compression,
            0xa => Self::Connect,
            0xb => Self::EnhanceYourCalm,
            0xc => Self::InadequateSecurity,
            0xd => Self::Http11Required,
            _ => Self::InternalError,
        }
    }
}

#[repr(u16)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SettingId {
    HeaderTableSize = 0x1,
    EnablePush = 0x2,
    MaxConcurrentStreams = 0x3,
    InitialWindowSize = 0x4,
    MaxFrameSize = 0x5,
    MaxHeaderListSize = 0x6,
    EnableConnectProtocol = 0x8,
}

impl SettingId {
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            0x1 => Some(Self::HeaderTableSize),
            0x2 => Some(Self::EnablePush),
            0x3 => Some(Self::MaxConcurrentStreams),
            0x4 => Some(Self::InitialWindowSize),
            0x5 => Some(Self::MaxFrameSize),
            0x6 => Some(Self::MaxHeaderListSize),
            0x8 => Some(Self::EnableConnectProtocol),
            _ => None,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ParseError {
    Need {
        actual: usize,
        required: usize,
    },
    BadType(u8),
    FrameSize {
        kind: Type,
        actual: usize,
        expected: usize,
    },
    FrameMinimum {
        kind: Type,
        actual: usize,
        minimum: usize,
    },
    FrameLimit {
        kind: Type,
        actual: usize,
        limit: usize,
    },
    SettingsSize {
        actual: usize,
        ack: frame::Ack,
    },
    Connection {
        kind: Type,
        actual: stream::Id,
    },
    Stream {
        kind: Type,
        actual: stream::Id,
    },
    Dependency {
        stream: stream::Id,
        dependency: stream::Id,
    },
    Promise {
        stream: stream::Id,
        promised: stream::Id,
    },
    Padding {
        actual: usize,
        required: usize,
    },
    ZeroIncrement,
}

impl From<sark_http::field::metadata::block::ContentError> for ErrorCode {
    fn from(error: sark_http::field::metadata::block::ContentError) -> Self {
        use sark_http::field::metadata::block::ContentError;
        match error {
            ContentError::ForbiddenLength { .. }
            | ContentError::NonemptyLength { .. }
            | ContentError::Exceeded { .. }
            | ContentError::Incomplete { .. }
            | ContentError::Trailers => Self::ProtocolError,
        }
    }
}

const _: () = assert!(core::mem::size_of::<ParseError>() <= 3 * core::mem::size_of::<usize>());
