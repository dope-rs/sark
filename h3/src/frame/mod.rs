//! HTTP/3 frame wire values and parsing.

use std::{marker, ops};

use dope_quic::wire::varint;
use o3::{
    alloc,
    buffer::write::{self, Sink},
};

use crate::sources;

mod control;
mod data;
mod decode;
mod settings;
mod stream;
mod wire;
mod writer;

pub(crate) use control::Control;
pub(crate) use data::Data;
pub(crate) use decode::{Decoder, Read};
pub use settings::Settings;
pub(crate) use stream::Stream;
pub(crate) use wire::Wire;
pub(crate) use writer::{Output, WriteError, Writer};

pub const TYPE_DATA: varint::VarInt = varint::VarInt::from_u8(0x00);
pub const TYPE_HEADERS: varint::VarInt = varint::VarInt::from_u8(0x01);
pub const TYPE_CANCEL_PUSH: varint::VarInt = varint::VarInt::from_u8(0x03);
pub const TYPE_SETTINGS: varint::VarInt = varint::VarInt::from_u8(0x04);
pub const TYPE_PUSH_PROMISE: varint::VarInt = varint::VarInt::from_u8(0x05);
pub const TYPE_GOAWAY: varint::VarInt = varint::VarInt::from_u8(0x07);
pub const TYPE_MAX_PUSH_ID: varint::VarInt = varint::VarInt::from_u8(0x0d);
pub const STREAM_TYPE_CONTROL: varint::VarInt = varint::VarInt::from_u8(0x00);
pub const STREAM_TYPE_PUSH: varint::VarInt = varint::VarInt::from_u8(0x01);
pub const STREAM_TYPE_QPACK_ENCODER: varint::VarInt = varint::VarInt::from_u8(0x02);
pub const STREAM_TYPE_QPACK_DECODER: varint::VarInt = varint::VarInt::from_u8(0x03);
pub const SETTINGS_QPACK_MAX_TABLE_CAPACITY: varint::VarInt = varint::VarInt::from_u8(0x01);
pub const SETTINGS_MAX_FIELD_SECTION_SIZE: varint::VarInt = varint::VarInt::from_u8(0x06);
pub const SETTINGS_QPACK_BLOCKED_STREAMS: varint::VarInt = varint::VarInt::from_u8(0x07);
pub const SETTINGS_ENABLE_CONNECT_PROTOCOL: varint::VarInt = varint::VarInt::from_u8(0x08);

/// Closed HTTP/3 application error code with its canonical wire value.
#[repr(u16)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    NoError = 0x0100,
    GeneralProtocol = 0x0101,
    Internal = 0x0102,
    StreamCreation = 0x0103,
    ClosedCriticalStream = 0x0104,
    FrameUnexpected = 0x0105,
    Payload = 0x0106,
    ExcessiveLoad = 0x0107,
    Id = 0x0108,
    Settings = 0x0109,
    MissingSettings = 0x010a,
    RequestRejected = 0x010b,
    RequestCancelled = 0x010c,
    RequestIncomplete = 0x010d,
    Message = 0x010e,
    Connect = 0x010f,
    VersionFallback = 0x0110,
    QpackDecompressionFailed = 0x0200,
    QpackEncoderStream = 0x0201,
    QpackDecoderStream = 0x0202,
}
impl ErrorCode {
    pub const fn wire_code(self) -> u16 {
        self as u16
    }

    pub const fn wire_value(self) -> u64 {
        self.wire_code() as u64
    }
}
impl From<ErrorCode> for u64 {
    fn from(code: ErrorCode) -> Self {
        code.wire_value()
    }
}
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ParseError {
    NeedMore,
    VarInt(varint::Error),
    PayloadInteger(varint::Error),
    ReservedFrame { kind: varint::VarInt },
    BadVarInt,
    BadSettings,
    DuplicateSetting,
    FrameTooLarge,
}

#[derive(Debug)]
pub enum EncodeError {
    Frame(ParseError),
    Reserve {
        required: usize,
        error: alloc::Error,
    },
}

impl From<ParseError> for EncodeError {
    fn from(error: ParseError) -> Self {
        Self::Frame(error)
    }
}

impl From<varint::Error> for ParseError {
    fn from(error: varint::Error) -> Self {
        Self::VarInt(error)
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Header {
    pub kind: varint::VarInt,
    pub length: varint::VarInt,
    pub header_len: usize,
}

impl Header {
    pub(crate) fn for_payload(kind: varint::VarInt, len: usize) -> Result<Self, ParseError> {
        let length = varint::VarInt::from_usize(len).ok_or(ParseError::FrameTooLarge)?;
        Ok(Self {
            kind,
            length,
            header_len: usize::from(kind.encoded_len()) + usize::from(length.encoded_len()),
        })
    }

    pub fn parse(buf: &[u8]) -> Result<Self, ParseError> {
        let (kind, n) = varint::VarInt::decode(buf)?;
        let tail = buf.get(n..).ok_or(ParseError::NeedMore)?;
        let (length, m) = varint::VarInt::decode(tail)?;
        let head = n.checked_add(m).ok_or(ParseError::FrameTooLarge)?;
        Ok(Self {
            kind,
            length,
            header_len: head,
        })
    }

    pub fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        out.write_slices([self.kind.pack().as_ref(), self.length.pack().as_ref()])
    }
}

pub(crate) struct PayloadView<'a, I: sources::Chunks<'a>> {
    span: sources::Span<'a, I>,
}

impl<'a, I: sources::Chunks<'a>> PayloadView<'a, I> {
    fn new(span: sources::Span<'a, I>) -> Self {
        Self { span }
    }
}

#[derive(Clone)]
pub(crate) struct Chunks<'view, 'a: 'view, I: sources::Chunks<'a>> {
    chunks: sources::Bounded<'a, I>,
    view: marker::PhantomData<&'view [u8]>,
}

impl<'view, 'a: 'view, I: sources::Chunks<'a>> Iterator for Chunks<'view, 'a, I> {
    type Item = &'view [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.chunks.next()
    }
}

impl<'a, I: sources::Chunks<'a>> crate::Source for PayloadView<'a, I> {
    type Chunks<'view>
        = Chunks<'view, 'a, I>
    where
        Self: 'view;

    fn len(&self) -> usize {
        self.span.len()
    }

    fn chunks(&self) -> Self::Chunks<'_> {
        Chunks {
            chunks: self.span.chunks(),
            view: marker::PhantomData,
        }
    }

    fn form(&self) -> sources::Form<'_> {
        self.span.form()
    }
}

pub(crate) enum Decoded<'a, I: sources::Chunks<'a>> {
    Data(ops::Range<usize>),
    Headers(PayloadView<'a, I>),
    CancelPush {
        push_id: varint::VarInt,
    },
    Settings(Settings),
    PushPromise {
        push_id: varint::VarInt,
        block: PayloadView<'a, I>,
    },
    GoAway {
        id: varint::VarInt,
    },
    MaxPushId {
        push_id: varint::VarInt,
    },
    Unknown {
        kind: u64,
    },
}

impl<'a, I: sources::Chunks<'a>> Decoded<'a, I> {
    pub(crate) const fn kind(&self) -> u64 {
        match self {
            Self::Data(_) => TYPE_DATA.get(),
            Self::Headers(_) => TYPE_HEADERS.get(),
            Self::CancelPush { .. } => TYPE_CANCEL_PUSH.get(),
            Self::Settings(_) => TYPE_SETTINGS.get(),
            Self::PushPromise { .. } => TYPE_PUSH_PROMISE.get(),
            Self::GoAway { .. } => TYPE_GOAWAY.get(),
            Self::MaxPushId { .. } => TYPE_MAX_PUSH_ID.get(),
            Self::Unknown { kind } => *kind,
        }
    }
}

fn decode_varint_cursor<'a, I: sources::Chunks<'a>>(
    input: &mut sources::Cursor<'a, I>,
) -> Result<varint::VarInt, ParseError> {
    let first = input.peek().ok_or(varint::Error::Underflow {
        actual: 0,
        required: 1,
    })?;
    let width = 1usize << usize::from(first >> 6);
    if input.remaining() < width {
        return Err(varint::Error::Underflow {
            actual: input.remaining(),
            required: width,
        }
        .into());
    }
    let mut bytes = [0u8; 8];
    let encoded = bytes.get_mut(..width).ok_or(ParseError::BadVarInt)?;
    let copied = input.copy_into(encoded);
    if copied != width {
        return Err(varint::Error::Underflow {
            actual: copied,
            required: width,
        }
        .into());
    }
    let (value, _) = varint::VarInt::decode(encoded)?;
    Ok(value)
}

impl ParseError {
    /// The complete frame payload is already bounded; more stream bytes cannot
    /// complete an integer that crosses that boundary.
    fn payload(self) -> Self {
        match self {
            Self::VarInt(error) => Self::PayloadInteger(error),
            error => error,
        }
    }
}

fn decode_payload_varint<'a, I: sources::Chunks<'a>>(
    input: &mut sources::Cursor<'a, I>,
) -> Result<varint::VarInt, ParseError> {
    decode_varint_cursor(input).map_err(ParseError::payload)
}

fn check_kind(kind: varint::VarInt) -> Result<(), ParseError> {
    match kind.get() {
        0x02 | 0x06 | 0x08 | 0x09 => Err(ParseError::ReservedFrame { kind }),
        _ => Ok(()),
    }
}

pub(crate) fn decode_varint_source<S: crate::Source + ?Sized>(
    input: &S,
) -> Result<(varint::VarInt, usize), ParseError> {
    let mut cursor = crate::Source::cursor(input);
    let total = cursor.remaining();
    let value = decode_varint_cursor(&mut cursor)?;
    Ok((value, total - cursor.remaining()))
}

#[derive(Clone, PartialEq, Eq)]
pub enum Payload<'a> {
    Data(&'a [u8]),
    Headers(&'a [u8]),
    CancelPush {
        push_id: varint::VarInt,
    },
    Settings(Settings),
    PushPromise {
        push_id: varint::VarInt,
        block: &'a [u8],
    },
    GoAway {
        id: varint::VarInt,
    },
    MaxPushId {
        push_id: varint::VarInt,
    },
    Unknown {
        kind: varint::VarInt,
        payload: &'a [u8],
    },
}

impl<'a> Payload<'a> {
    pub fn parse(buf: &'a [u8], limit: usize) -> Result<(Self, usize), ParseError> {
        let header = Header::parse(buf)?;
        check_kind(header.kind)?;
        let Ok(len) = usize::try_from(header.length.get()) else {
            return Err(ParseError::FrameTooLarge);
        };
        if len > limit {
            return Err(ParseError::FrameTooLarge);
        }
        let end = header
            .header_len
            .checked_add(len)
            .ok_or(ParseError::FrameTooLarge)?;
        let payload = buf
            .get(header.header_len..end)
            .ok_or(ParseError::NeedMore)?;
        let frame = match header.kind {
            crate::frame::TYPE_DATA => Self::Data(payload),
            crate::frame::TYPE_HEADERS => Self::Headers(payload),
            crate::frame::TYPE_CANCEL_PUSH => Self::CancelPush {
                push_id: Self::parse_int(payload)?,
            },
            crate::frame::TYPE_SETTINGS => Self::Settings(Settings::decode(payload)?),
            crate::frame::TYPE_PUSH_PROMISE => {
                let (push_id, n) =
                    varint::VarInt::decode(payload).map_err(ParseError::PayloadInteger)?;
                Self::PushPromise {
                    push_id,
                    block: payload.get(n..).ok_or(ParseError::NeedMore)?,
                }
            }
            crate::frame::TYPE_GOAWAY => {
                let id = Self::parse_int(payload)?;
                Self::GoAway { id }
            }
            crate::frame::TYPE_MAX_PUSH_ID => Self::MaxPushId {
                push_id: Self::parse_int(payload)?,
            },
            kind => Self::Unknown { kind, payload },
        };
        Ok((frame, end))
    }

    pub fn encode(
        kind: varint::VarInt,
        payload: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), EncodeError> {
        let header = Header::for_payload(kind, payload.len())?;
        let required = header.header_len + payload.len();
        out.write_slices([
            header.kind.pack().as_ref(),
            header.length.pack().as_ref(),
            payload,
        ])
        .map_err(|error| EncodeError::Reserve { required, error })
    }

    fn parse_int(payload: &[u8]) -> Result<varint::VarInt, ParseError> {
        let (value, n) = varint::VarInt::decode(payload).map_err(ParseError::PayloadInteger)?;
        if n != payload.len() {
            return Err(ParseError::BadVarInt);
        }
        Ok(value)
    }
}

const _: () = assert!(core::mem::size_of::<ErrorCode>() == core::mem::size_of::<u16>());
