use o3::buffer::ByteSink;
use o3::num::BoundedU32;

use crate::stream::StreamId;

type FrameLengthValue = BoundedU32<0, 0x00ff_ffff>;
type WindowIncrementValue = BoundedU32<1, 0x7fff_ffff>;

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FrameLength(u32);

impl FrameLength {
    pub const MAX: u32 = 0x00ff_ffff;
    pub const ZERO: Self = Self(0);
    const FOUR: Self = Self(4);
    const FIVE: Self = Self(5);
    const EIGHT: Self = Self(8);

    pub const fn new(value: u32) -> Option<Self> {
        match FrameLengthValue::new(value) {
            Some(value) => Some(Self(value.get())),
            None => None,
        }
    }

    pub const fn from_usize(value: usize) -> Option<Self> {
        match FrameLengthValue::from_usize(value) {
            Some(value) => Some(Self(value.get())),
            None => None,
        }
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }

    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    const fn from_wire_bytes(high: u8, middle: u8, low: u8) -> Self {
        Self(u32::from_be_bytes([0, high, middle, low]))
    }

    const fn wire_bytes(self) -> [u8; 3] {
        let bytes = self.0.to_be_bytes();
        [bytes[1], bytes[2], bytes[3]]
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WindowIncrement(u32);

impl WindowIncrement {
    pub const MAX: u32 = 0x7fff_ffff;

    pub const fn new(value: u32) -> Option<Self> {
        match WindowIncrementValue::new(value) {
            Some(value) => Some(Self(value.get())),
            None => None,
        }
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    const fn from_wire(value: u32) -> Option<Self> {
        Self::new(value & Self::MAX)
    }

    const fn wire_bytes(self) -> [u8; 4] {
        self.0.to_be_bytes()
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

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Flags(pub u8);

impl Flags {
    pub const END_STREAM: u8 = 0x1;
    pub const ACK: u8 = 0x1;
    pub const END_HEADERS: u8 = 0x4;
    pub const PADDED: u8 = 0x8;
    pub const PRIORITY: u8 = 0x20;

    pub fn has(self, bit: u8) -> bool {
        (self.0 & bit) != 0
    }

    pub fn strip(self, payload: &[u8]) -> Result<&[u8], ParseError> {
        if !self.has(Self::PADDED) {
            return Ok(payload);
        }
        if payload.is_empty() {
            return Err(ParseError::Padding);
        }
        let pad_len = payload[0] as usize;
        if 1 + pad_len > payload.len() {
            return Err(ParseError::Padding);
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
            _ => None,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    NeedMore,
    BadType(u8),
    BadLength,
    FrameSize,
    Protocol,
    Padding,
    ZeroIncrement,
}

pub const HEADER_LEN: usize = 9;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub length: FrameLength,
    pub kind: Type,
    pub flags: Flags,
    pub stream_id: StreamId,
}

impl FrameHeader {
    pub fn parse(buf: &[u8]) -> Result<Self, ParseError> {
        if buf.len() < HEADER_LEN {
            return Err(ParseError::NeedMore);
        }
        let length = FrameLength::from_wire_bytes(buf[0], buf[1], buf[2]);
        let kind = Type::from_u8(buf[3]).map_err(ParseError::BadType)?;
        let flags = Flags(buf[4]);
        let stream_id = StreamId::from_wire(u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]));
        Ok(Self {
            length,
            kind,
            flags,
            stream_id,
        })
    }

    pub(crate) fn wire_bytes(&self) -> [u8; HEADER_LEN] {
        let len = self.length.wire_bytes();
        let stream_id = self.stream_id.wire_bytes();
        [
            len[0],
            len[1],
            len[2],
            self.kind as u8,
            self.flags.0,
            stream_id[0],
            stream_id[1],
            stream_id[2],
            stream_id[3],
        ]
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        out.write_slice(&self.wire_bytes())
    }

    fn require_stream(&self) -> Result<(), ParseError> {
        if self.stream_id.is_zero() {
            Err(ParseError::Protocol)
        } else {
            Ok(())
        }
    }

    fn require_connection(&self) -> Result<(), ParseError> {
        if self.stream_id.is_zero() {
            Ok(())
        } else {
            Err(ParseError::Protocol)
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PriorityFields {
    pub exclusive: bool,
    pub dependency: StreamId,
    pub weight: u8,
}

impl PriorityFields {
    pub fn parse(buf: &[u8]) -> Result<Self, ParseError> {
        if buf.len() < 5 {
            return Err(ParseError::FrameSize);
        }
        let raw = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let exclusive = (raw & 0x8000_0000) != 0;
        let dependency = StreamId::from_wire(raw);
        let weight = buf[4];
        Ok(Self {
            exclusive,
            dependency,
            weight,
        })
    }

    fn wire_bytes(&self) -> [u8; 5] {
        let mut raw = self.dependency.as_u32();
        if self.exclusive {
            raw |= 0x8000_0000;
        }
        let dependency = raw.to_be_bytes();
        [
            dependency[0],
            dependency[1],
            dependency[2],
            dependency[3],
            self.weight,
        ]
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        out.write_slice(&self.wire_bytes())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Data<'a> {
    stream_id: StreamId,
    end_stream: bool,
    payload: &'a [u8],
    length: FrameLength,
}

impl<'a> Data<'a> {
    pub fn new(stream_id: StreamId, end_stream: bool, payload: &'a [u8]) -> Option<Self> {
        if stream_id.is_zero() {
            return None;
        }
        Some(Self {
            stream_id,
            end_stream,
            payload,
            length: FrameLength::from_usize(payload.len())?,
        })
    }

    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub const fn end_stream(&self) -> bool {
        self.end_stream
    }

    pub const fn payload(&self) -> &'a [u8] {
        self.payload
    }

    pub fn parse(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        header.require_stream()?;
        let body = header.flags.strip(payload)?;
        Self::new(header.stream_id, header.flags.has(Flags::END_STREAM), body)
            .ok_or(ParseError::FrameSize)
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        Self::encode_parts(
            self.stream_id,
            self.end_stream,
            self.payload,
            &[],
            self.length,
            out,
        )
    }

    pub(crate) fn encode_parts<W: ByteSink>(
        stream_id: StreamId,
        end_stream: bool,
        first: &[u8],
        second: &[u8],
        length: FrameLength,
        out: &mut W,
    ) -> Result<(), W::Error> {
        let len = length.as_usize();
        debug_assert!(len <= first.len().saturating_add(second.len()));
        let header = Self::wire_header(stream_id, end_stream, length);
        let first_len = len.min(first.len());
        out.write_slices([&header, &first[..first_len], &second[..len - first_len]])
    }

    fn wire_header(stream_id: StreamId, end_stream: bool, length: FrameLength) -> [u8; HEADER_LEN] {
        let flags = if end_stream { Flags::END_STREAM } else { 0 };
        FrameHeader {
            length,
            kind: Type::Data,
            flags: Flags(flags),
            stream_id,
        }
        .wire_bytes()
    }

    pub(crate) fn encode_header<W: ByteSink>(
        stream_id: StreamId,
        end_stream: bool,
        length: FrameLength,
        out: &mut W,
    ) -> Result<(), W::Error> {
        out.write_slice(&Self::wire_header(stream_id, end_stream, length))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Headers<'a> {
    stream_id: StreamId,
    end_stream: bool,
    end_headers: bool,
    priority: Option<PriorityFields>,
    block_fragment: &'a [u8],
    length: FrameLength,
}

impl<'a> Headers<'a> {
    pub fn new(
        stream_id: StreamId,
        end_stream: bool,
        end_headers: bool,
        priority: Option<PriorityFields>,
        block_fragment: &'a [u8],
    ) -> Option<Self> {
        if stream_id.is_zero() {
            return None;
        }
        let priority_len: usize = if priority.is_some() { 5 } else { 0 };
        let length = priority_len.checked_add(block_fragment.len())?;
        Some(Self {
            stream_id,
            end_stream,
            end_headers,
            priority,
            block_fragment,
            length: FrameLength::from_usize(length)?,
        })
    }

    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub const fn end_stream(&self) -> bool {
        self.end_stream
    }

    pub const fn end_headers(&self) -> bool {
        self.end_headers
    }

    pub const fn priority(&self) -> Option<PriorityFields> {
        self.priority
    }

    pub const fn block_fragment(&self) -> &'a [u8] {
        self.block_fragment
    }

    pub fn parse(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        header.require_stream()?;
        let unpadded = header.flags.strip(payload)?;
        let (priority, rest) = if header.flags.has(Flags::PRIORITY) {
            if unpadded.len() < 5 {
                return Err(ParseError::FrameSize);
            }
            let pri = PriorityFields::parse(&unpadded[..5])?;
            (Some(pri), &unpadded[5..])
        } else {
            (None, unpadded)
        };
        Self::new(
            header.stream_id,
            header.flags.has(Flags::END_STREAM),
            header.flags.has(Flags::END_HEADERS),
            priority,
            rest,
        )
        .ok_or(ParseError::FrameSize)
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        let mut flags: u8 = 0;
        if self.end_stream {
            flags |= Flags::END_STREAM;
        }
        if self.end_headers {
            flags |= Flags::END_HEADERS;
        }
        if self.priority.is_some() {
            flags |= Flags::PRIORITY;
        }
        let header = FrameHeader {
            length: self.length,
            kind: Type::Headers,
            flags: Flags(flags),
            stream_id: self.stream_id,
        }
        .wire_bytes();
        let priority = self.priority.map(|fields| fields.wire_bytes());
        let priority = priority
            .as_ref()
            .map_or(&[][..], |fields| fields.as_slice());
        out.write_slices([&header, priority, self.block_fragment])
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Priority {
    stream_id: StreamId,
    fields: PriorityFields,
}

impl Priority {
    pub const fn new(stream_id: StreamId, fields: PriorityFields) -> Option<Self> {
        if stream_id.is_zero() || stream_id.as_u32() == fields.dependency.as_u32() {
            None
        } else {
            Some(Self { stream_id, fields })
        }
    }

    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub const fn fields(&self) -> PriorityFields {
        self.fields
    }

    pub fn parse(header: FrameHeader, payload: &[u8]) -> Result<Self, ParseError> {
        if payload.len() != 5 {
            return Err(ParseError::FrameSize);
        }
        header.require_stream()?;
        let fields = PriorityFields::parse(payload)?;
        Self::new(header.stream_id, fields).ok_or(ParseError::Protocol)
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        let header = FrameHeader {
            length: FrameLength::FIVE,
            kind: Type::Priority,
            flags: Flags(0),
            stream_id: self.stream_id,
        }
        .wire_bytes();
        out.write_slices([&header, &self.fields.wire_bytes()])
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RstStream {
    stream_id: StreamId,
    error: ErrorCode,
}

impl RstStream {
    pub const fn new(stream_id: StreamId, error: ErrorCode) -> Option<Self> {
        if stream_id.is_zero() {
            None
        } else {
            Some(Self { stream_id, error })
        }
    }

    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub const fn error(&self) -> ErrorCode {
        self.error
    }

    pub fn parse(header: FrameHeader, payload: &[u8]) -> Result<Self, ParseError> {
        if payload.len() != 4 {
            return Err(ParseError::FrameSize);
        }
        header.require_stream()?;
        let raw = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        Self::new(header.stream_id, ErrorCode::from_u32(raw)).ok_or(ParseError::Protocol)
    }

    pub(crate) fn wire_bytes(self) -> [u8; HEADER_LEN + 4] {
        let header = FrameHeader {
            length: FrameLength::FOUR,
            kind: Type::RstStream,
            flags: Flags(0),
            stream_id: self.stream_id,
        }
        .wire_bytes();
        let error = (self.error as u32).to_be_bytes();
        let mut wire = [0; HEADER_LEN + 4];
        wire[..HEADER_LEN].copy_from_slice(&header);
        wire[HEADER_LEN..].copy_from_slice(&error);
        wire
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        out.write_slice(&self.wire_bytes())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Settings<'a> {
    ack: bool,
    params: &'a [u8],
    length: FrameLength,
}

impl<'a> Settings<'a> {
    pub fn new(ack: bool, params: &'a [u8]) -> Option<Self> {
        if (ack && !params.is_empty()) || !params.len().is_multiple_of(6) {
            return None;
        }
        Some(Self {
            ack,
            params,
            length: FrameLength::from_usize(params.len())?,
        })
    }

    pub const fn ack(&self) -> bool {
        self.ack
    }

    pub const fn params(&self) -> &'a [u8] {
        self.params
    }

    pub fn parse(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        let ack = header.flags.has(Flags::ACK);
        let settings = Self::new(ack, payload).ok_or(ParseError::FrameSize)?;
        header.require_connection()?;
        Ok(settings)
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        let flags = if self.ack { Flags::ACK } else { 0 };
        let header = FrameHeader {
            length: self.length,
            kind: Type::Settings,
            flags: Flags(flags),
            stream_id: StreamId::CONNECTION,
        }
        .wire_bytes();
        out.write_slices([&header, self.params])
    }

    pub fn iter(&self) -> SettingsIter<'a> {
        SettingsIter { rest: self.params }
    }
}

pub struct SettingsIter<'a> {
    rest: &'a [u8],
}

impl Iterator for SettingsIter<'_> {
    type Item = (Option<SettingId>, u32);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.len() < 6 {
            return None;
        }
        let id_raw = u16::from_be_bytes([self.rest[0], self.rest[1]]);
        let val = u32::from_be_bytes([self.rest[2], self.rest[3], self.rest[4], self.rest[5]]);
        self.rest = &self.rest[6..];
        Some((SettingId::from_u16(id_raw), val))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PushPromise<'a> {
    stream_id: StreamId,
    promised_stream_id: StreamId,
    end_headers: bool,
    block_fragment: &'a [u8],
    length: FrameLength,
}

impl<'a> PushPromise<'a> {
    pub fn new(
        stream_id: StreamId,
        promised_stream_id: StreamId,
        end_headers: bool,
        block_fragment: &'a [u8],
    ) -> Option<Self> {
        if stream_id.is_zero() || promised_stream_id.is_zero() {
            return None;
        }
        let length = 4usize.checked_add(block_fragment.len())?;
        Some(Self {
            stream_id,
            promised_stream_id,
            end_headers,
            block_fragment,
            length: FrameLength::from_usize(length)?,
        })
    }

    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub const fn promised_stream_id(&self) -> StreamId {
        self.promised_stream_id
    }

    pub const fn end_headers(&self) -> bool {
        self.end_headers
    }

    pub const fn block_fragment(&self) -> &'a [u8] {
        self.block_fragment
    }

    pub fn parse(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        header.require_stream()?;
        let unpadded = header.flags.strip(payload)?;
        if unpadded.len() < 4 {
            return Err(ParseError::FrameSize);
        }
        let promised = StreamId::from_wire(u32::from_be_bytes([
            unpadded[0],
            unpadded[1],
            unpadded[2],
            unpadded[3],
        ]));
        Self::new(
            header.stream_id,
            promised,
            header.flags.has(Flags::END_HEADERS),
            &unpadded[4..],
        )
        .ok_or(ParseError::Protocol)
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        let flags = if self.end_headers {
            Flags::END_HEADERS
        } else {
            0
        };
        let header = FrameHeader {
            length: self.length,
            kind: Type::PushPromise,
            flags: Flags(flags),
            stream_id: self.stream_id,
        }
        .wire_bytes();
        out.write_slices([
            &header,
            &self.promised_stream_id.wire_bytes(),
            self.block_fragment,
        ])
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Ping {
    pub ack: bool,
    pub opaque: [u8; 8],
}

impl Ping {
    pub fn parse(header: FrameHeader, payload: &[u8]) -> Result<Self, ParseError> {
        if payload.len() != 8 {
            return Err(ParseError::FrameSize);
        }
        header.require_connection()?;
        let mut opaque = [0u8; 8];
        opaque.copy_from_slice(payload);
        Ok(Self {
            ack: header.flags.has(Flags::ACK),
            opaque,
        })
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        let flags = if self.ack { Flags::ACK } else { 0 };
        let header = FrameHeader {
            length: FrameLength::EIGHT,
            kind: Type::Ping,
            flags: Flags(flags),
            stream_id: StreamId::CONNECTION,
        }
        .wire_bytes();
        out.write_slices([&header, &self.opaque])
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct GoAway<'a> {
    last_stream_id: StreamId,
    error: ErrorCode,
    debug: &'a [u8],
    length: FrameLength,
}

impl<'a> GoAway<'a> {
    pub fn new(last_stream_id: StreamId, error: ErrorCode, debug: &'a [u8]) -> Option<Self> {
        let length = 8usize.checked_add(debug.len())?;
        Some(Self {
            last_stream_id,
            error,
            debug,
            length: FrameLength::from_usize(length)?,
        })
    }

    pub const fn last_stream_id(&self) -> StreamId {
        self.last_stream_id
    }

    pub const fn error(&self) -> ErrorCode {
        self.error
    }

    pub const fn debug(&self) -> &'a [u8] {
        self.debug
    }

    pub fn parse(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        header.require_connection()?;
        if payload.len() < 8 {
            return Err(ParseError::FrameSize);
        }
        let last = StreamId::from_wire(u32::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3],
        ]));
        let err_raw = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
        Self::new(last, ErrorCode::from_u32(err_raw), &payload[8..]).ok_or(ParseError::FrameSize)
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        let header = FrameHeader {
            length: self.length,
            kind: Type::GoAway,
            flags: Flags(0),
            stream_id: StreamId::CONNECTION,
        }
        .wire_bytes();
        out.write_slices([
            &header,
            &self.last_stream_id.wire_bytes(),
            &(self.error as u32).to_be_bytes(),
            self.debug,
        ])
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct WindowUpdate {
    pub stream_id: StreamId,
    pub increment: WindowIncrement,
}

impl WindowUpdate {
    pub fn parse(header: FrameHeader, payload: &[u8]) -> Result<Self, ParseError> {
        if payload.len() != 4 {
            return Err(ParseError::FrameSize);
        }
        let increment = WindowIncrement::from_wire(u32::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3],
        ]))
        .ok_or(ParseError::ZeroIncrement)?;
        Ok(Self {
            stream_id: header.stream_id,
            increment,
        })
    }

    pub(crate) fn wire_bytes(self) -> [u8; HEADER_LEN + 4] {
        let header = FrameHeader {
            length: FrameLength::FOUR,
            kind: Type::WindowUpdate,
            flags: Flags(0),
            stream_id: self.stream_id,
        }
        .wire_bytes();
        let increment = self.increment.wire_bytes();
        let mut wire = [0; HEADER_LEN + 4];
        wire[..HEADER_LEN].copy_from_slice(&header);
        wire[HEADER_LEN..].copy_from_slice(&increment);
        wire
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        out.write_slice(&self.wire_bytes())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Continuation<'a> {
    stream_id: StreamId,
    end_headers: bool,
    block_fragment: &'a [u8],
    length: FrameLength,
}

impl<'a> Continuation<'a> {
    pub fn new(stream_id: StreamId, end_headers: bool, block_fragment: &'a [u8]) -> Option<Self> {
        if stream_id.is_zero() {
            return None;
        }
        Some(Self {
            stream_id,
            end_headers,
            block_fragment,
            length: FrameLength::from_usize(block_fragment.len())?,
        })
    }

    pub const fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub const fn end_headers(&self) -> bool {
        self.end_headers
    }

    pub const fn block_fragment(&self) -> &'a [u8] {
        self.block_fragment
    }

    pub fn parse(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        header.require_stream()?;
        Self::new(
            header.stream_id,
            header.flags.has(Flags::END_HEADERS),
            payload,
        )
        .ok_or(ParseError::FrameSize)
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        let flags = if self.end_headers {
            Flags::END_HEADERS
        } else {
            0
        };
        let header = FrameHeader {
            length: self.length,
            kind: Type::Continuation,
            flags: Flags(flags),
            stream_id: self.stream_id,
        }
        .wire_bytes();
        out.write_slices([&header, self.block_fragment])
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Frame<'a> {
    Data(Data<'a>),
    Headers(Headers<'a>),
    Priority(Priority),
    RstStream(RstStream),
    Settings(Settings<'a>),
    PushPromise(PushPromise<'a>),
    Ping(Ping),
    GoAway(GoAway<'a>),
    WindowUpdate(WindowUpdate),
    Continuation(Continuation<'a>),
}

impl<'a> Frame<'a> {
    pub fn parse(buf: &'a [u8]) -> Result<(Self, usize), ParseError> {
        let header = FrameHeader::parse(buf)?;
        let total = HEADER_LEN + header.length.as_usize();
        if buf.len() < total {
            return Err(ParseError::NeedMore);
        }
        let payload = &buf[HEADER_LEN..total];
        let frame = Self::parse_payload(header, payload)?;
        Ok((frame, total))
    }

    pub fn parse_payload(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        if payload.len() != header.length.as_usize() {
            return Err(ParseError::BadLength);
        }
        Ok(match header.kind {
            Type::Data => Self::Data(Data::parse(header, payload)?),
            Type::Headers => Self::Headers(Headers::parse(header, payload)?),
            Type::Priority => Self::Priority(Priority::parse(header, payload)?),
            Type::RstStream => Self::RstStream(RstStream::parse(header, payload)?),
            Type::Settings => Self::Settings(Settings::parse(header, payload)?),
            Type::PushPromise => Self::PushPromise(PushPromise::parse(header, payload)?),
            Type::Ping => Self::Ping(Ping::parse(header, payload)?),
            Type::GoAway => Self::GoAway(GoAway::parse(header, payload)?),
            Type::WindowUpdate => Self::WindowUpdate(WindowUpdate::parse(header, payload)?),
            Type::Continuation => Self::Continuation(Continuation::parse(header, payload)?),
        })
    }

    pub fn encode<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        match self {
            Self::Data(f) => f.encode(out),
            Self::Headers(f) => f.encode(out),
            Self::Priority(f) => f.encode(out),
            Self::RstStream(f) => f.encode(out),
            Self::Settings(f) => f.encode(out),
            Self::PushPromise(f) => f.encode(out),
            Self::Ping(f) => f.encode(out),
            Self::GoAway(f) => f.encode(out),
            Self::WindowUpdate(f) => f.encode(out),
            Self::Continuation(f) => f.encode(out),
        }
    }
}
