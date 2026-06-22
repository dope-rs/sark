use crate::stream::StreamId;

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
    pub length: u32,
    pub kind: Type,
    pub flags: Flags,
    pub stream_id: StreamId,
}

impl FrameHeader {
    pub fn parse(buf: &[u8]) -> Result<Self, ParseError> {
        if buf.len() < HEADER_LEN {
            return Err(ParseError::NeedMore);
        }
        let length = u32::from_be_bytes([0, buf[0], buf[1], buf[2]]);
        let kind = Type::from_u8(buf[3]).map_err(ParseError::BadType)?;
        let flags = Flags(buf[4]);
        let stream_id =
            StreamId::from_u32_masked(u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]));
        Ok(Self {
            length,
            kind,
            flags,
            stream_id,
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        let len = self.length & 0x00ff_ffff;
        out.push((len >> 16) as u8);
        out.push((len >> 8) as u8);
        out.push(len as u8);
        out.push(self.kind as u8);
        out.push(self.flags.0);
        out.extend_from_slice(&self.stream_id.masked().to_be_bytes());
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
        let dependency = StreamId::from_u32_masked(raw);
        let weight = buf[4];
        Ok(Self {
            exclusive,
            dependency,
            weight,
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        let mut raw = self.dependency.masked();
        if self.exclusive {
            raw |= 0x8000_0000;
        }
        out.extend_from_slice(&raw.to_be_bytes());
        out.push(self.weight);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Data<'a> {
    pub stream_id: StreamId,
    pub end_stream: bool,
    pub payload: &'a [u8],
}

impl<'a> Data<'a> {
    pub fn parse(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        header.require_stream()?;
        let body = header.flags.strip(payload)?;
        Ok(Self {
            stream_id: header.stream_id,
            end_stream: header.flags.has(Flags::END_STREAM),
            payload: body,
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        let flags = if self.end_stream {
            Flags::END_STREAM
        } else {
            0
        };
        FrameHeader {
            length: self.payload.len() as u32,
            kind: Type::Data,
            flags: Flags(flags),
            stream_id: self.stream_id,
        }
        .encode(out);
        out.extend_from_slice(self.payload);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Headers<'a> {
    pub stream_id: StreamId,
    pub end_stream: bool,
    pub end_headers: bool,
    pub priority: Option<PriorityFields>,
    pub block_fragment: &'a [u8],
}

impl<'a> Headers<'a> {
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
        Ok(Self {
            stream_id: header.stream_id,
            end_stream: header.flags.has(Flags::END_STREAM),
            end_headers: header.flags.has(Flags::END_HEADERS),
            priority,
            block_fragment: rest,
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
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
        let priority_len = if self.priority.is_some() { 5 } else { 0 };
        let length = (priority_len + self.block_fragment.len()) as u32;
        FrameHeader {
            length,
            kind: Type::Headers,
            flags: Flags(flags),
            stream_id: self.stream_id,
        }
        .encode(out);
        if let Some(pri) = self.priority {
            pri.encode(out);
        }
        out.extend_from_slice(self.block_fragment);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Priority {
    pub stream_id: StreamId,
    pub fields: PriorityFields,
}

impl Priority {
    pub fn parse(header: FrameHeader, payload: &[u8]) -> Result<Self, ParseError> {
        if payload.len() != 5 {
            return Err(ParseError::FrameSize);
        }
        header.require_stream()?;
        let fields = PriorityFields::parse(payload)?;
        Ok(Self {
            stream_id: header.stream_id,
            fields,
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        FrameHeader {
            length: 5,
            kind: Type::Priority,
            flags: Flags(0),
            stream_id: self.stream_id,
        }
        .encode(out);
        self.fields.encode(out);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RstStream {
    pub stream_id: StreamId,
    pub error: ErrorCode,
}

impl RstStream {
    pub fn parse(header: FrameHeader, payload: &[u8]) -> Result<Self, ParseError> {
        if payload.len() != 4 {
            return Err(ParseError::FrameSize);
        }
        header.require_stream()?;
        let raw = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        Ok(Self {
            stream_id: header.stream_id,
            error: ErrorCode::from_u32(raw),
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        FrameHeader {
            length: 4,
            kind: Type::RstStream,
            flags: Flags(0),
            stream_id: self.stream_id,
        }
        .encode(out);
        out.extend_from_slice(&(self.error as u32).to_be_bytes());
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Settings<'a> {
    pub ack: bool,
    pub params: &'a [u8],
}

impl<'a> Settings<'a> {
    pub fn parse(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        let ack = header.flags.has(Flags::ACK);
        if ack && !payload.is_empty() {
            return Err(ParseError::FrameSize);
        }
        if !payload.is_empty() && !payload.len().is_multiple_of(6) {
            return Err(ParseError::FrameSize);
        }
        header.require_connection()?;
        Ok(Self {
            ack,
            params: payload,
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        let flags = if self.ack { Flags::ACK } else { 0 };
        FrameHeader {
            length: self.params.len() as u32,
            kind: Type::Settings,
            flags: Flags(flags),
            stream_id: StreamId(0),
        }
        .encode(out);
        out.extend_from_slice(self.params);
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
    pub stream_id: StreamId,
    pub promised_stream_id: StreamId,
    pub end_headers: bool,
    pub block_fragment: &'a [u8],
}

impl<'a> PushPromise<'a> {
    pub fn parse(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        header.require_stream()?;
        let unpadded = header.flags.strip(payload)?;
        if unpadded.len() < 4 {
            return Err(ParseError::FrameSize);
        }
        let promised = StreamId::from_u32_masked(u32::from_be_bytes([
            unpadded[0],
            unpadded[1],
            unpadded[2],
            unpadded[3],
        ]));
        Ok(Self {
            stream_id: header.stream_id,
            promised_stream_id: promised,
            end_headers: header.flags.has(Flags::END_HEADERS),
            block_fragment: &unpadded[4..],
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        let flags = if self.end_headers {
            Flags::END_HEADERS
        } else {
            0
        };
        let length = (4 + self.block_fragment.len()) as u32;
        FrameHeader {
            length,
            kind: Type::PushPromise,
            flags: Flags(flags),
            stream_id: self.stream_id,
        }
        .encode(out);
        out.extend_from_slice(&self.promised_stream_id.masked().to_be_bytes());
        out.extend_from_slice(self.block_fragment);
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

    pub fn encode(&self, out: &mut Vec<u8>) {
        let flags = if self.ack { Flags::ACK } else { 0 };
        FrameHeader {
            length: 8,
            kind: Type::Ping,
            flags: Flags(flags),
            stream_id: StreamId(0),
        }
        .encode(out);
        out.extend_from_slice(&self.opaque);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct GoAway<'a> {
    pub last_stream_id: StreamId,
    pub error: ErrorCode,
    pub debug: &'a [u8],
}

impl<'a> GoAway<'a> {
    pub fn parse(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        header.require_connection()?;
        if payload.len() < 8 {
            return Err(ParseError::FrameSize);
        }
        let last = StreamId::from_u32_masked(u32::from_be_bytes([
            payload[0], payload[1], payload[2], payload[3],
        ]));
        let err_raw = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
        Ok(Self {
            last_stream_id: last,
            error: ErrorCode::from_u32(err_raw),
            debug: &payload[8..],
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        let length = (8 + self.debug.len()) as u32;
        FrameHeader {
            length,
            kind: Type::GoAway,
            flags: Flags(0),
            stream_id: StreamId(0),
        }
        .encode(out);
        out.extend_from_slice(&self.last_stream_id.masked().to_be_bytes());
        out.extend_from_slice(&(self.error as u32).to_be_bytes());
        out.extend_from_slice(self.debug);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct WindowUpdate {
    pub stream_id: StreamId,
    pub increment: u32,
}

impl WindowUpdate {
    const MASK: u32 = 0x7fff_ffff;

    pub fn parse(header: FrameHeader, payload: &[u8]) -> Result<Self, ParseError> {
        if payload.len() != 4 {
            return Err(ParseError::FrameSize);
        }
        let inc = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & Self::MASK;
        if inc == 0 {
            return Err(ParseError::ZeroIncrement);
        }
        Ok(Self {
            stream_id: header.stream_id,
            increment: inc,
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        FrameHeader {
            length: 4,
            kind: Type::WindowUpdate,
            flags: Flags(0),
            stream_id: self.stream_id,
        }
        .encode(out);
        let inc = self.increment & Self::MASK;
        out.extend_from_slice(&inc.to_be_bytes());
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Continuation<'a> {
    pub stream_id: StreamId,
    pub end_headers: bool,
    pub block_fragment: &'a [u8],
}

impl<'a> Continuation<'a> {
    pub fn parse(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        header.require_stream()?;
        Ok(Self {
            stream_id: header.stream_id,
            end_headers: header.flags.has(Flags::END_HEADERS),
            block_fragment: payload,
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        let flags = if self.end_headers {
            Flags::END_HEADERS
        } else {
            0
        };
        FrameHeader {
            length: self.block_fragment.len() as u32,
            kind: Type::Continuation,
            flags: Flags(flags),
            stream_id: self.stream_id,
        }
        .encode(out);
        out.extend_from_slice(self.block_fragment);
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
        let total = HEADER_LEN + header.length as usize;
        if buf.len() < total {
            return Err(ParseError::NeedMore);
        }
        let payload = &buf[HEADER_LEN..total];
        let frame = Self::parse_payload(header, payload)?;
        Ok((frame, total))
    }

    pub fn parse_payload(header: FrameHeader, payload: &'a [u8]) -> Result<Self, ParseError> {
        if payload.len() != header.length as usize {
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

    pub fn encode(&self, out: &mut Vec<u8>) {
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
