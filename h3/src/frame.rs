use dope_quic::varint::{self, VarInt};

pub const TYPE_DATA: VarInt = VarInt::from_u8(0x00);
pub const TYPE_HEADERS: VarInt = VarInt::from_u8(0x01);
pub const TYPE_CANCEL_PUSH: VarInt = VarInt::from_u8(0x03);
pub const TYPE_SETTINGS: VarInt = VarInt::from_u8(0x04);
pub const TYPE_PUSH_PROMISE: VarInt = VarInt::from_u8(0x05);
pub const TYPE_GOAWAY: VarInt = VarInt::from_u8(0x07);
pub const TYPE_MAX_PUSH_ID: VarInt = VarInt::from_u8(0x0d);

pub const STREAM_TYPE_CONTROL: VarInt = VarInt::from_u8(0x00);
pub const STREAM_TYPE_PUSH: VarInt = VarInt::from_u8(0x01);
pub const STREAM_TYPE_QPACK_ENCODER: VarInt = VarInt::from_u8(0x02);
pub const STREAM_TYPE_QPACK_DECODER: VarInt = VarInt::from_u8(0x03);

pub const SETTINGS_QPACK_MAX_TABLE_CAPACITY: VarInt = VarInt::from_u8(0x01);
pub const SETTINGS_MAX_FIELD_SECTION_SIZE: VarInt = VarInt::from_u8(0x06);
pub const SETTINGS_QPACK_BLOCKED_STREAMS: VarInt = VarInt::from_u8(0x07);
pub const SETTINGS_ENABLE_CONNECT_PROTOCOL: VarInt = VarInt::from_u8(0x08);

#[repr(u64)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    NoError = 0x0100,
    GeneralProtocol = 0x0101,
    Internal = 0x0102,
    StreamCreation = 0x0103,
    ClosedCriticalStream = 0x0104,
    FrameUnexpected = 0x0105,
    Frame = 0x0106,
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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    NeedMore,
    BadVarInt,
    BadSettings,
    DuplicateSetting,
    FrameTooLarge,
}

impl From<varint::Error> for ParseError {
    fn from(err: varint::Error) -> Self {
        match err {
            varint::Error::Underflow => Self::NeedMore,
            varint::Error::TooLarge => Self::BadVarInt,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub kind: VarInt,
    pub length: VarInt,
    pub header_len: usize,
}

impl FrameHeader {
    pub fn parse(buf: &[u8]) -> Result<Self, ParseError> {
        let (kind, n) = VarInt::decode(buf)?;
        let (length, m) = VarInt::decode(&buf[n..])?;
        Ok(Self {
            kind,
            length,
            header_len: n + m,
        })
    }

    pub fn encode(&self, out: &mut impl Extend<u8>) {
        self.kind.encode(out);
        self.length.encode(out);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Settings {
    pub qpack_max_table_capacity: u64,
    pub max_field_section_size: Option<u64>,
    pub qpack_blocked_streams: u64,
    pub enable_connect_protocol: bool,
}

impl Settings {
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), ParseError> {
        Self::push_setting(
            out,
            SETTINGS_QPACK_MAX_TABLE_CAPACITY,
            self.qpack_max_table_capacity,
        )?;
        if let Some(value) = self.max_field_section_size {
            Self::push_setting(out, SETTINGS_MAX_FIELD_SECTION_SIZE, value)?;
        }
        Self::push_setting(
            out,
            SETTINGS_QPACK_BLOCKED_STREAMS,
            self.qpack_blocked_streams,
        )?;
        if self.enable_connect_protocol {
            Self::push_setting(out, SETTINGS_ENABLE_CONNECT_PROTOCOL, 1)?;
        }
        Ok(())
    }

    fn push_setting(out: &mut Vec<u8>, id: VarInt, value: u64) -> Result<(), ParseError> {
        id.encode(out);
        VarInt::new(value).ok_or(ParseError::BadVarInt)?.encode(out);
        Ok(())
    }

    fn set_once(seen: &mut u8, bit: u8) -> Result<(), ParseError> {
        let mask = 1u8 << bit;
        if (*seen & mask) != 0 {
            return Err(ParseError::DuplicateSetting);
        }
        *seen |= mask;
        Ok(())
    }

    pub fn decode(mut payload: &[u8]) -> Result<Self, ParseError> {
        let mut out = Self::default();
        let mut seen = 0u8;
        while !payload.is_empty() {
            let (id, n) = VarInt::decode(payload)?;
            let (value, m) = VarInt::decode(&payload[n..])?;
            payload = &payload[n + m..];
            match id {
                SETTINGS_QPACK_MAX_TABLE_CAPACITY => {
                    Self::set_once(&mut seen, 0)?;
                    out.qpack_max_table_capacity = value.get();
                }
                SETTINGS_MAX_FIELD_SECTION_SIZE => {
                    Self::set_once(&mut seen, 1)?;
                    out.max_field_section_size = Some(value.get());
                }
                SETTINGS_QPACK_BLOCKED_STREAMS => {
                    Self::set_once(&mut seen, 2)?;
                    out.qpack_blocked_streams = value.get();
                }
                SETTINGS_ENABLE_CONNECT_PROTOCOL => {
                    Self::set_once(&mut seen, 3)?;
                    out.enable_connect_protocol = value.get() == 1;
                    if value.get() > 1 {
                        return Err(ParseError::BadSettings);
                    }
                }
                _ if (0x02..=0x05).contains(&id.get()) => {
                    return Err(ParseError::BadSettings);
                }
                _ => {}
            }
        }
        Ok(out)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame<'a> {
    Data(&'a [u8]),
    Headers(&'a [u8]),
    CancelPush { push_id: VarInt },
    Settings(Settings),
    PushPromise { push_id: VarInt, block: &'a [u8] },
    GoAway { id: VarInt },
    MaxPushId { push_id: VarInt },
    Unknown { kind: VarInt, payload: &'a [u8] },
}

impl<'a> Frame<'a> {
    pub fn parse(buf: &'a [u8], max_frame_size: usize) -> Result<(Self, usize), ParseError> {
        let header = FrameHeader::parse(buf)?;
        let len = usize::try_from(header.length.get()).map_err(|_| ParseError::FrameTooLarge)?;
        if len > max_frame_size {
            return Err(ParseError::FrameTooLarge);
        }
        let end = header
            .header_len
            .checked_add(len)
            .ok_or(ParseError::FrameTooLarge)?;
        if buf.len() < end {
            return Err(ParseError::NeedMore);
        }
        let payload = &buf[header.header_len..end];
        let frame = match header.kind {
            TYPE_DATA => Self::Data(payload),
            TYPE_HEADERS => Self::Headers(payload),
            TYPE_CANCEL_PUSH => Self::CancelPush {
                push_id: Self::parse_varint_payload(payload)?,
            },
            TYPE_SETTINGS => Self::Settings(Settings::decode(payload)?),
            TYPE_PUSH_PROMISE => {
                let (push_id, n) = VarInt::decode(payload)?;
                Self::PushPromise {
                    push_id,
                    block: &payload[n..],
                }
            }
            TYPE_GOAWAY => {
                let id = Self::parse_varint_payload(payload)?;
                Self::GoAway { id }
            }
            TYPE_MAX_PUSH_ID => Self::MaxPushId {
                push_id: Self::parse_varint_payload(payload)?,
            },
            kind => Self::Unknown { kind, payload },
        };
        Ok((frame, end))
    }

    pub fn encode(kind: VarInt, payload: &[u8], out: &mut Vec<u8>) -> Result<(), ParseError> {
        kind.encode(out);
        VarInt::from_usize(payload.len())
            .ok_or(ParseError::FrameTooLarge)?
            .encode(out);
        out.extend_from_slice(payload);
        Ok(())
    }

    pub fn encode_varint(kind: VarInt, value: VarInt, out: &mut Vec<u8>) -> Result<(), ParseError> {
        let mut payload = Vec::new();
        value.encode(&mut payload);
        Self::encode(kind, &payload, out)
    }

    pub fn encode_push_promise(
        push_id: VarInt,
        block: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), ParseError> {
        let mut payload = Vec::new();
        push_id.encode(&mut payload);
        payload.extend_from_slice(block);
        Self::encode(TYPE_PUSH_PROMISE, &payload, out)
    }

    fn parse_varint_payload(payload: &[u8]) -> Result<VarInt, ParseError> {
        let (value, n) = VarInt::decode(payload)?;
        if n != payload.len() {
            return Err(ParseError::BadVarInt);
        }
        Ok(value)
    }
}
