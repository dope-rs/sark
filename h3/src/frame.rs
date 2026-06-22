use sark_core::http::{VarInt, varint};

pub const TYPE_DATA: u64 = 0x00;
pub const TYPE_HEADERS: u64 = 0x01;
pub const TYPE_CANCEL_PUSH: u64 = 0x03;
pub const TYPE_SETTINGS: u64 = 0x04;
pub const TYPE_PUSH_PROMISE: u64 = 0x05;
pub const TYPE_GOAWAY: u64 = 0x07;
pub const TYPE_MAX_PUSH_ID: u64 = 0x0d;

pub const STREAM_TYPE_CONTROL: u64 = 0x00;
pub const STREAM_TYPE_PUSH: u64 = 0x01;
pub const STREAM_TYPE_QPACK_ENCODER: u64 = 0x02;
pub const STREAM_TYPE_QPACK_DECODER: u64 = 0x03;

pub const SETTINGS_QPACK_MAX_TABLE_CAPACITY: u64 = 0x01;
pub const SETTINGS_MAX_FIELD_SECTION_SIZE: u64 = 0x06;
pub const SETTINGS_QPACK_BLOCKED_STREAMS: u64 = 0x07;
pub const SETTINGS_ENABLE_CONNECT_PROTOCOL: u64 = 0x08;

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
    pub kind: u64,
    pub length: u64,
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

    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), ParseError> {
        VarInt::encode(self.kind, out)?;
        VarInt::encode(self.length, out)?;
        Ok(())
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

    fn push_setting(out: &mut Vec<u8>, id: u64, value: u64) -> Result<(), ParseError> {
        VarInt::encode(id, out)?;
        VarInt::encode(value, out)?;
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
                    out.qpack_max_table_capacity = value;
                }
                SETTINGS_MAX_FIELD_SECTION_SIZE => {
                    Self::set_once(&mut seen, 1)?;
                    out.max_field_section_size = Some(value);
                }
                SETTINGS_QPACK_BLOCKED_STREAMS => {
                    Self::set_once(&mut seen, 2)?;
                    out.qpack_blocked_streams = value;
                }
                SETTINGS_ENABLE_CONNECT_PROTOCOL => {
                    Self::set_once(&mut seen, 3)?;
                    out.enable_connect_protocol = value == 1;
                    if value > 1 {
                        return Err(ParseError::BadSettings);
                    }
                }
                0x02..=0x05 => return Err(ParseError::BadSettings),
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
    CancelPush { push_id: u64 },
    Settings(Settings),
    PushPromise { push_id: u64, block: &'a [u8] },
    GoAway { id: u64 },
    MaxPushId { push_id: u64 },
    Unknown { kind: u64, payload: &'a [u8] },
}

impl<'a> Frame<'a> {
    pub fn parse(buf: &'a [u8], max_frame_size: usize) -> Result<(Self, usize), ParseError> {
        let header = FrameHeader::parse(buf)?;
        let len = usize::try_from(header.length).map_err(|_| ParseError::FrameTooLarge)?;
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

    pub fn encode(kind: u64, payload: &[u8], out: &mut Vec<u8>) -> Result<(), ParseError> {
        VarInt::encode(kind, out)?;
        VarInt::encode(payload.len() as u64, out)?;
        out.extend_from_slice(payload);
        Ok(())
    }

    pub fn encode_varint(kind: u64, value: u64, out: &mut Vec<u8>) -> Result<(), ParseError> {
        let mut payload = Vec::new();
        VarInt::encode(value, &mut payload)?;
        Self::encode(kind, &payload, out)
    }

    pub fn encode_push_promise(
        push_id: u64,
        block: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), ParseError> {
        let mut payload = Vec::new();
        VarInt::encode(push_id, &mut payload)?;
        payload.extend_from_slice(block);
        Self::encode(TYPE_PUSH_PROMISE, &payload, out)
    }

    fn parse_varint_payload(payload: &[u8]) -> Result<u64, ParseError> {
        let (value, n) = VarInt::decode(payload)?;
        if n != payload.len() {
            return Err(ParseError::BadVarInt);
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_frame_round_trips() {
        let settings = Settings {
            qpack_max_table_capacity: 0,
            max_field_section_size: Some(4096),
            qpack_blocked_streams: 0,
            enable_connect_protocol: true,
        };
        let mut payload = Vec::new();
        settings.encode(&mut payload).unwrap();
        let mut wire = Vec::new();
        Frame::encode(TYPE_SETTINGS, &payload, &mut wire).unwrap();

        let (frame, n) = Frame::parse(&wire, 1024).unwrap();
        assert_eq!(n, wire.len());
        assert_eq!(frame, Frame::Settings(settings));
    }

    #[test]
    fn duplicate_settings_are_rejected() {
        let mut payload = Vec::new();
        Settings::push_setting(&mut payload, SETTINGS_QPACK_BLOCKED_STREAMS, 0).unwrap();
        Settings::push_setting(&mut payload, SETTINGS_QPACK_BLOCKED_STREAMS, 1).unwrap();

        assert_eq!(
            Settings::decode(&payload),
            Err(ParseError::DuplicateSetting)
        );
    }

    #[test]
    fn http2_settings_ids_are_rejected() {
        let mut payload = Vec::new();
        Settings::push_setting(&mut payload, 0x04, 65_535).unwrap();

        assert_eq!(Settings::decode(&payload), Err(ParseError::BadSettings));
    }

    #[test]
    fn id_frames_round_trip() {
        let mut wire = Vec::new();
        Frame::encode_varint(TYPE_CANCEL_PUSH, 7, &mut wire).unwrap();
        assert_eq!(
            Frame::parse(&wire, 1024).unwrap().0,
            Frame::CancelPush { push_id: 7 }
        );

        wire.clear();
        Frame::encode_varint(TYPE_MAX_PUSH_ID, 9, &mut wire).unwrap();
        assert_eq!(
            Frame::parse(&wire, 1024).unwrap().0,
            Frame::MaxPushId { push_id: 9 }
        );
    }

    #[test]
    fn push_promise_round_trips() {
        let mut wire = Vec::new();
        Frame::encode_push_promise(3, b"fields", &mut wire).unwrap();
        assert_eq!(
            Frame::parse(&wire, 1024).unwrap().0,
            Frame::PushPromise {
                push_id: 3,
                block: b"fields"
            }
        );
    }
}
