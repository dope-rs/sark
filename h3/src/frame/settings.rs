use dope_quic::wire::varint;
use o3::buffer::{storage::inline, write};
use sark_http::field::metadata::block;

use crate::{
    frame::{self, ParseError, PayloadView},
    sources,
};

#[derive(Clone, PartialEq, Eq)]
pub struct Settings {
    table: varint::VarInt,
    fields: Option<varint::VarInt>,
    blocked: varint::VarInt,
    connect: block::Connect,
}

impl Settings {
    pub const fn new(
        table: varint::VarInt,
        fields: Option<varint::VarInt>,
        blocked: varint::VarInt,
        connect: block::Connect,
    ) -> Self {
        Self {
            table,
            fields,
            blocked,
            connect,
        }
    }

    /// Maximum QPACK dynamic-table capacity advertised to the peer.
    pub const fn table(&self) -> varint::VarInt {
        self.table
    }

    /// Maximum accepted field-section size, when one was advertised.
    pub const fn field_limit(&self) -> Option<varint::VarInt> {
        self.fields
    }

    /// Maximum QPACK blocked-stream count advertised to the peer.
    pub const fn blocked(&self) -> varint::VarInt {
        self.blocked
    }

    pub const fn connect(&self) -> block::Connect {
        self.connect
    }

    pub(crate) fn enable(&mut self) {
        self.connect = block::Connect::Enabled;
    }

    pub fn encoded_len(&self) -> usize {
        Self::setting_len(frame::SETTINGS_QPACK_MAX_TABLE_CAPACITY, self.table)
            + self.fields.map_or(0, |value| {
                Self::setting_len(frame::SETTINGS_MAX_FIELD_SECTION_SIZE, value)
            })
            + Self::setting_len(frame::SETTINGS_QPACK_BLOCKED_STREAMS, self.blocked)
            + match self.connect {
                block::Connect::Disabled => 0,
                block::Connect::Enabled => Self::setting_len(
                    frame::SETTINGS_ENABLE_CONNECT_PROTOCOL,
                    varint::VarInt::from_u8(1),
                ),
            }
    }

    pub fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        let fields = match self.fields {
            Some(value) => [frame::SETTINGS_MAX_FIELD_SECTION_SIZE.pack(), value.pack()],
            None => [inline::Bytes::new(); 2],
        };
        let connect = match self.connect {
            block::Connect::Disabled => [inline::Bytes::new(); 2],
            block::Connect::Enabled => [
                frame::SETTINGS_ENABLE_CONNECT_PROTOCOL.pack(),
                varint::VarInt::from_u8(1).pack(),
            ],
        };
        out.write_slices([
            frame::SETTINGS_QPACK_MAX_TABLE_CAPACITY.pack().as_ref(),
            self.table.pack().as_ref(),
            fields[0].as_ref(),
            fields[1].as_ref(),
            frame::SETTINGS_QPACK_BLOCKED_STREAMS.pack().as_ref(),
            self.blocked.pack().as_ref(),
            connect[0].as_ref(),
            connect[1].as_ref(),
        ])
    }

    fn setting_len(id: varint::VarInt, value: varint::VarInt) -> usize {
        usize::from(id.encoded_len()) + usize::from(value.encoded_len())
    }

    pub fn decode(mut payload: &[u8]) -> Result<Self, ParseError> {
        let mut decoder = Decoder::new();
        while !payload.is_empty() {
            let (id, n) = varint::VarInt::decode(payload).map_err(ParseError::PayloadInteger)?;
            let tail = payload.get(n..).ok_or(ParseError::NeedMore)?;
            let (value, m) = varint::VarInt::decode(tail).map_err(ParseError::PayloadInteger)?;
            payload = tail.get(m..).ok_or(ParseError::NeedMore)?;
            decoder.push(id, value)?;
        }
        Ok(decoder.settings)
    }

    pub(crate) fn from_view<'a, I: sources::Chunks<'a>>(
        payload: &PayloadView<'a, I>,
    ) -> Result<Self, ParseError> {
        let mut decoder = Decoder::new();
        let mut cursor = crate::Source::cursor(payload);
        while cursor.remaining() != 0 {
            let id = frame::decode_payload_varint(&mut cursor)?;
            let value = frame::decode_payload_varint(&mut cursor)?;
            decoder.push(id, value)?;
        }
        Ok(decoder.settings)
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self::new(
            varint::VarInt::ZERO,
            None,
            varint::VarInt::ZERO,
            block::Connect::Disabled,
        )
    }
}

struct Decoder {
    settings: Settings,
    seen: u8,
}

#[derive(Clone, Copy)]
enum Setting {
    Table,
    Fields,
    Blocked,
    Connect,
}

impl Setting {
    const fn mask(self) -> u8 {
        match self {
            Self::Table => 1,
            Self::Fields => 2,
            Self::Blocked => 4,
            Self::Connect => 8,
        }
    }
}

impl Decoder {
    fn new() -> Self {
        Self {
            settings: Settings::default(),
            seen: 0,
        }
    }

    fn push(&mut self, id: varint::VarInt, value: varint::VarInt) -> Result<(), ParseError> {
        let setting = match id {
            frame::SETTINGS_QPACK_MAX_TABLE_CAPACITY => Some(Setting::Table),
            frame::SETTINGS_MAX_FIELD_SECTION_SIZE => Some(Setting::Fields),
            frame::SETTINGS_QPACK_BLOCKED_STREAMS => Some(Setting::Blocked),
            frame::SETTINGS_ENABLE_CONNECT_PROTOCOL => Some(Setting::Connect),
            _ if id.get() == 0 || (0x02..=0x05).contains(&id.get()) => {
                return Err(ParseError::BadSettings);
            }
            _ => None,
        };
        let Some(setting) = setting else {
            return Ok(());
        };
        let mask = setting.mask();
        if self.seen & mask != 0 {
            return Err(ParseError::DuplicateSetting);
        }
        self.seen |= mask;
        match setting {
            Setting::Table => self.settings.table = value,
            Setting::Fields => self.settings.fields = Some(value),
            Setting::Blocked => self.settings.blocked = value,
            Setting::Connect => match value.get() {
                0 => self.settings.connect = block::Connect::Disabled,
                1 => self.settings.connect = block::Connect::Enabled,
                _ => return Err(ParseError::BadSettings),
            },
        }
        Ok(())
    }
}
