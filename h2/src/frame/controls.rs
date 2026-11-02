//! SETTINGS parameters and related control-frame values.

use o3::buffer::write;

use crate::{
    frame::{self, layout, numbers},
    stream,
};

const HEADER_LEN: usize = frame::HEADER_LEN;

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Priority {
    stream_id: stream::Nonzero,
    fields: layout::Priority,
}

impl Priority {
    pub const fn new(stream: stream::Id, fields: layout::Priority) -> Option<Self> {
        let Some(id) = stream.nonzero() else {
            return None;
        };
        let Ok(fields) = fields.check(id) else {
            return None;
        };
        Some(Self {
            stream_id: id,
            fields,
        })
    }

    pub const fn stream_id(&self) -> stream::Id {
        self.stream_id.get()
    }

    pub const fn fields(&self) -> layout::Priority {
        self.fields
    }

    pub fn parse(header: layout::Header, payload: &[u8]) -> Result<Self, numbers::ParseError> {
        let Ok(payload) = payload.try_into() else {
            return Err(numbers::ParseError::FrameSize {
                kind: numbers::Type::Priority,
                actual: payload.len(),
                expected: 5,
            });
        };
        Self::read(header, payload)
    }

    pub(crate) fn read(
        header: layout::Header,
        payload: &[u8; 5],
    ) -> Result<Self, numbers::ParseError> {
        let id = header.try_stream()?;
        let fields = layout::Priority::read(payload).check(id)?;
        Ok(Self {
            stream_id: id,
            fields,
        })
    }

    pub fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        let header = layout::Header {
            length: numbers::Length::FIVE,
            kind: numbers::Type::Priority,
            flags: numbers::Flags::new(0),
            stream_id: self.stream_id.get(),
        }
        .wire_bytes();
        out.write_slices([&header, &self.fields.wire_bytes()])
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct RstStream {
    stream_id: stream::Nonzero,
    error: numbers::ErrorCode,
}

impl RstStream {
    pub(crate) const fn from_stream(stream: stream::Nonzero, error: numbers::ErrorCode) -> Self {
        Self {
            stream_id: stream,
            error,
        }
    }

    pub const fn new(stream: stream::Id, error: numbers::ErrorCode) -> Option<Self> {
        let Some(id) = stream.nonzero() else {
            return None;
        };
        Some(Self {
            stream_id: id,
            error,
        })
    }

    pub const fn stream_id(&self) -> stream::Id {
        self.stream_id.get()
    }

    pub(crate) const fn stream(&self) -> stream::Nonzero {
        self.stream_id
    }

    pub const fn error(&self) -> numbers::ErrorCode {
        self.error
    }

    pub fn parse(header: layout::Header, payload: &[u8]) -> Result<Self, numbers::ParseError> {
        if payload.len() != 4 {
            return Err(numbers::ParseError::FrameSize {
                kind: numbers::Type::RstStream,
                actual: payload.len(),
                expected: 4,
            });
        }
        let id = header.try_stream()?;
        let raw = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        Ok(Self {
            stream_id: id,
            error: numbers::ErrorCode::from_u32(raw),
        })
    }

    pub(crate) fn wire_bytes(self) -> [u8; HEADER_LEN + 4] {
        let header = layout::Header {
            length: numbers::Length::FOUR,
            kind: numbers::Type::RstStream,
            flags: numbers::Flags::new(0),
            stream_id: self.stream_id.get(),
        }
        .wire_bytes();
        let error = (self.error as u32).to_be_bytes();
        let mut wire = [0; HEADER_LEN + 4];
        wire[..HEADER_LEN].copy_from_slice(&header);
        wire[HEADER_LEN..].copy_from_slice(&error);
        wire
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Settings<'a> {
    ack: frame::Ack,
    params: &'a [u8],
    length: numbers::Length,
}

impl<'a> Settings<'a> {
    pub const EMPTY: Settings<'static> = Settings {
        ack: frame::Ack::Required,
        params: &[],
        length: numbers::Length::ZERO,
    };

    pub fn new(ack: frame::Ack, params: &'a [u8]) -> Option<Self> {
        if (matches!(ack, frame::Ack::Complete) && !params.is_empty())
            || !params.len().is_multiple_of(6)
        {
            return None;
        }
        Some(Self {
            ack,
            params,
            length: numbers::Length::from_usize(params.len())?,
        })
    }

    pub fn parse(header: layout::Header, payload: &'a [u8]) -> Result<Self, numbers::ParseError> {
        let ack = frame::Ack::from(header.flags);
        let settings = Self::new(ack, payload).ok_or(numbers::ParseError::SettingsSize {
            actual: payload.len(),
            ack,
        })?;
        header.check_conn()?;
        Ok(settings)
    }

    pub fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        let header = layout::Header {
            length: self.length,
            kind: numbers::Type::Settings,
            flags: numbers::Flags::new(self.ack.flags()),
            stream_id: stream::Id::CONNECTION,
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
    type Item = (Option<numbers::SettingId>, u32);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.len() < 6 {
            return None;
        }
        let id_raw = u16::from_be_bytes([self.rest[0], self.rest[1]]);
        let val = u32::from_be_bytes([self.rest[2], self.rest[3], self.rest[4], self.rest[5]]);
        self.rest = &self.rest[6..];
        Some((numbers::SettingId::from_u16(id_raw), val))
    }
}
