use dope_quic::wire::varint;
use o3::buffer::write;

use crate::frame::{
    Header, ParseError, Settings, TYPE_CANCEL_PUSH, TYPE_GOAWAY, TYPE_MAX_PUSH_ID, TYPE_SETTINGS,
    Wire,
};

#[derive(Clone, Copy)]
enum Payload<'a> {
    Settings(&'a Settings),
    Varint(varint::VarInt),
}

pub(crate) struct Control<'a> {
    header: Header,
    payload: Payload<'a>,
}

impl<'a> Control<'a> {
    pub(crate) fn settings(settings: &'a Settings) -> Result<Self, ParseError> {
        Ok(Self {
            header: Header::for_payload(TYPE_SETTINGS, settings.encoded_len())?,
            payload: Payload::Settings(settings),
        })
    }

    pub(crate) fn cancel_push(push_id: varint::VarInt) -> Self {
        Self::varint(TYPE_CANCEL_PUSH, push_id)
    }

    pub(crate) fn goaway(id: varint::VarInt) -> Self {
        Self::varint(TYPE_GOAWAY, id)
    }

    pub(crate) fn max_push_id(push_id: varint::VarInt) -> Self {
        Self::varint(TYPE_MAX_PUSH_ID, push_id)
    }

    fn varint(kind: varint::VarInt, value: varint::VarInt) -> Self {
        let len = value.encoded_len();
        Self {
            header: Header {
                kind,
                length: varint::VarInt::from_u8(len),
                header_len: usize::from(kind.encoded_len()) + 1,
            },
            payload: Payload::Varint(value),
        }
    }
}

impl Wire for Control<'_> {
    fn encoded_len(&self) -> usize {
        self.header.header_len + self.header.length.get() as usize
    }

    fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        self.header.encode(out)?;
        match self.payload {
            Payload::Settings(settings) => settings.encode(out),
            Payload::Varint(value) => value.encode(out),
        }
    }
}
