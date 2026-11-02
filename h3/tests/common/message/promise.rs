use dope_quic::wire::varint::VarInt;
use o3::{alloc, buffer::write::Sink};
use sark_h3::{
    frame,
    frame::{
        Payload, STREAM_TYPE_CONTROL, STREAM_TYPE_PUSH, TYPE_CANCEL_PUSH, TYPE_PUSH_PROMISE,
        TYPE_SETTINGS,
    },
    qpack::encoder,
};
use sark_http::field::{self, literal, metadata::section::rules};

use crate::common::{conn::build::Fields as _, field::entry, message::Frame as _};

#[derive(Clone, Copy)]
pub struct Authority;

impl rules::Push for Authority {
    const SCHEMES: Option<&[&[u8]]> = Some(&[b"https"]);
    const AUTHORITIES: Option<&[&[u8]]> = Some(&[b"example.test"]);
}

impl Authority {
    pub fn try_request(self) -> Result<Vec<u8>, encoder::Error> {
        self.try_block()
    }
}

impl field::Source for Authority {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        for entry in [
            literal!(entry(b":method", b"GET")),
            literal!(entry(b":scheme", b"https")),
            literal!(entry(b":path", b"/asset")),
            literal!(entry(b":authority", b"example.test")),
        ] {
            sink.entry(entry);
        }
    }
}

pub trait Push {
    fn try_stream(self, frames: &[u8]) -> Result<Vec<u8>, alloc::Error>;
    fn try_promise(self, block: &[u8]) -> Result<Vec<u8>, frame::EncodeError>;
    fn try_cancel(self) -> Result<Vec<u8>, frame::EncodeError>;
    fn try_control(self) -> Result<Vec<u8>, frame::EncodeError>;
}
impl Push for VarInt {
    fn try_stream(self, frames: &[u8]) -> Result<Vec<u8>, alloc::Error> {
        let mut wire = Vec::new();
        wire.write_slices([
            STREAM_TYPE_PUSH.pack().as_ref(),
            self.pack().as_ref(),
            frames,
        ])?;
        Ok(wire)
    }
    fn try_promise(self, block: &[u8]) -> Result<Vec<u8>, frame::EncodeError> {
        let mut payload = Vec::new();
        payload
            .write_slices([self.pack().as_ref(), block])
            .map_err(|error| frame::EncodeError::Reserve {
                required: usize::from(self.encoded_len()) + block.len(),
                error,
            })?;
        payload.try_frame(TYPE_PUSH_PROMISE)
    }
    fn try_cancel(self) -> Result<Vec<u8>, frame::EncodeError> {
        self.pack().as_ref().try_frame(TYPE_CANCEL_PUSH)
    }
    fn try_control(self) -> Result<Vec<u8>, frame::EncodeError> {
        let mut wire = Vec::new();
        STREAM_TYPE_CONTROL
            .encode(&mut wire)
            .map_err(|error| frame::EncodeError::Reserve {
                required: usize::from(STREAM_TYPE_CONTROL.encoded_len()),
                error,
            })?;
        Payload::encode(TYPE_SETTINGS, &[], &mut wire)?;
        let cancel = self.try_cancel()?;
        wire.write_slice(&cancel)
            .map_err(|error| frame::EncodeError::Reserve {
                required: cancel.len(),
                error,
            })?;
        Ok(wire)
    }
}
