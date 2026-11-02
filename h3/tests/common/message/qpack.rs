use dope_quic::wire::stream::Id;
use o3::{alloc, buffer::write::Sink};
use sark_h3::{
    frame::STREAM_TYPE_QPACK_ENCODER,
    qpack::encoder::{self, Encoder},
};
use sark_http::field::literal;

use crate::common::{conn::build, field::entry};

#[derive(Clone, Copy)]
pub enum Kind {
    Request,
    Response,
}

impl Kind {
    pub fn try_blocked(self, stream: Id) -> Result<(Vec<u8>, encoder::Wire), build::Error> {
        let field = literal!(entry(b"x-dynamic", b"retained"));
        let mut encoder = Encoder::try_table(256).map_err(build::Error::Encoder)?;
        encoder.set_blocked(1);
        let mut first = Vec::new();
        encoder
            .encode(stream.get(), [field], &mut first)
            .map_err(build::Error::Encode)?;
        let code = encoder.take_wire();
        let mut block = Vec::new();
        match self {
            Kind::Request => encoder
                .encode(
                    stream.get(),
                    [
                        literal!(entry(b":method", b"GET")),
                        literal!(entry(b":scheme", b"https")),
                        literal!(entry(b":authority", b"example.test")),
                        literal!(entry(b":path", b"/")),
                        field,
                    ],
                    &mut block,
                )
                .map_err(build::Error::Encode)?,
            Kind::Response => encoder
                .encode(
                    stream.get(),
                    [literal!(entry(b":status", b"200")), field],
                    &mut block,
                )
                .map_err(build::Error::Encode)?,
        }
        Ok((block, code))
    }
}
pub trait Wire {
    fn try_wire(&self) -> Result<Vec<u8>, alloc::Error>;
}
impl Wire for encoder::Wire {
    fn try_wire(&self) -> Result<Vec<u8>, alloc::Error> {
        let mut wire = Vec::new();
        wire.write_slices([STREAM_TYPE_QPACK_ENCODER.pack().as_ref(), self])?;
        Ok(wire)
    }
}
