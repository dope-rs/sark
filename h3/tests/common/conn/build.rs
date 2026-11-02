use sark_h3::{
    frame,
    qpack::encoder::{self, Encoder},
};
use sark_http::field;

#[derive(Debug)]
pub enum Error {
    Encoder(encoder::BuildError),
    Encode(encoder::Error),
}

#[derive(Debug)]
pub enum Build {
    Fields(encoder::Error),
    Frame(frame::EncodeError),
}

pub trait Fields: field::Source + Sized {
    fn try_block(self) -> Result<Vec<u8>, encoder::Error> {
        let mut block = Vec::new();
        Encoder::new().encode(0, self, &mut block)?;
        Ok(block)
    }

    fn try_headers(self) -> Result<Vec<u8>, Build> {
        let block = self.try_block().map_err(Build::Fields)?;
        let mut wire = Vec::new();
        frame::Payload::encode(frame::TYPE_HEADERS, &block, &mut wire).map_err(Build::Frame)?;
        Ok(wire)
    }
}
impl<T: field::Source> Fields for T {}

pub trait Settings {
    fn try_wire(&self) -> Result<Vec<u8>, frame::EncodeError>;
}
impl Settings for frame::Settings {
    fn try_wire(&self) -> Result<Vec<u8>, frame::EncodeError> {
        let mut payload = Vec::new();
        self.encode(&mut payload)
            .map_err(|error| frame::EncodeError::Reserve {
                required: self.encoded_len(),
                error,
            })?;
        let mut wire = Vec::new();
        frame::STREAM_TYPE_CONTROL
            .encode(&mut wire)
            .map_err(|error| frame::EncodeError::Reserve {
                required: usize::from(frame::STREAM_TYPE_CONTROL.encoded_len()),
                error,
            })?;
        frame::Payload::encode(frame::TYPE_SETTINGS, &payload, &mut wire)?;
        Ok(wire)
    }
}
