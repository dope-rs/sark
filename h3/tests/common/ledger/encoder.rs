use sark_h3::qpack::{
    encoder,
    stream::{self, Instruction},
};
use sark_http::field;

use super::Section as _;

pub trait Encoder: Sized {
    fn try_dynamic(table: usize, slots: usize, blocked: u64) -> Result<Self, encoder::BuildError>;
    fn try_feedback(&mut self, input: Instruction) -> Result<(), stream::Error>;
    fn try_field(&mut self, stream: u64, field: field::Entry<'_>) -> Result<bool, encoder::Error>;
}

impl Encoder for encoder::Encoder {
    fn try_dynamic(table: usize, slots: usize, blocked: u64) -> Result<Self, encoder::BuildError> {
        let mut encoder = Self::try_bounded(table, slots)?;
        encoder.set_blocked(blocked);
        Ok(encoder)
    }

    fn try_feedback(&mut self, input: Instruction) -> Result<(), stream::Error> {
        let mut wire = Vec::new();
        input.encode(&mut wire);
        let used = self.ingest(&wire)?;
        assert_eq!(used, wire.len());
        Ok(())
    }

    fn try_field(&mut self, stream: u64, field: field::Entry<'_>) -> Result<bool, encoder::Error> {
        Ok(self.encode_for(stream, [field])?.is_dynamic())
    }
}
