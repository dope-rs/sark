use sark_h3::qpack::{
    encoder,
    stream::{self, Instruction},
};

pub trait Encoder: Sized {
    fn try_dynamic(table: usize, slots: usize, blocked: u64) -> Result<Self, encoder::BuildError>;
    fn try_feedback(&mut self, input: Instruction) -> Result<(usize, usize), stream::Error>;
}

impl Encoder for encoder::Encoder {
    fn try_dynamic(table: usize, slots: usize, blocked: u64) -> Result<Self, encoder::BuildError> {
        let mut encoder = Self::try_bounded(table, slots)?;
        encoder.set_blocked(blocked);
        Ok(encoder)
    }

    fn try_feedback(&mut self, input: Instruction) -> Result<(usize, usize), stream::Error> {
        let mut wire = Vec::new();
        input.encode(&mut wire);
        Ok((self.ingest(&wire)?, wire.len()))
    }
}
