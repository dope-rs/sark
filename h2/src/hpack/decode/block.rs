use sark_http::field::metadata::map::record;

use crate::hpack::decode::{self, limit, mode, phase::Phase};

pub struct Block {
    phase: Phase<mode::Emit>,
    budget: limit::Budget,
}

impl From<decode::Decoder> for Block {
    fn from(decoder: decode::Decoder) -> Self {
        let budget = decoder.budget();
        Self {
            phase: Phase::Representation(decoder),
            budget,
        }
    }
}

impl Block {
    pub fn decode<S>(self, input: &[u8], sink: &mut S) -> Result<Self, decode::Error>
    where
        S: FnMut(record::Span<'_>, record::Span<'_>),
    {
        let Self { phase, mut budget } = self;
        let (phase, _) = phase
            .decode(&mut budget, input, sink)
            .map_err(|(_, cause)| cause)?;
        Ok(Self { phase, budget })
    }

    pub fn finish(self) -> Result<(decode::Decoder, limit::Admission), decode::Error> {
        Ok((self.phase.finish()?, self.budget.admission))
    }
}
