use sark_http::payload::compress::dynamic::table::delta;

use crate::hpack::encode::Encoder;

pub(crate) struct Commit {
    encoder: delta::Commit<Encoder>,
}

const _: () = assert!(size_of::<Commit>() == size_of::<Encoder>());

impl Commit {
    pub(super) const fn new(encoder: delta::Commit<Encoder>) -> Self {
        Self { encoder }
    }

    pub(crate) fn apply(self) -> Encoder {
        let mut encoder = self.encoder.apply();
        encoder.finish_block();
        encoder
    }

    pub(crate) fn discard(self) -> Encoder {
        self.encoder.rollback()
    }
}
