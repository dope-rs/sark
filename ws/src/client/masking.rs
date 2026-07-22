use core::cell::Cell;

use rand_chacha::ChaCha20Rng;
use rand_core::{Rng, SeedableRng};

#[derive(Default)]
pub(super) struct MaskSequence {
    stream: Cell<Option<ChaCha20Rng>>,
}

impl MaskSequence {
    pub(super) fn next(&self) -> [u8; 4] {
        let mut stream = self.stream.take().unwrap_or_else(|| {
            let mut seed = [0u8; 32];
            getrandom::fill(&mut seed).expect("OS CSPRNG (getrandom) unavailable");
            ChaCha20Rng::from_seed(seed)
        });
        let mut mask = [0u8; 4];
        stream.fill_bytes(&mut mask);
        self.stream.set(Some(stream));
        mask
    }
}
