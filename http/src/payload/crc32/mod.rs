//! Streaming IEEE CRC32 with one CPU admission per hasher.
//!
//! Bytes enter least-significant bit first under polynomial `0xedb88320`.
//! The register starts at all ones and is complemented only when finalized.

use o3::base::marker::ThreadBound;

mod table;

const POLYNOMIAL: u32 = 0xedb8_8320;

/// One instruction set folding bytes into a CRC32 register.
trait Backend: Sized {
    /// Returns the backend only when the running CPU admits every instruction it uses.
    fn admit() -> Option<Self>;

    /// Folds `bytes` into `value`, defaulting to the table when no CPU is admitted.
    fn update(&self, value: u32, bytes: &[u8]) -> u32 {
        table::update(value, bytes)
    }
}

cfg_select! {
    target_arch = "aarch64" => {
        mod aarch64;
        use aarch64::Aarch64 as Enabled;
    }
    target_arch = "x86_64" => {
        mod x86;
        use x86::X86 as Enabled;
    }
    _ => {
        mod scalar;
        use scalar::Scalar as Enabled;
    }
}

/// An IEEE CRC32 register retaining its admitted instruction set.
/// Cloning retains that admission and copies the independent register.
#[derive(Clone, Debug)]
pub struct Hasher {
    value: u32,
    engine: Option<Enabled>,
    _thread: ThreadBound,
}

impl Hasher {
    pub fn new() -> Self {
        Self {
            value: u32::MAX,
            engine: Enabled::admit(),
            _thread: ThreadBound::NEW,
        }
    }

    pub fn hash(bytes: &[u8]) -> u32 {
        let mut state = Self::new();
        state.update(bytes);
        state.finalize()
    }

    /// Appends bytes without changing the initial or final XOR convention.
    pub fn update(&mut self, bytes: &[u8]) {
        self.value = match &self.engine {
            Some(enabled) => enabled.update(self.value, bytes),
            None => table::update(self.value, bytes),
        };
    }

    pub const fn finalize(self) -> u32 {
        !self.value
    }
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Multiplication by x^-bits modulo the reciprocal IEEE polynomial.
const fn shift(mut value: u32, mut bits: usize) -> u32 {
    while bits != 0 {
        value = (value >> 1) ^ (POLYNOMIAL & 0_u32.wrapping_sub(value & 1));
        bits -= 1;
    }
    value
}
