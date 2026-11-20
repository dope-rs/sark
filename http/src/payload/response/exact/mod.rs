mod overflow;

pub use overflow::Overflow;

use super::Encode;

pub trait Body: Encode {
    fn encoded_len(&self) -> usize;
}

/// An encoder whose size must be admitted before transport output is prepared.
pub trait TryBody: Encode {
    fn try_len(&self) -> Result<usize, Overflow>;
}

impl<B: Body> TryBody for B {
    fn try_len(&self) -> Result<usize, Overflow> {
        Ok(self.encoded_len())
    }
}
