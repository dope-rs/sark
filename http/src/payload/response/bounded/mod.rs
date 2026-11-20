use std::convert::Infallible;

use super::Encode;

pub trait Body: Encode {
    fn max_len(&self) -> usize;
}

/// An encoder whose upper bound is admitted before preparing its output.
pub trait TryBody: Encode {
    type Error;

    fn try_max_len(&self) -> Result<usize, Self::Error>;
}

impl<B: Body> TryBody for B {
    type Error = Infallible;

    fn try_max_len(&self) -> Result<usize, Self::Error> {
        Ok(self.max_len())
    }
}
