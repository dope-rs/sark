use std::cell::Cell;

use o3::buffer::write::{Sink, consume};
use sark_http::payload::response::{Encode, bounded, exact};

pub struct Body<'a>(pub &'a Cell<usize>);

impl Encode for Body<'_> {
    fn encode<W>(self, out: &mut W) -> Result<(), consume::Error>
    where
        W: Sink<Error = consume::Error>,
    {
        self.0.set(self.0.get() + 1);
        out.write_slice(b"body")
    }
}

impl exact::TryBody for Body<'_> {
    fn try_len(&self) -> Result<usize, exact::Overflow> {
        self.0.set(self.0.get() + 1);
        Ok(4)
    }
}

impl bounded::Body for Body<'_> {
    fn max_len(&self) -> usize {
        self.0.set(self.0.get() + 1);
        4
    }
}
