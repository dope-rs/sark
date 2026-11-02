use std::cell::Cell;

use o3::buffer::{write, write::consume};
use sark_http::payload::response::{Encode, bounded, exact};
pub struct Counted<'a> {
    pub bytes: &'a [u8],
    pub bound: usize,
    pub encodes: &'a Cell<usize>,
}

impl bounded::Body for Counted<'_> {
    fn max_len(&self) -> usize {
        self.bound
    }
}

impl Encode for Counted<'_> {
    fn encode<W>(self, out: &mut W) -> Result<(), consume::Error>
    where
        W: write::Sink<Error = consume::Error>,
    {
        self.encodes.set(self.encodes.get() + 1);
        out.write_slice(self.bytes)
    }
}

#[derive(Debug)]
pub struct Checked<'a> {
    pub bytes: &'a [u8],
    pub bound: Result<usize, exact::Overflow>,
    pub bounds: &'a Cell<usize>,
    pub encodes: &'a Cell<usize>,
}

impl bounded::TryBody for Checked<'_> {
    type Error = exact::Overflow;

    fn try_max_len(&self) -> Result<usize, Self::Error> {
        self.bounds.set(self.bounds.get() + 1);
        self.bound
    }
}

impl Encode for Checked<'_> {
    fn encode<W>(self, out: &mut W) -> Result<(), consume::Error>
    where
        W: write::Sink<Error = consume::Error>,
    {
        self.encodes.set(self.encodes.get() + 1);
        out.write_slice(self.bytes)
    }
}

pub struct Short;

impl exact::Body for Short {
    fn encoded_len(&self) -> usize {
        2
    }
}

impl Encode for Short {
    fn encode<W>(self, out: &mut W) -> Result<(), consume::Error>
    where
        W: write::Sink<Error = consume::Error>,
    {
        out.write_slice(b"x")
    }
}
