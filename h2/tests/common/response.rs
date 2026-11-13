use std::cell::Cell;

use o3::buffer::write::{Sink, consume};
use sark_http::{
    field,
    payload::response::{Encode, bounded, exact},
};

use crate::common::headers::entry;

pub const HEAD: [field::Entry<'static>; 4] = [
    field::literal!(entry(b":method", b"HEAD")),
    field::literal!(entry(b":scheme", b"http")),
    field::literal!(entry(b":path", b"/")),
    field::literal!(entry(b":authority", b"localhost")),
];

pub const CONNECT: [field::Entry<'static>; 2] = [
    field::literal!(entry(b":method", b"CONNECT")),
    field::literal!(entry(b":authority", b"localhost:443")),
];

pub struct Body<'a> {
    pub bytes: &'a [u8],
    pub len: usize,
    pub encoded: &'a Cell<usize>,
    pub dropped: &'a Cell<usize>,
}

impl Encode for Body<'_> {
    fn encode<W>(self, out: &mut W) -> Result<(), consume::Error>
    where
        W: Sink<Error = consume::Error>,
    {
        self.encoded.set(self.encoded.get() + 1);
        out.write_slice(self.bytes)
    }
}

impl exact::Body for Body<'_> {
    fn encoded_len(&self) -> usize {
        self.len
    }
}

impl bounded::Body for Body<'_> {
    fn max_len(&self) -> usize {
        self.len
    }
}

impl Drop for Body<'_> {
    fn drop(&mut self) {
        self.dropped.set(self.dropped.get() + 1);
    }
}
