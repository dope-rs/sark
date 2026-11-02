use crate::{
    input::Content,
    source::string::{Admitted, String},
};

pub(crate) struct Decoder<'a> {
    raw: &'a [u8],
    string: String<Admitted>,
    pending: u32,
    remaining: u8,
}

impl<'a> Decoder<'a> {
    pub(crate) fn new(content: Content<'a>) -> Self {
        Self {
            raw: content.bytes(),
            string: String::admitted(),
            pending: 0,
            remaining: 0,
        }
    }
}

impl Iterator for Decoder<'_> {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        loop {
            if self.remaining != 0 {
                let byte = self.pending as u8;
                self.pending >>= 8;
                self.remaining -= 1;
                return Some(byte);
            }
            let (&byte, rest) = self.raw.split_first()?;
            let (pending, left) = self.string.decode(byte).into_parts();
            self.raw = rest;
            self.pending = pending;
            self.remaining = left;
        }
    }
}
