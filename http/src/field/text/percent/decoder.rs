use core::{mem, ops, ptr};

use crate::field::text::percent::{octet, scan, utf8};

#[derive(Debug)]
pub enum Error {
    Bounds {
        capacity: usize,
        source: ops::Range<usize>,
        destination: usize,
    },
}

pub struct Decoder<'a> {
    bytes: &'a mut [u8],
    read: usize,
    start: usize,
    written: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a mut [u8], src: ops::Range<usize>, dst: usize) -> Result<Self, Error> {
        let capacity = bytes.len();
        let Some(read) = src.start.checked_sub(dst).filter(|_| src.start <= src.end) else {
            return Err(Error::Bounds {
                capacity,
                source: src,
                destination: dst,
            });
        };
        let Some(bytes) = bytes.get_mut(dst..src.end) else {
            return Err(Error::Bounds {
                capacity,
                source: src,
                destination: dst,
            });
        };
        Ok(Self {
            bytes,
            read,
            start: dst,
            written: 0,
        })
    }

    /// Decodes within the admitted window without expanding the input.
    /// Each token consumes at least as many bytes as UTF-8 repair emits.
    pub fn decode(mut self) -> Written {
        let mut utf8 = utf8::Utf8::new();
        while self.read < self.bytes.len() {
            let byte = self.bytes[self.read];
            let rest = &self.bytes[self.read + 1..];
            let literal = if byte == b'%' || !byte.is_ascii() {
                0
            } else {
                1 + scan::Scan::new(rest).literal()
            };
            if literal != 0 && utf8.is_ready() {
                self.copy(literal);
                continue;
            }
            let (byte, consumed) = if literal != 0 || byte != b'%' {
                (byte, 1)
            } else {
                rest.first()
                    .copied()
                    .zip(rest.get(1).copied())
                    .and_then(|(high, low)| octet::decode(high, low))
                    .map_or((byte, 1), |byte| (byte, 3))
            };
            utf8.push(self.bytes, &mut self.written, byte);
            self.read += consumed;
        }
        utf8.finish(self.bytes, &mut self.written);
        Written::new(self.start..self.start + self.written)
    }

    fn copy(&mut self, len: usize) {
        // SAFETY: the literal scan bounds len by this owner's unread suffix.
        // Emission never overtakes read, and this run starts with no pending
        // UTF-8 bytes. Both ranges stay in the admitted window; copy permits overlap.
        unsafe {
            let bytes = self.bytes.as_mut_ptr();
            ptr::copy(bytes.add(self.read), bytes.add(self.written), len);
        }
        self.read += len;
        self.written += len;
    }
}

const _: () = assert!(
    mem::size_of::<Decoder<'_>>() == mem::size_of::<&mut [u8]>() + 3 * mem::size_of::<usize>()
);

/// An exact range of UTF-8 emitted by the repairing decoder.
/// Only admitted decoding constructs this proof; the owner must preserve its bytes.
pub struct Written {
    range: ops::Range<usize>,
}

impl Written {
    const fn new(range: ops::Range<usize>) -> Self {
        Self { range }
    }

    pub fn range(self) -> ops::Range<usize> {
        self.range
    }
}

const _: () = assert!(size_of::<Written>() == 2 * size_of::<usize>());
