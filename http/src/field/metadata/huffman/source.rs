use core::num::NonZero;

use o3::{alloc, buffer::write::Sink};

use crate::field::metadata::huffman::decoder::{DecodeError, Decoder, Error, Failure, Progress};

#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq)]
/// Bytes before HPACK/QPACK Huffman encoding.
pub struct Source<'a>(&'a [u8]);

/// The exact encoded length retained with its unchanged source bytes.
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Measured<'a> {
    source: Source<'a>,
    len: usize,
}

/// An exact encoded length that cannot be represented by the destination domain.
/// Failed narrowing proves its high word is nonzero. A slice's maximum length
/// and `u8` code lengths bound the complete result by two native words.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Overflow {
    high: NonZero<usize>,
    low: usize,
}

/// A source portion whose byte count admits every `u8` code length into `u64`.
struct Chunk<'a>(&'a [u8]);

#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq)]
/// Bytes carrying an HPACK/QPACK Huffman bit stream.
pub struct Encoded<'a>(&'a [u8]);

impl<'a> Source<'a> {
    pub const fn new(input: &'a [u8]) -> Self {
        Self(input)
    }

    /// Measures each admitted chunk with `u64` sums and retains its source.
    /// Slice length and `u8` code lengths bound the total by `u128`, avoiding
    /// checked arithmetic within the byte loop.
    pub fn try_measure(self) -> Result<Measured<'a>, Overflow> {
        let mut input = self.0;
        let mut bits = u128::from(Chunk::take(&mut input).bits());
        while !input.is_empty() {
            bits += u128::from(Chunk::take(&mut input).bits());
        }
        let encoded = bits.div_ceil(8);
        let len = usize::try_from(encoded).map_err(|_| Overflow {
            high: NonZero::<usize>::MIN.saturating_add((encoded >> usize::BITS) as usize - 1),
            low: encoded as usize,
        })?;
        Ok(Measured { source: self, len })
    }

    pub fn encode(self, out: &mut impl Extend<u8>) {
        let mut buffer: u64 = 0;
        let mut bits_in: u32 = 0;
        for &byte in self.0 {
            let (code, len) = super::CODES[byte as usize];
            let len = len as u32;
            buffer = (buffer << len) | u64::from(code);
            bits_in += len;
            while bits_in >= 8 {
                bits_in -= 8;
                out.extend([(buffer >> bits_in) as u8]);
            }
        }
        if bits_in > 0 {
            let pad_bits = 8 - bits_in;
            buffer = (buffer << pad_bits) | ((1u64 << pad_bits) - 1);
            out.extend([buffer as u8]);
        }
    }
}

impl Measured<'_> {
    pub const fn encoded_len(&self) -> usize {
        self.len
    }

    pub fn encode(self, out: &mut impl Extend<u8>) {
        self.source.encode(out);
    }
}

impl Overflow {
    pub const fn encoded(self) -> NonZero<u128> {
        let encoded = ((self.high.get() as u128) << usize::BITS) | self.low as u128;
        NonZero::<u128>::MIN.saturating_add(encoded - 1)
    }
}

impl<'a> Chunk<'a> {
    const CAPACITY: NonZero<usize> = if usize::BITS < u64::BITS {
        NonZero::<usize>::MAX
    } else {
        NonZero::<usize>::MIN.saturating_add((u64::MAX / u8::MAX as u64) as usize - 1)
    };

    fn take(input: &mut &'a [u8]) -> Self {
        let (bytes, tail) = input.split_at(input.len().min(Self::CAPACITY.get()));
        *input = tail;
        Self(bytes)
    }

    fn bits(self) -> u64 {
        let mut bits = 0;
        for &byte in self.0 {
            bits += u64::from(super::CODES[byte as usize].1);
        }
        bits
    }
}

const _: () = {
    assert!(usize::BITS >= u8::BITS && usize::BITS <= u64::BITS);
    assert!(size_of::<Overflow>() == 2 * size_of::<usize>());
    assert!(align_of::<Overflow>() == align_of::<usize>());
};

impl<'a> Encoded<'a> {
    pub const fn new(input: &'a [u8]) -> Self {
        Self(input)
    }

    pub fn decode(self, out: Vec<u8>) -> Result<Vec<u8>, DecodeError<alloc::Error>> {
        let (out, _) = self.write_to(out).map_err(|failure| failure.cause)?;
        Ok(out)
    }

    /// Validates the complete encoded value and returns its decoded length
    /// without materializing output bytes.
    pub fn decoded_len(self) -> Result<usize, Error> {
        let (decoder, decoded) = Decoder::new().feed_count(self.0)?;
        decoder.finish()?;
        Ok(decoded)
    }

    /// Decodes the complete value directly into a byte destination.
    pub fn write_to<W: Sink>(self, sink: W) -> Result<(W, usize), Failure<W, W::Error>> {
        let (decoder, sink, decoded) = Decoder::new().feed_into(self.0, sink)?.into_parts();
        match decoder.finish() {
            Ok(()) => Ok((sink, decoded)),
            Err(cause) => Err(Failure {
                sink,
                cause: DecodeError::InvalidEncoding(cause),
                progress: Progress::new(self.0.len(), decoded),
            }),
        }
    }

    pub fn decode_with<E, F>(self, sink: F) -> Result<(), Failure<F, E>>
    where
        F: FnMut(u8) -> Result<(), E>,
    {
        let (decoder, sink, decoded) = Decoder::new().feed(self.0, sink)?.into_parts();
        match decoder.finish() {
            Ok(()) => Ok(()),
            Err(cause) => Err(Failure {
                sink,
                cause: DecodeError::InvalidEncoding(cause),
                progress: Progress::new(self.0.len(), decoded),
            }),
        }
    }
}
