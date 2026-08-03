use core::convert::Infallible;

use o3::buffer::ByteSink;
use sark_core::http::{
    HpackHuffmanEncoded, HpackHuffmanError, HpackHuffmanSource, PrefixedInt, ValidPrefixedIntWidth,
};

use super::DecoderError;

struct Discard;

impl ByteSink for Discard {
    type Error = Infallible;

    fn write_slices<const N: usize>(&mut self, _slices: [&[u8]; N]) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Copy, Clone)]
pub(super) struct Literal<'a> {
    bytes: &'a [u8],
    huffman: bool,
}

pub(super) enum DecodedLiteral<'a, 'scratch> {
    Borrowed(&'a [u8]),
    Scratch(&'scratch [u8]),
}

impl<'a, 'scratch> DecodedLiteral<'a, 'scratch> {
    pub(super) fn as_slice<'view>(&'view self) -> &'view [u8]
    where
        'a: 'view,
        'scratch: 'view,
    {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Scratch(bytes) => bytes,
        }
    }
}

impl<'a> Literal<'a> {
    pub(super) const fn raw(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            huffman: false,
        }
    }

    pub(super) fn parse<const BITS: u8>(buf: &'a [u8]) -> Result<(Self, usize), DecoderError>
    where
        PrefixedInt<BITS>: LiteralPrefix,
    {
        if buf.is_empty() {
            return Err(DecoderError::NeedMore);
        }
        let huffman = (buf[0] & <PrefixedInt<BITS> as LiteralPrefix>::HUFFMAN_BIT) != 0;
        let (len, n) = PrefixedInt::<BITS>::decode(buf)?;
        let len = usize::try_from(len.get()).map_err(|_| DecoderError::BadInteger)?;
        let end = n.checked_add(len).ok_or(DecoderError::BadInteger)?;
        if buf.len() < end {
            return Err(DecoderError::NeedMore);
        }
        Ok((
            Self {
                bytes: &buf[n..end],
                huffman,
            },
            end,
        ))
    }

    pub(super) fn encode<const BITS: u8>(self, huffman: bool, prefix_byte: u8, out: &mut Vec<u8>)
    where
        PrefixedInt<BITS>: LiteralPrefix,
    {
        if huffman {
            let source = HpackHuffmanSource::new(self.bytes);
            let len = source.encoded_len();
            PrefixedInt::<BITS>::new(len as u64).encode(
                prefix_byte | <PrefixedInt<BITS> as LiteralPrefix>::HUFFMAN_BIT,
                out,
            );
            source.encode(out);
        } else {
            PrefixedInt::<BITS>::new(self.bytes.len() as u64).encode(prefix_byte, out);
            out.extend_from_slice(self.bytes);
        }
    }

    pub(super) fn decode<const BITS: u8>(buf: &'a [u8]) -> Result<(Vec<u8>, usize), DecoderError>
    where
        PrefixedInt<BITS>: LiteralPrefix,
    {
        let (literal, consumed) = Self::parse::<BITS>(buf)?;
        Ok((literal.into_vec()?, consumed))
    }

    pub(super) fn write_to(
        self,
        out: &mut impl ByteSink,
        max_len: usize,
    ) -> Result<usize, DecoderError> {
        if self.huffman {
            let mut len = 0usize;
            HpackHuffmanEncoded::new(self.bytes)
                .decode_with(|byte| {
                    len = len.checked_add(1).ok_or(HpackHuffmanError)?;
                    if len > max_len {
                        return Err(HpackHuffmanError);
                    }
                    out.write_byte(byte).map_err(|_| HpackHuffmanError)?;
                    Ok(())
                })
                .map_err(|_| DecoderError::BadLiteral)?;
            Ok(len)
        } else {
            if self.bytes.len() > max_len {
                return Err(DecoderError::BadLiteral);
            }
            out.write_slice(self.bytes)
                .map_err(|_| DecoderError::Capacity)?;
            Ok(self.bytes.len())
        }
    }

    pub(super) const fn raw_bytes(self) -> Option<&'a [u8]> {
        if self.huffman { None } else { Some(self.bytes) }
    }

    pub(super) fn decode_into<'scratch>(
        self,
        scratch: &'scratch mut Vec<u8>,
        max_len: usize,
    ) -> Result<DecodedLiteral<'a, 'scratch>, DecoderError> {
        if let Some(bytes) = self.raw_bytes() {
            return if bytes.len() <= max_len {
                Ok(DecodedLiteral::Borrowed(bytes))
            } else {
                Err(DecoderError::BadLiteral)
            };
        }
        scratch.clear();
        HpackHuffmanEncoded::new(self.bytes)
            .decode_with(|byte| {
                if scratch.len() == max_len {
                    return Err(HpackHuffmanError);
                }
                scratch.push(byte);
                Ok(())
            })
            .map_err(|_| DecoderError::BadLiteral)?;
        Ok(DecodedLiteral::Scratch(scratch))
    }

    pub(super) fn decoded_len(self, max_len: usize) -> Result<usize, DecoderError> {
        self.write_to(&mut Discard, max_len)
    }

    fn into_vec(self) -> Result<Vec<u8>, DecoderError> {
        let mut out = Vec::new();
        if self.huffman {
            HpackHuffmanEncoded::new(self.bytes)
                .decode(&mut out)
                .map_err(|_| DecoderError::BadLiteral)?;
        } else {
            out.extend_from_slice(self.bytes);
        }
        Ok(out)
    }
}

pub(super) trait LiteralPrefix: ValidPrefixedIntWidth {
    const HUFFMAN_BIT: u8;
}

macro_rules! literal_prefixes {
    ($($bits:literal),+ $(,)?) => {
        $(
            impl LiteralPrefix for PrefixedInt<$bits> {
                const HUFFMAN_BIT: u8 = 1 << $bits;
            }
        )+
    };
}

literal_prefixes!(3, 5, 7);
