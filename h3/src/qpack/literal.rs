use sark_core::http::{
    FieldValueWriter, HpackHuffmanEncoded, HpackHuffmanError, HpackHuffmanSource, PrefixedInt,
    ValidPrefixedIntWidth,
};

use super::DecoderError;

#[derive(Copy, Clone)]
pub(super) struct Literal<'a> {
    bytes: &'a [u8],
    huffman: bool,
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
        out: &mut FieldValueWriter<'_>,
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
                    out.push(byte);
                    Ok(())
                })
                .map_err(|_| DecoderError::BadLiteral)?;
            Ok(len)
        } else {
            if self.bytes.len() > max_len {
                return Err(DecoderError::BadLiteral);
            }
            out.extend_from_slice(self.bytes);
            Ok(self.bytes.len())
        }
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
