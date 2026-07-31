use core::convert::Infallible;

use sark_core::http::{HpackHuffmanEncoded, HpackHuffmanSource, PrefixedInt};

use super::DecoderError;

pub(super) const MAX_LITERAL_LEN: usize = 1 << 24;

pub(super) struct Literal<'a>(&'a [u8]);

pub(super) struct DecodedLiteral {
    pub(super) consumed: usize,
    pub(super) len: usize,
    pub(super) retained: bool,
}

impl<'a> Literal<'a> {
    pub(super) const fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub(super) fn encode(self, huffman: bool, out: &mut Vec<u8>) {
        if huffman {
            let source = HpackHuffmanSource::new(self.0);
            let len = source.encoded_len();
            PrefixedInt::<7>::new(len as u64).encode(0x80, out);
            source.encode(out);
        } else {
            PrefixedInt::<7>::new(self.0.len() as u64).encode(0x00, out);
            out.extend_from_slice(self.0);
        }
    }

    pub(super) fn decode_into(
        buf: &[u8],
        scratch: &mut Vec<u8>,
        retain_limit: Option<usize>,
    ) -> Result<DecodedLiteral, DecoderError> {
        if buf.is_empty() {
            return Err(DecoderError::NeedMore);
        }
        let huffman = (buf[0] & 0x80) != 0;
        let (len, n) = PrefixedInt::<7>::decode(buf)?;
        let len = len.get();
        if len > MAX_LITERAL_LEN as u64 {
            return Err(DecoderError::BadString);
        }
        let len = len as usize;
        if n > buf.len() || len > buf.len() - n {
            return Err(DecoderError::NeedMore);
        }
        let end = n.checked_add(len).ok_or(DecoderError::BadString)?;
        let payload = &buf[n..end];
        scratch.clear();
        if huffman {
            let mut decoded_len = 0usize;
            HpackHuffmanEncoded::new(payload)
                .decode_with(|byte| {
                    decoded_len += 1;
                    if retain_limit.is_none_or(|limit| decoded_len <= limit) {
                        scratch.push(byte);
                    }
                    Ok::<_, Infallible>(())
                })
                .map_err(|_| DecoderError::BadString)?;
            let retained = retain_limit.is_none_or(|limit| decoded_len <= limit);
            if !retained {
                scratch.clear();
            }
            Ok(DecodedLiteral {
                consumed: end,
                len: decoded_len,
                retained,
            })
        } else {
            if retain_limit.is_none_or(|limit| payload.len() <= limit) {
                scratch.extend_from_slice(payload);
            }
            Ok(DecodedLiteral {
                consumed: end,
                len: payload.len(),
                retained: scratch.len() == payload.len(),
            })
        }
    }
}
