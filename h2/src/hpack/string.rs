use sark_core::http::HpackHuffman;

use super::DecoderError;
use super::integer::Integer;

pub(super) struct Codec;

impl Codec {
    pub(super) fn encode(input: &[u8], huffman: bool, out: &mut Vec<u8>) {
        if huffman {
            let len = HpackHuffman::encoded_len(input);
            Integer::encode(len as u64, 7, 0x80, out);
            HpackHuffman::encode(input, out);
        } else {
            Integer::encode(input.len() as u64, 7, 0x00, out);
            out.extend_from_slice(input);
        }
    }

    pub(super) fn decode_into(buf: &[u8], scratch: &mut Vec<u8>) -> Result<usize, DecoderError> {
        if buf.is_empty() {
            return Err(DecoderError::NeedMore);
        }
        let huffman = (buf[0] & 0x80) != 0;
        let (len, n) = Integer::decode(buf, 7)?;
        let len = len as usize;
        if buf.len() < n + len {
            return Err(DecoderError::NeedMore);
        }
        let payload = &buf[n..n + len];
        scratch.clear();
        if huffman {
            HpackHuffman::decode(payload, scratch).map_err(|_| DecoderError::BadString)?;
        } else {
            scratch.extend_from_slice(payload);
        }
        Ok(n + len)
    }
}
