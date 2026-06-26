use sark_core::http::HpackHuffman;

use super::DecoderError;
use super::integer::Integer;

pub(super) struct Codec;

pub(super) const MAX_LITERAL_LEN: usize = 1 << 24;

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
            HpackHuffman::decode(payload, scratch).map_err(|_| DecoderError::BadString)?;
        } else {
            scratch.extend_from_slice(payload);
        }
        Ok(end)
    }
}
