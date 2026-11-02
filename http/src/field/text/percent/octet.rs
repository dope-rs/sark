use crate::field::text::hex;

const HEX: &[u8; 16] = b"0123456789ABCDEF";

pub(super) fn encode(byte: u8) -> [u8; 3] {
    [b'%', HEX[(byte >> 4) as usize], HEX[(byte & 0x0f) as usize]]
}

pub(super) fn decode(high: u8, low: u8) -> Option<u8> {
    Some(hex(high)? << 4 | hex(low)?)
}
