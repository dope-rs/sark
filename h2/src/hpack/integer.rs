use super::DecoderError;

pub(super) struct Integer;

impl Integer {
    pub(super) fn encode(value: u64, prefix_bits: u8, prefix_byte: u8, out: &mut Vec<u8>) {
        let max_prefix: u64 = (1u64 << prefix_bits) - 1;
        let mask: u8 = !((1u8 << prefix_bits).wrapping_sub(1));
        let high = prefix_byte & mask;
        if value < max_prefix {
            out.push(high | (value as u8));
            return;
        }
        out.push(high | (max_prefix as u8));
        let mut remaining = value - max_prefix;
        while remaining >= 128 {
            out.push(((remaining & 0x7f) as u8) | 0x80);
            remaining >>= 7;
        }
        out.push(remaining as u8);
    }

    pub(super) fn decode(buf: &[u8], prefix_bits: u8) -> Result<(u64, usize), DecoderError> {
        if buf.is_empty() {
            return Err(DecoderError::NeedMore);
        }
        let max_prefix: u64 = (1u64 << prefix_bits) - 1;
        let mask: u8 = max_prefix as u8;
        let first = (buf[0] & mask) as u64;
        if first < max_prefix {
            return Ok((first, 1));
        }
        let mut value: u64 = max_prefix;
        let mut shift: u32 = 0;
        let mut pos: usize = 1;
        loop {
            if pos >= buf.len() {
                return Err(DecoderError::NeedMore);
            }
            let b = buf[pos];
            pos += 1;
            let chunk = (b & 0x7f) as u64;
            let shifted = chunk.checked_shl(shift).ok_or(DecoderError::BadInteger)?;
            value = value.checked_add(shifted).ok_or(DecoderError::BadInteger)?;
            if b & 0x80 == 0 {
                return Ok((value, pos));
            }
            shift = shift.checked_add(7).ok_or(DecoderError::BadInteger)?;
            if shift >= 64 {
                return Err(DecoderError::BadInteger);
            }
        }
    }
}
