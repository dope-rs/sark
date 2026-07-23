#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefixedIntError {
    NeedMore,
    Overflow,
}

pub struct PrefixedInt;

impl PrefixedInt {
    pub fn encode(value: u64, prefix_bits: u8, prefix_byte: u8, out: &mut Vec<u8>) {
        let max_prefix = Self::max_prefix(prefix_bits);
        let mask = if prefix_bits == 8 {
            0
        } else {
            !((1u8 << prefix_bits).wrapping_sub(1))
        };
        let high = prefix_byte & mask;
        if value < max_prefix {
            out.push(high | value as u8);
            return;
        }
        out.push(high | max_prefix as u8);
        let mut remaining = value - max_prefix;
        while remaining >= 128 {
            out.push(((remaining & 0x7f) as u8) | 0x80);
            remaining >>= 7;
        }
        out.push(remaining as u8);
    }

    pub fn decode(buf: &[u8], prefix_bits: u8) -> Result<(u64, usize), PrefixedIntError> {
        let Some(&byte) = buf.first() else {
            return Err(PrefixedIntError::NeedMore);
        };
        let max_prefix = Self::max_prefix(prefix_bits);
        let mask = if prefix_bits == 8 {
            u8::MAX
        } else {
            max_prefix as u8
        };
        let first = (byte & mask) as u64;
        if first < max_prefix {
            return Ok((first, 1));
        }

        let mut value = max_prefix;
        let mut shift = 0u32;
        let mut pos = 1usize;
        loop {
            let Some(&byte) = buf.get(pos) else {
                return Err(PrefixedIntError::NeedMore);
            };
            pos += 1;
            let chunk = (byte & 0x7f) as u64;
            let shifted = chunk.checked_shl(shift).ok_or(PrefixedIntError::Overflow)?;
            value = value
                .checked_add(shifted)
                .ok_or(PrefixedIntError::Overflow)?;
            if byte & 0x80 == 0 {
                return Ok((value, pos));
            }
            shift = shift.checked_add(7).ok_or(PrefixedIntError::Overflow)?;
            if shift >= 64 {
                return Err(PrefixedIntError::Overflow);
            }
        }
    }

    fn max_prefix(prefix_bits: u8) -> u64 {
        assert!(
            (1..=8).contains(&prefix_bits),
            "prefix width must be between one and eight bits"
        );
        (1u64 << prefix_bits) - 1
    }
}
