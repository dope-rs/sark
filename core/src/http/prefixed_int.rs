#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefixedIntError {
    NeedMore,
    Overflow,
}

/// An HPACK/QPACK integer whose prefix width is fixed by its type.
///
/// Every `u64` is representable; construction only records the wire format.
///
/// Invalid wire widths expose no safe constructor or codec:
///
/// ```compile_fail
/// use sark_core::http::PrefixedInt;
///
/// let _ = PrefixedInt::<0>::new(0);
/// ```
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrefixedInt<const BITS: u8>(u64);

#[doc(hidden)]
pub trait ValidPrefixedIntWidth {
    const MAX_PREFIX: u64;
}

macro_rules! valid_widths {
    ($($bits:literal),+ $(,)?) => {
        $(
            impl ValidPrefixedIntWidth for PrefixedInt<$bits> {
                const MAX_PREFIX: u64 = (1u64 << $bits) - 1;
            }
        )+
    };
}

valid_widths!(1, 2, 3, 4, 5, 6, 7, 8);

impl<const BITS: u8> PrefixedInt<BITS>
where
    Self: ValidPrefixedIntWidth,
{
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn encoded_len(self) -> usize {
        if self.0 < Self::MAX_PREFIX {
            return 1;
        }
        let remaining = self.0 - Self::MAX_PREFIX;
        2 + ((remaining | 1).ilog2() / 7) as usize
    }

    pub fn encode(self, prefix_byte: u8, out: &mut impl Extend<u8>) {
        let low_mask = Self::MAX_PREFIX as u8;
        let high = prefix_byte & !low_mask;
        if self.0 < Self::MAX_PREFIX {
            out.extend([high | self.0 as u8]);
            return;
        }
        out.extend([high | low_mask]);
        let mut remaining = self.0 - Self::MAX_PREFIX;
        while remaining >= 128 {
            out.extend([((remaining & 0x7f) as u8) | 0x80]);
            remaining >>= 7;
        }
        out.extend([remaining as u8]);
    }

    pub fn decode(buf: &[u8]) -> Result<(Self, usize), PrefixedIntError> {
        let Some(&byte) = buf.first() else {
            return Err(PrefixedIntError::NeedMore);
        };
        let first = (byte & Self::MAX_PREFIX as u8) as u64;
        if first < Self::MAX_PREFIX {
            return Ok((Self(first), 1));
        }

        let mut value = Self::MAX_PREFIX;
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
                return Ok((Self(value), pos));
            }
            shift = shift.checked_add(7).ok_or(PrefixedIntError::Overflow)?;
            if shift >= 64 {
                return Err(PrefixedIntError::Overflow);
            }
        }
    }
}

impl<const BITS: u8> From<PrefixedInt<BITS>> for u64
where
    PrefixedInt<BITS>: ValidPrefixedIntWidth,
{
    fn from(value: PrefixedInt<BITS>) -> Self {
        value.get()
    }
}
