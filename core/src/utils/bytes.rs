pub struct Word;

impl Word {
    pub fn swar_eq_ci<const N: usize>(haystack: &[u8], expected: &[u8; N]) -> bool {
        let Some(h) = haystack.first_chunk::<N>() else {
            return false;
        };
        const M64: u64 = u64::from_le_bytes([0x20; 8]);
        const M32: u32 = u32::from_le_bytes([0x20; 4]);
        const M16: u16 = u16::from_le_bytes([0x20; 2]);
        let mut i = 0usize;
        while i + 8 <= N {
            let raw = u64::from_le_bytes(h[i..i + 8].try_into().unwrap()) | M64;
            let exp = u64::from_le_bytes(expected[i..i + 8].try_into().unwrap());
            if raw != exp {
                return false;
            }
            i += 8;
        }
        if i + 4 <= N {
            let raw = u32::from_le_bytes(h[i..i + 4].try_into().unwrap()) | M32;
            let exp = u32::from_le_bytes(expected[i..i + 4].try_into().unwrap());
            if raw != exp {
                return false;
            }
            i += 4;
        }
        if i + 2 <= N {
            let raw = u16::from_le_bytes(h[i..i + 2].try_into().unwrap()) | M16;
            let exp = u16::from_le_bytes(expected[i..i + 2].try_into().unwrap());
            if raw != exp {
                return false;
            }
            i += 2;
        }
        if i < N && h[i] | 0x20 != expected[i] {
            return false;
        }
        true
    }
}

pub struct Ascii;

impl Ascii {
    #[inline(always)]
    pub fn fold_usize(mut acc: usize, chunk: &[u8]) -> Option<usize> {
        for &b in chunk {
            if !b.is_ascii_digit() {
                return None;
            }
            acc = acc.checked_mul(10)?.checked_add((b - b'0') as usize)?;
        }
        Some(acc)
    }

    #[inline(always)]
    pub fn fold_u64(mut acc: u64, chunk: &[u8]) -> Option<u64> {
        for &b in chunk {
            if !b.is_ascii_digit() {
                return None;
            }
            acc = acc.checked_mul(10)?.checked_add((b - b'0') as u64)?;
        }
        Some(acc)
    }

    #[inline(always)]
    pub fn parse_usize(bytes: &[u8]) -> Option<usize> {
        if bytes.is_empty() {
            return None;
        }
        Self::fold_usize(0, bytes)
    }

    #[inline(always)]
    pub fn parse_u64(bytes: &[u8]) -> Option<u64> {
        if bytes.is_empty() {
            return None;
        }
        Self::fold_u64(0, bytes)
    }
}
