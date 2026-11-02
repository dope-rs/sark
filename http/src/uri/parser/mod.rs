mod raw;

use core::net;

use crate::field::{metadata::map::record, text};

pub(super) struct Parser<'a>(record::Span<'a>);

impl<'a> Parser<'a> {
    pub(super) const fn new(value: record::Span<'a>) -> Self {
        Self(value)
    }

    pub(super) fn v4(self) -> Option<net::Ipv4Addr> {
        let mut octets = [0; 4];
        let [prefix @ .., last] = &mut octets;
        let mut output = prefix.iter_mut();
        let mut digits = 0;
        let mut octet = 0_u16;
        for byte in self.0.iter().copied() {
            match byte {
                b'0'..=b'9' => {
                    if digits != 0 && octet == 0 {
                        return None;
                    }
                    digits += 1;
                    if digits > 3 {
                        return None;
                    }
                    octet = octet * 10 + u16::from(byte - b'0');
                    if octet > u16::from(u8::MAX) {
                        return None;
                    }
                }
                b'.' => {
                    if digits == 0 {
                        return None;
                    }
                    *output.next()? = octet as u8;
                    digits = 0;
                    octet = 0;
                }
                0..=b'-' | b'/' | b':'..=u8::MAX => return None,
            }
        }
        if output.len() != 0 || digits == 0 {
            return None;
        }
        *last = octet as u8;
        Some(net::Ipv4Addr::from(octets))
    }

    pub(super) fn v6(self) -> Option<net::Ipv6Addr> {
        let value = self.0;
        let mut words = [0; 8];
        let capacity = words.len();
        let mut output = words.iter_mut();
        let mut gap = None;
        let mut at = 0;
        if value.starts_with(b"::") {
            gap = Some(0);
            at = 2;
        } else if value.starts_with(b":") {
            return None;
        }
        while at < value.len() {
            let word = output.next()?;
            let end = value
                .slice(at..value.len())?
                .iter()
                .position(|byte| *byte == b':')
                .map_or(value.len(), |offset| at + offset);
            let part = value.slice(at..end)?;
            if part.iter().any(|byte| *byte == b'.') {
                if end != value.len() {
                    return None;
                }
                let tail = output.next()?;
                let [a, b, c, d] = Self(part).v4()?.octets();
                *word = u16::from_be_bytes([a, b]);
                *tail = u16::from_be_bytes([c, d]);
                break;
            }
            *word = Self::word(part)?;
            if end == value.len() {
                break;
            }
            if value.get(end + 1) == Some(b':') {
                if gap.is_some() {
                    return None;
                }
                gap = Some(capacity - output.len());
                at = end + 2;
            } else {
                at = end + 1;
                if at == value.len() {
                    return None;
                }
            }
        }
        let count = capacity - output.len();
        if let Some(gap) = gap {
            if count >= words.len() {
                return None;
            }
            // SAFETY: gap retains an earlier initialized prefix, and the iterator
            // yields at most eight exclusively borrowed output words.
            // The gate admits count < eight, so moving its tail and filling the gap fit.
            unsafe { raw::expand(&mut words, gap, count) };
        } else if count != words.len() {
            return None;
        }
        Some(net::Ipv6Addr::from(words))
    }

    fn word(value: record::Span<'_>) -> Option<u16> {
        if value.is_empty() || value.len() > 4 {
            return None;
        }
        value.iter().try_fold(0, |word, byte| {
            Some((word << 4) | u16::from(text::hex(*byte)?))
        })
    }
}
