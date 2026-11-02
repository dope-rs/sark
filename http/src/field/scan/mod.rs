//! Allocation-free scans over immutable wire bytes.

use core::{mem, num::NonZero};

const LANE: usize = 16;

mod raw;

/// One actual forward-search match retained on its exact immutable source.
#[derive(Clone, Copy)]
pub struct Match<'a> {
    bytes: &'a [u8],
    end: NonZero<usize>,
}

impl<'a> Match<'a> {
    pub fn find(byte: u8, bytes: &'a [u8]) -> Option<Self> {
        let index = memchr::memchr(byte, bytes)?;
        // SAFETY: memchr returned this byte's position inside the exact retained source.
        Some(unsafe { raw::found(bytes, index) })
    }

    pub fn find2(first: u8, second: u8, bytes: &'a [u8]) -> Option<Self> {
        let index = memchr::memchr2(first, second, bytes)?;
        // SAFETY: memchr2 returned a matched position inside the exact retained source.
        Some(unsafe { raw::found(bytes, index) })
    }

    pub(crate) fn find_by(bytes: &'a [u8], mut matches: impl FnMut(u8) -> bool) -> Option<Self> {
        let index = bytes.iter().position(|byte| matches(*byte))?;
        // SAFETY: the iterator returned this position inside the exact retained source.
        Some(unsafe { raw::found(bytes, index) })
    }

    pub const fn start(self) -> usize {
        self.end.get() - 1
    }

    pub const fn end(self) -> NonZero<usize> {
        self.end
    }

    pub fn byte(self) -> u8 {
        raw::byte(self)
    }

    pub fn prefix(self) -> &'a [u8] {
        raw::before::<false>(self)
    }

    pub fn through(self) -> &'a [u8] {
        raw::before::<true>(self)
    }

    pub fn tail(self) -> &'a [u8] {
        raw::after::<true>(self)
    }

    pub fn suffix(self) -> &'a [u8] {
        raw::after::<false>(self)
    }
}

#[derive(PartialEq, Eq)]
pub enum Outcome<T> {
    Found(T),
    Invalid { offset: usize, actual: u8 },
    Partial,
}

/// A pointer-sized view over bytes scanned as HTTP/1 field syntax.
#[repr(transparent)]
pub struct Bytes<'a>(&'a [u8]);

impl<'a> Bytes<'a> {
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub fn name(&self, start: usize) -> Outcome<(usize, u8)> {
        let Some(rest) = self.0.get(start..) else {
            return Outcome::Partial;
        };
        let (prefix, rest) = rest.split_at(rest.len().min(LANE));
        let outcome = name_tail(prefix, start);
        if !matches!(outcome, Outcome::Partial) {
            return outcome;
        }
        if rest.is_empty() {
            return Outcome::Partial;
        }
        let index = start + prefix.len();
        if rest.len() < LANE {
            return name_tail(rest, index);
        }
        name_simd(rest, index)
    }

    pub fn value(&self, start: usize) -> Outcome<usize> {
        let Some(rest) = self.0.get(start..) else {
            return Outcome::Partial;
        };
        value_simd(rest, start)
    }
}

fn name_simd(bytes: &[u8], mut index: usize) -> Outcome<(usize, u8)> {
    let (lanes, tail) = bytes.as_chunks::<LANE>();
    let (groups, lanes) = lanes.as_chunks::<4>();
    for group in groups {
        let [a, b, c, d] = group;
        let masks = [
            mask::<true>(*a),
            mask::<true>(*b),
            mask::<true>(*c),
            mask::<true>(*d),
        ];
        let Some(found) = NonZero::new(packed(masks)) else {
            index += 4 * LANE;
            continue;
        };
        let (offset, byte) = first(group, found);
        return name_at(index + offset, byte);
    }
    for lane in lanes {
        let bits = mask::<true>(*lane);
        if bits != 0 {
            let (offset, byte) = in_lane(lane, bits);
            return name_at(index + offset, byte);
        }
        index += LANE;
    }
    if tail.is_empty() {
        Outcome::Partial
    } else {
        name_tail(tail, index)
    }
}

fn value_simd(bytes: &[u8], mut index: usize) -> Outcome<usize> {
    let Some((lane, rest)) = bytes.split_first_chunk::<LANE>() else {
        return value_tail(bytes, index);
    };
    let bits = mask::<false>(*lane);
    if bits != 0 {
        let (offset, byte) = in_lane(lane, bits);
        return value_at(bytes, offset, index + offset, byte);
    }
    index += LANE;
    let (lanes, tail) = rest.as_chunks::<LANE>();
    let (groups, lanes) = lanes.as_chunks::<4>();
    let mut consumed = LANE;
    for group in groups {
        let [a, b, c, d] = group;
        let masks = [
            mask::<false>(*a),
            mask::<false>(*b),
            mask::<false>(*c),
            mask::<false>(*d),
        ];
        let Some(found) = NonZero::new(packed(masks)) else {
            index += 4 * LANE;
            consumed += 4 * LANE;
            continue;
        };
        let (offset, byte) = first(group, found);
        return value_at(bytes, consumed + offset, index + offset, byte);
    }
    for lane in lanes {
        let bits = mask::<false>(*lane);
        if bits != 0 {
            let (offset, byte) = in_lane(lane, bits);
            return value_at(bytes, consumed + offset, index + offset, byte);
        }
        index += LANE;
        consumed += LANE;
    }
    if tail.is_empty() {
        Outcome::Partial
    } else {
        value_tail(tail, index)
    }
}

fn name_tail(bytes: &[u8], index: usize) -> Outcome<(usize, u8)> {
    for (offset, &byte) in bytes.iter().enumerate() {
        let pos = index + offset;
        if byte == b':' || byte == b'\r' {
            return Outcome::Found((pos, byte));
        }
        if !crate::field::Error::is_name_byte(byte) {
            return Outcome::Invalid {
                offset: pos,
                actual: byte,
            };
        }
    }
    Outcome::Partial
}

fn value_tail(bytes: &[u8], index: usize) -> Outcome<usize> {
    for (offset, &byte) in bytes.iter().enumerate() {
        if byte == b'\r' {
            return value_at(bytes, offset, index + offset, byte);
        }
        if !crate::field::Error::is_text(byte) {
            return Outcome::Invalid {
                offset: index + offset,
                actual: byte,
            };
        }
    }
    Outcome::Partial
}

/// Lowest byte one nonzero lane mask selects inside that lane.
///
/// A lane mask carries exactly `LANE` bits, so its lowest set bit already names
/// an in-lane index: the byte needs no second range check and no panic path.
fn in_lane(lane: &[u8; LANE], bits: u16) -> (usize, u8) {
    let offset = bits.trailing_zeros() as usize & (LANE - 1);
    (offset, lane[offset])
}

/// Byte-ordered bits of the four lane masks of one group.
fn packed(masks: [u16; 4]) -> u64 {
    let mut bits = 0;
    for (lane, mask) in masks.into_iter().enumerate() {
        bits |= u64::from(mask) << (lane * LANE);
    }
    bits
}

/// Lowest byte one nonzero group mask selects.
///
/// The mask carries one bit per byte, so its lowest set bit names the byte
/// without a second search or a panic path.
fn first(group: &[[u8; LANE]; 4], found: NonZero<u64>) -> (usize, u8) {
    let ordinal = found.get().trailing_zeros() as usize;
    (ordinal, group[ordinal / LANE][ordinal % LANE])
}

fn mask<const NAME: bool>(lane: [u8; LANE]) -> u16 {
    let flags = lane.map(|byte| {
        let invalid = if NAME {
            !crate::field::Error::is_name_byte(byte)
        } else {
            !crate::field::Error::is_text(byte)
        };
        u8::from(invalid) << 7
    });
    let mut bits = 0;
    // Each byte is 0 or 0x80; packing preserves byte order without cross-byte carries.
    for (index, chunk) in flags.as_chunks::<8>().0.iter().enumerate() {
        let word = u64::from_le_bytes(*chunk);
        bits |= ((word.wrapping_mul(0x0002_0408_1020_4081) >> 56) as u16) << (index * 8);
    }
    bits
}

fn name_at(pos: usize, byte: u8) -> Outcome<(usize, u8)> {
    if byte == b':' || byte == b'\r' {
        Outcome::Found((pos, byte))
    } else {
        Outcome::Invalid {
            offset: pos,
            actual: byte,
        }
    }
}

fn value_at(bytes: &[u8], offset: usize, pos: usize, byte: u8) -> Outcome<usize> {
    if byte != b'\r' {
        return Outcome::Invalid {
            offset: pos,
            actual: byte,
        };
    }
    let Some(next) = bytes.get(offset + 1) else {
        return Outcome::Partial;
    };
    if *next == b'\n' {
        Outcome::Found(pos)
    } else {
        Outcome::Invalid {
            offset: pos + 1,
            actual: *next,
        }
    }
}

const _: () = {
    let word = mem::size_of::<usize>();
    assert!(mem::size_of::<Bytes<'_>>() == 2 * word);
    assert!(mem::size_of::<Outcome<usize>>() == 2 * word);
    assert!(mem::size_of::<Outcome<(usize, u8)>>() == 3 * word);
    assert!(mem::size_of::<Match<'_>>() == 3 * word);
    assert!(mem::size_of::<Option<Match<'_>>>() == 3 * word);
};
