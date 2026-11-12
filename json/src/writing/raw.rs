use std::{mem::MaybeUninit, slice};

pub(super) const PAIRS: [[u8; 2]; 100] = {
    let mut pairs = [[0; 2]; 100];
    let mut value = 0;
    while value < pairs.len() {
        pairs[value] = [b'0' + (value / 10) as u8, b'0' + (value % 10) as u8];
        value += 1;
    }
    pairs
};

pub(super) fn write(mut value: u64, bytes: &mut [MaybeUninit<[u8; 2]>; 10]) -> &[u8] {
    let mut start = bytes.len();
    while value >= 100 {
        start -= 1;
        bytes[start].write(PAIRS[(value % 100) as usize]);
        value /= 100;
    }
    start -= 1;
    bytes[start].write(PAIRS[value as usize]);
    let bytes = &bytes[start..];
    // SAFETY: a u64 needs at most ten pairs; each suffix pair was fully written.
    // The byte slice borrows only that initialized suffix of this same storage.
    let bytes = unsafe { slice::from_raw_parts(bytes.as_ptr().cast(), bytes.len() * 2) };
    &bytes[usize::from(value < 10)..]
}
