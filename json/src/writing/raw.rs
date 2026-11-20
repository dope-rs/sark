use std::{mem::MaybeUninit, slice};

use sark_http::h1::codec::numbers::raw::PAIRS;

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
