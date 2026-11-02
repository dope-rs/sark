use std::{mem, num::NonZero, ops, ptr, str};

use crate::h1::codec::numbers::Wire as _;

/// # Safety
/// The string's length must equal its capacity, retaining its exact global layout.
pub(super) unsafe fn boxed(value: String) -> Box<str> {
    let mut value = mem::ManuallyDrop::new(value);
    let bytes = ptr::slice_from_raw_parts_mut(value.as_mut_ptr(), value.len());
    // SAFETY: the consumed String owns valid UTF-8 in exactly this many bytes;
    // its u8 allocation and Box<str> share the global allocator and layout.
    unsafe { Box::from_raw(bytes as *mut str) }
}

/// # Safety
/// The range must be inside this unchanged URI owner, on admitted UTF-8 boundaries.
pub(super) unsafe fn slice(value: &str, range: ops::Range<usize>) -> &str {
    // SAFETY: the caller carries the exact owner's admitted component boundaries.
    unsafe { value.get_unchecked(range) }
}

pub(super) fn decimal(port: NonZero<u16>, digits: &mut [u8; 20]) -> &str {
    let bytes = usize::from(port.get()).decimal(digits);
    // SAFETY: Wire::decimal emits only initialized ASCII decimal digits.
    unsafe { str::from_utf8_unchecked(bytes) }
}

/// # Safety
/// The offset must be an admitted UTF-8 boundary; capacity must cover the resulting bytes.
/// The text must not overlap the destination allocation.
pub(super) unsafe fn insert(value: &mut String, at: usize, text: &str) {
    // SAFETY: admission proves capacity and the UTF-8 split; the overlapping move
    // opens the exact gap before the disjoint text fills it.
    unsafe {
        let bytes = value.as_mut_vec();
        let len = bytes.len();
        let out = bytes.as_mut_ptr();
        out.add(at + text.len()).copy_from(out.add(at), len - at);
        out.add(at)
            .copy_from_nonoverlapping(text.as_ptr(), text.len());
        bytes.set_len(len + text.len());
    }
}
