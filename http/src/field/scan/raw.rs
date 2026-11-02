use std::num::NonZero;

use crate::field::scan::Match;

/// # Safety
/// The index must be the actual forward-search result inside these unchanged bytes.
pub(super) unsafe fn found(bytes: &[u8], index: usize) -> Match<'_> {
    Match {
        bytes,
        // SAFETY: a slice position is below its isize-bounded length, so index + 1 is nonzero.
        end: unsafe { NonZero::new_unchecked(index + 1) },
    }
}

pub(super) fn byte(found: Match<'_>) -> u8 {
    // SAFETY: construction retains the actual matched position inside the immutable source.
    unsafe { *found.bytes.get_unchecked(found.start()) }
}

pub(super) fn before<const INCLUDE: bool>(found: Match<'_>) -> &[u8] {
    let end = found.end.get() - usize::from(!INCLUDE);
    // SAFETY: the retained matched byte proves both its start and exclusive end are in bounds.
    unsafe { found.bytes.get_unchecked(..end) }
}

pub(super) fn after<const INCLUDE: bool>(found: Match<'_>) -> &[u8] {
    let start = found.end.get() - usize::from(INCLUDE);
    // SAFETY: the retained match admits the suffix with or without its initial matched byte.
    unsafe { found.bytes.get_unchecked(start..) }
}
