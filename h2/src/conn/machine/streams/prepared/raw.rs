use crate::frame::layout;

/// # Safety
/// `length` must not exceed the byte sum of these unchanged source slices.
pub(super) unsafe fn parts(source: layout::Parts<'_>, length: usize) -> (&[u8], &[u8]) {
    let split = length.min(source.first.len());
    // SAFETY: the admitted sum covers length; split lies in first and the
    // remainder lies in second. Both output borrows retain the source lifetime.
    unsafe {
        (
            source.first.get_unchecked(..split),
            source.second.get_unchecked(..length - split),
        )
    }
}
