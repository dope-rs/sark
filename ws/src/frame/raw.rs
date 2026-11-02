use crate::frame::{Frame, Writer};

/// Writes a frame into an admitted destination.
/// # Safety
/// `dst.len()` must cover this exact retained frame's wire length.
pub(crate) unsafe fn write(frame: Frame<'_>, dst: &mut [u8]) {
    Writer { frame, dst }.write();
}
