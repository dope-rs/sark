/// # Safety
/// The retained gap and final count must satisfy `gap <= count < words.len()`.
pub(super) unsafe fn expand(words: &mut [u16; 8], gap: usize, count: usize) {
    let omitted = words.len() - count;
    let out = words.as_mut_ptr();
    // SAFETY: the admitted tail ends exactly at eight after the overlapping move;
    // the preceding gap is inside the same exclusively borrowed array.
    unsafe {
        out.add(gap + omitted).copy_from(out.add(gap), count - gap);
        out.add(gap).write_bytes(0, omitted);
    }
}
