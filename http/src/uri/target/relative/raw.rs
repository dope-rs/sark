use o3::alloc::{self, array};

/// # Safety
/// The segments and one preceding slash each must emit exactly `path_len` bytes.
/// The plan extent must equal the prefix, measured path, and query lengths combined.
pub(super) unsafe fn normalize<'a>(
    plan: array::Plan<u8, usize>,
    prefix: &str,
    path_len: usize,
    segments: impl Iterator<Item = &'a str>,
    query: &str,
) -> Result<String, alloc::Exhausted> {
    let len = *plan.extent();
    let mut bytes = plan.try_vec()?;
    let out = bytes.as_mut_ptr();
    let mut end = prefix.len() + path_len;
    // SAFETY: this same measured iterator emits each borrowed segment and preceding
    // slash into its exact range; prefix and query fill the disjoint remainder.
    // All copied components are UTF-8 and inserted separators are ASCII.
    unsafe {
        out.copy_from_nonoverlapping(prefix.as_ptr(), prefix.len());
        out.add(end)
            .copy_from_nonoverlapping(query.as_ptr(), query.len());
        for segment in segments {
            end -= segment.len();
            out.add(end)
                .copy_from_nonoverlapping(segment.as_ptr(), segment.len());
            end -= 1;
            out.add(end).write(b'/');
        }
        bytes.set_len(len);
        Ok(String::from_utf8_unchecked(bytes))
    }
}
