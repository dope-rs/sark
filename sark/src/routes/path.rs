pub(crate) fn seg_next(path: &[u8], idx: usize) -> Option<(usize, usize, usize)> {
    if idx >= path.len() {
        return None;
    }
    let start = idx + 1;
    let mut end = start;
    while end < path.len() && path[end] != b'/' {
        end += 1;
    }
    Some((start, end, end))
}
