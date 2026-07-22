use std::ops::Range;

use o3::buffer::{Borrowed, Bytes};

mod storage;

#[doc(hidden)]
pub use storage::RequestStorage;

#[derive(Clone, Copy)]
pub struct BodyLen {
    len: usize,
}

impl BodyLen {
    pub const fn from_declared(len: usize) -> Self {
        Self { len }
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

fn uri_path_end(frame: &[u8], uri_range: &Range<usize>) -> usize {
    match frame.get(uri_range.clone()) {
        Some(segment) => segment
            .iter()
            .position(|byte| *byte == b'?')
            .map(|offset| uri_range.start + offset)
            .unwrap_or(uri_range.end),
        None => uri_range.end,
    }
}

pub struct Ref<'req> {
    head: &'req [u8],
    body: &'req [u8],
    uri_start: usize,
    uri_path_end: usize,
    declared_body_len: usize,
}

impl<'req> Ref<'req> {
    pub fn from_slice(uri_range: Range<usize>, head: &'req [u8], body: &'req [u8]) -> Self {
        let uri_path_end = uri_path_end(head, &uri_range);
        debug_assert!(
            uri_range.start <= uri_path_end && uri_path_end <= uri_range.end,
            "uri_path_end must be inside uri_range"
        );
        Self {
            head,
            body,
            uri_start: uri_range.start,
            uri_path_end,
            declared_body_len: body.len(),
        }
    }

    fn path_abs(&self, range: &Range<usize>) -> Option<Range<usize>> {
        let path_len = self.uri_path_end.saturating_sub(self.uri_start);
        if range.start > range.end || range.end > path_len {
            return None;
        }
        Some((self.uri_start + range.start)..(self.uri_start + range.end))
    }

    pub fn path_frame(&self, range: Range<usize>) -> Option<Bytes<Borrowed<'req>>> {
        let absolute = self.path_abs(&range)?;
        if absolute.end > self.head.len() {
            return None;
        }
        Some(Bytes::<Borrowed<'req>>::from(self.head).slice(absolute))
    }

    pub fn frame_at(&self, range: Range<usize>) -> Option<Bytes<Borrowed<'req>>> {
        if range.start > range.end || range.end > self.head.len() {
            return None;
        }
        Some(Bytes::<Borrowed<'req>>::from(self.head).slice(range))
    }

    pub fn body_frame(&self) -> Bytes<Borrowed<'req>> {
        Bytes::<Borrowed<'req>>::from(self.body)
    }

    pub fn declared_body_len(&self) -> usize {
        self.declared_body_len
    }

    pub fn set_declared_body_len(&mut self, len: usize) {
        self.declared_body_len = len;
    }
}

#[cfg(test)]
mod tests {
    use super::Ref;

    #[test]
    fn request_ref_is_exactly_two_slices_and_three_words() {
        assert_eq!(
            size_of::<Ref<'_>>(),
            7 * size_of::<usize>(),
            "request views must not regain frame-chain or typestate storage",
        );
    }
}
