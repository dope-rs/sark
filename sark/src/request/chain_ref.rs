use std::ops::Range;
use std::slice;

use sark_core::http::LocalFrameBytesRef;

use super::path::PathView;

const INLINE_FRAMES: usize = 1;

pub(crate) struct SplitFrameChainRef<'req> {
    total_len: usize,
    frames: [LocalFrameBytesRef<'req>; INLINE_FRAMES],
    len: u8,
}

impl<'req> SplitFrameChainRef<'req> {
    pub(crate) fn new() -> Self {
        Self {
            total_len: 0,
            frames: [LocalFrameBytesRef::from_slice(&[])],
            len: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.total_len
    }

    pub(crate) fn push(&mut self, frame: LocalFrameBytesRef<'req>) {
        assert!(
            (self.len as usize) < INLINE_FRAMES,
            "SplitFrameChainRef: exceeded {INLINE_FRAMES} inline frames"
        );
        self.total_len = self.total_len.saturating_add(frame.len());
        self.frames[self.len as usize] = frame;
        self.len += 1;
    }

    fn local_frame(
        &self,
        range: Range<usize>,
    ) -> Option<(&LocalFrameBytesRef<'req>, usize, usize)> {
        if range.start > range.end || range.end > self.total_len {
            return None;
        }
        let mut base = 0usize;
        let active = &self.frames[..self.len as usize];
        for frame in active {
            let frame_len = frame.len();
            let frame_end = base + frame_len;
            if range.start >= frame_end && frame_len != 0 {
                base = frame_end;
                continue;
            }
            if range.end <= frame_end {
                let start = range.start - base;
                let end = range.end - base;
                return Some((frame, start, end));
            }
            return None;
        }
        if range.start == range.end && range.start == self.total_len {
            return active.last().map(|f| (f, f.len(), f.len()));
        }
        None
    }

    pub(crate) fn bytes_range(&self, range: Range<usize>) -> Option<&[u8]> {
        if range.start > range.end || range.end > self.total_len {
            return None;
        }
        if range.start == range.end {
            return Some(&[]);
        }
        let (frame, start, end) = self.local_frame(range)?;
        Some(&frame.as_bytes()[start..end])
    }

    pub(crate) fn local_direct(&self, range: Range<usize>) -> Option<LocalFrameBytesRef<'req>> {
        if range.start > range.end || range.end > self.total_len {
            return None;
        }
        if range.start == range.end {
            return Some(LocalFrameBytesRef::from_slice(&[]));
        }
        self.local_frame(range)
            .map(|(frame, start, end)| frame.clone().slice(start..end))
    }

    pub(crate) fn path_view(&self, range: Range<usize>) -> PathView<'_> {
        match self.bytes_range(range) {
            Some(slice) => PathView::Slice(slice),
            None => PathView::Slice(&[]),
        }
    }

    pub(crate) fn iter_frames(&self) -> slice::Iter<'_, LocalFrameBytesRef<'req>> {
        self.frames[..self.len as usize].iter()
    }
}
