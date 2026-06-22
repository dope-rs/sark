use std::cell::OnceCell;
use std::ops::Range;
use std::slice;

use o3::buffer::Shared;
use sark_core::http::LocalFrameBytes;

use super::path::PathView;

#[derive(Clone)]
enum Frames {
    Empty,
    One(LocalFrameBytes),
    Two(LocalFrameBytes, LocalFrameBytes),
    Many(Vec<LocalFrameBytes>),
}

pub(crate) struct SplitFrameChain {
    total_len: usize,
    frames: Frames,
    compact: OnceCell<Box<LocalFrameBytes>>,
}

impl Clone for SplitFrameChain {
    fn clone(&self) -> Self {
        Self {
            total_len: self.total_len,
            frames: self.frames.clone(),
            compact: OnceCell::new(),
        }
    }
}

impl SplitFrameChain {
    pub(super) fn for_each_range(&self, range: Range<usize>, mut f: impl FnMut(&[u8])) -> bool {
        if range.start > range.end || range.end > self.total_len {
            return false;
        }
        if range.start == range.end {
            return true;
        }
        let mut base = 0usize;
        let mut started = false;
        let mut end_seen = false;
        for frame in self.iter_frames() {
            let bytes = frame.as_bytes();
            let frame_len = bytes.len();
            let frame_end = base + frame_len;
            if !started {
                if range.start >= frame_end {
                    base = frame_end;
                    continue;
                }
                started = true;
            }
            let start = range.start.saturating_sub(base);
            let end = if range.end < frame_end {
                end_seen = true;
                range.end - base
            } else {
                frame_len
            };
            if start < end {
                f(&bytes[start..end]);
            }
            if end_seen {
                return true;
            }
            base = frame_end;
        }
        false
    }

    fn local_frame(&self, range: Range<usize>) -> Option<(&LocalFrameBytes, usize, usize)> {
        if range.start > range.end || range.end > self.total_len {
            return None;
        }
        let mut base = 0usize;
        for frame in self.iter_frames() {
            let frame_len = frame.len();
            let frame_end = base + frame_len;
            if range.start >= frame_end {
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
        None
    }

    pub(crate) fn new() -> Self {
        Self {
            total_len: 0,
            frames: Frames::Empty,
            compact: OnceCell::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.total_len
    }

    pub(crate) fn push(&mut self, frame: LocalFrameBytes) {
        self.total_len = self.total_len.saturating_add(frame.len());
        let _ = self.compact.take();
        self.frames = match std::mem::replace(&mut self.frames, Frames::Empty) {
            Frames::Empty => Frames::One(frame),
            Frames::One(first) => Frames::Two(first, frame),
            Frames::Two(first, second) => Frames::Many(vec![first, second, frame]),
            Frames::Many(mut heap) => {
                heap.push(frame);
                Frames::Many(heap)
            }
        };
    }

    pub(crate) fn iter_frames(&self) -> SplitFrameIter<'_> {
        let inner = match &self.frames {
            Frames::Empty => SplitFrameIterInner::Empty,
            Frames::One(first) => SplitFrameIterInner::One(Some(first)),
            Frames::Two(first, second) => SplitFrameIterInner::Two {
                first: Some(first),
                second: Some(second),
            },
            Frames::Many(heap) => SplitFrameIterInner::Many(heap.iter()),
        };
        SplitFrameIter { inner }
    }

    pub(super) fn compact(&self) -> &LocalFrameBytes {
        self.compact.get_or_init(|| {
            let mut out = vec![0u8; self.total_len];
            let mut written = 0usize;
            for frame in self.iter_frames() {
                let bytes = frame.as_bytes();
                let end = written + bytes.len();
                out[written..end].copy_from_slice(bytes);
                written = end;
            }
            assert!(
                written == self.total_len,
                "split frame compact invariant: written bytes must match total length"
            );
            Box::new(LocalFrameBytes::from_shared(Shared::from(out)))
        })
    }

    pub(super) fn direct_bytes_range(&self, range: Range<usize>) -> Option<&[u8]> {
        if range.start > range.end || range.end > self.total_len {
            return None;
        }
        if range.start == range.end {
            return Some(&[]);
        }
        self.local_frame(range)
            .map(|(frame, start, end)| &frame.as_bytes()[start..end])
    }

    pub(super) fn local_direct(&self, range: Range<usize>) -> Option<LocalFrameBytes> {
        if range.start > range.end || range.end > self.total_len {
            return None;
        }
        if range.start == range.end {
            return Some(LocalFrameBytes::from_shared(Shared::new()));
        }
        self.local_frame(range.clone())
            .map(|(frame, start, end)| frame.clone().slice(start..end))
    }

    pub(super) fn bytes_range(&self, range: Range<usize>) -> Option<&[u8]> {
        if range.start > range.end || range.end > self.total_len {
            return None;
        }
        if range.start == range.end {
            return Some(&[]);
        }
        let mut base = 0usize;
        for frame in self.iter_frames() {
            let bytes = frame.as_bytes();
            let frame_len = bytes.len();
            if range.start >= base + frame_len {
                base += frame_len;
                continue;
            }
            let start = range.start - base;
            let need = range.end - range.start;
            if start + need <= frame_len {
                return Some(&bytes[start..start + need]);
            }
            let compact = self.compact().as_bytes();
            return Some(&compact[range.start..range.end]);
        }
        None
    }

    pub(super) fn path_view(&self, range: Range<usize>) -> PathView<'_> {
        if let Some((frame, start, end)) = self.local_frame(range.clone()) {
            return PathView::Local { frame, start, end };
        }
        PathView::Chain {
            chain: self,
            start: range.start,
            end: range.end,
        }
    }
}

impl AsRef<[u8]> for SplitFrameChain {
    fn as_ref(&self) -> &[u8] {
        self.compact().as_bytes()
    }
}

pub(crate) struct SplitFrameIter<'a> {
    inner: SplitFrameIterInner<'a>,
}

enum SplitFrameIterInner<'a> {
    Empty,
    One(Option<&'a LocalFrameBytes>),
    Two {
        first: Option<&'a LocalFrameBytes>,
        second: Option<&'a LocalFrameBytes>,
    },
    Many(slice::Iter<'a, LocalFrameBytes>),
}

impl<'a> Iterator for SplitFrameIter<'a> {
    type Item = &'a LocalFrameBytes;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            SplitFrameIterInner::Empty => None,
            SplitFrameIterInner::One(slot) => slot.take(),
            SplitFrameIterInner::Two { first, second } => {
                if let Some(out) = first.take() {
                    return Some(out);
                }
                second.take()
            }
            SplitFrameIterInner::Many(iter) => iter.next(),
        }
    }
}
