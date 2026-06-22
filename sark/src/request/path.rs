use std::ops::Range;

use sark_core::http::LocalFrameBytes;

use super::ChunkerCore;
use super::chain::{SplitFrameChain, SplitFrameIter};

trait AsciiInt: Copy {
    fn parse_ascii(bytes: &[u8]) -> Option<Self>;
    fn fold_ascii(acc: Self, chunk: &[u8]) -> Option<Self>;
    fn zero() -> Self;
}

impl AsciiInt for usize {
    fn parse_ascii(bytes: &[u8]) -> Option<Self> {
        Ascii::parse_usize(bytes)
    }
    fn fold_ascii(acc: Self, chunk: &[u8]) -> Option<Self> {
        Ascii::fold_usize(acc, chunk)
    }
    fn zero() -> Self {
        0
    }
}

impl AsciiInt for u64 {
    fn parse_ascii(bytes: &[u8]) -> Option<Self> {
        Ascii::parse_u64(bytes)
    }
    fn fold_ascii(acc: Self, chunk: &[u8]) -> Option<Self> {
        Ascii::fold_u64(acc, chunk)
    }
    fn zero() -> Self {
        0
    }
}

trait BytesCmp {
    fn eq(a: &[u8], b: &[u8]) -> bool;
}

struct ExactEq;
impl BytesCmp for ExactEq {
    fn eq(a: &[u8], b: &[u8]) -> bool {
        a == b
    }
}

struct AsciiCaseEq;
impl BytesCmp for AsciiCaseEq {
    fn eq(a: &[u8], b: &[u8]) -> bool {
        a.eq_ignore_ascii_case(b)
    }
}
use sark_core::utils::bytes::Ascii;

use crate::routes::path::seg_next;

#[allow(private_interfaces)]
#[derive(Clone, Copy)]
pub enum PathView<'a> {
    Slice(&'a [u8]),
    Local {
        frame: &'a LocalFrameBytes,
        start: usize,
        end: usize,
    },
    Chain {
        chain: &'a SplitFrameChain,
        start: usize,
        end: usize,
    },
}

impl<'a> PathView<'a> {
    pub(crate) fn len(self) -> usize {
        match self {
            Self::Slice(path) => path.len(),
            Self::Local { start, end, .. } => end - start,
            Self::Chain { start, end, .. } => end - start,
        }
    }

    pub fn as_slice(self) -> Option<&'a [u8]> {
        match self {
            Self::Slice(path) => Some(path),
            Self::Local {
                frame, start, end, ..
            } => Some(&frame.as_bytes()[start..end]),
            Self::Chain { chain, start, end } => chain.direct_bytes_range(start..end),
        }
    }

    pub fn to_vec(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len());
        self.append_to(&mut out);
        out
    }

    pub(crate) fn copy_range_local(self, start: usize, end: usize) -> Option<LocalFrameBytes> {
        if end < start || end > self.len() {
            return None;
        }
        match self {
            Self::Slice(path) => Some(LocalFrameBytes::from_shared(
                o3::buffer::Shared::copy_from_slice(&path[start..end]),
            )),
            Self::Local {
                frame, start: base, ..
            } => Some(frame.clone().slice((base + start)..(base + end))),
            Self::Chain {
                chain, start: base, ..
            } => Some(chain.compact().clone().slice((base + start)..(base + end))),
        }
    }

    pub(crate) fn append_to(self, out: &mut impl o3::buffer::Write) {
        match self {
            Self::Slice(path) => out.put_slice(path),
            Self::Local {
                frame, start, end, ..
            } => out.put_slice(&frame.as_bytes()[start..end]),
            Self::Chain { chain, start, end } => {
                let _ = chain.for_each_range(start..end, |chunk| out.put_slice(chunk));
            }
        }
    }

    pub(crate) fn parse_u64(self) -> Option<u64> {
        self.parse_u64_range(0, self.len())
    }

    fn parse_int_range<T: AsciiInt>(self, start: usize, end: usize) -> Option<T> {
        if end < start || end > self.len() || start == end {
            return None;
        }
        match self {
            Self::Slice(path) => T::parse_ascii(&path[start..end]),
            Self::Local {
                frame, start: base, ..
            } => T::parse_ascii(&frame.as_bytes()[(base + start)..(base + end)]),
            Self::Chain {
                chain, start: base, ..
            } => {
                let mut acc = Some(T::zero());
                let _ = chain.for_each_range((base + start)..(base + end), |chunk| {
                    acc = acc.and_then(|v| T::fold_ascii(v, chunk));
                });
                acc
            }
        }
    }

    pub(crate) fn parse_usize_range(self, start: usize, end: usize) -> Option<usize> {
        self.parse_int_range::<usize>(start, end)
    }

    pub(crate) fn parse_u64_range(self, start: usize, end: usize) -> Option<u64> {
        self.parse_int_range::<u64>(start, end)
    }

    pub(crate) fn eq_bytes(self, expected: &[u8]) -> bool {
        self.eq_range(0, self.len(), expected)
    }

    fn eq_range_with<C: BytesCmp>(self, start: usize, end: usize, expected: &[u8]) -> bool {
        if end < start || end > self.len() || expected.len() != end - start {
            return false;
        }
        match self {
            Self::Slice(path) => C::eq(&path[start..end], expected),
            Self::Local {
                frame, start: base, ..
            } => C::eq(&frame.as_bytes()[(base + start)..(base + end)], expected),
            Self::Chain {
                chain, start: base, ..
            } => {
                let mut matched = 0usize;
                let mut ok = true;
                let _ = chain.for_each_range((base + start)..(base + end), |chunk| {
                    if !ok {
                        return;
                    }
                    let next = matched + chunk.len();
                    if !C::eq(chunk, &expected[matched..next]) {
                        ok = false;
                        return;
                    }
                    matched = next;
                });
                ok && matched == expected.len()
            }
        }
    }

    pub(crate) fn eq_range(self, start: usize, end: usize, expected: &[u8]) -> bool {
        self.eq_range_with::<ExactEq>(start, end, expected)
    }

    pub(crate) fn eq_range_ignore_ascii_case(
        self,
        start: usize,
        end: usize,
        expected: &[u8],
    ) -> bool {
        self.eq_range_with::<AsciiCaseEq>(start, end, expected)
    }

    pub(crate) fn next_seg(self, idx: usize) -> Option<(usize, usize, usize)> {
        if idx >= self.len() {
            return None;
        }
        match self {
            Self::Slice(path) => seg_next(path, idx),
            Self::Local { frame, start, end } => seg_next(&frame.as_bytes()[start..end], idx),
            Self::Chain { chain, start, end } => {
                let mut pos = 0usize;
                let mut seg_start = None;
                let mut seg_end = None;
                let mut done = false;
                let _ = chain.for_each_range(start..end, |chunk| {
                    if done {
                        return;
                    }
                    let mut i = 0usize;
                    while i < chunk.len() {
                        let abs = pos + i;
                        let b = chunk[i];
                        if seg_start.is_none() {
                            if abs < idx || b == b'/' {
                                i += 1;
                                continue;
                            }
                            seg_start = Some(abs);
                        }
                        if b == b'/' {
                            seg_end = Some(abs);
                            done = true;
                            break;
                        }
                        i += 1;
                    }
                    pos += chunk.len();
                });
                let start = seg_start?;
                let end = seg_end.unwrap_or(self.len());
                Some((start, end, end))
            }
        }
    }
}

pub(crate) type BodyChunks<'a> = BodyChainIter<'a>;

pub struct BodyChainIter<'a> {
    frames: SplitFrameIter<'a>,
    core: ChunkerCore,
}

impl<'a> BodyChainIter<'a> {
    pub(super) fn new(chain: &'a SplitFrameChain, range: Range<usize>) -> Self {
        assert!(range.start <= range.end, "invalid body chunk range");
        assert!(
            range.end <= chain.len(),
            "body chunk range exceeds split-chain length"
        );
        Self {
            frames: chain.iter_frames(),
            core: ChunkerCore {
                skip: range.start,
                remaining: range.end - range.start,
            },
        }
    }
}

impl<'a> Iterator for BodyChainIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        while self.core.remaining != 0 {
            let bytes = self.frames.next()?.as_bytes();
            if let Some(out) = self.core.step(bytes) {
                return Some(out);
            }
        }
        None
    }
}
