use core::ptr;

use crate::field::metadata::map::record::Span;

#[derive(Clone, Copy)]
pub(in crate::payload::compress::dynamic) struct Ring<'a>(&'a [u8]);

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::payload::compress::dynamic) struct Cursor(u32);

impl Cursor {
    pub(in crate::payload::compress::dynamic) const ZERO: Self = Self(0);

    pub(in crate::payload::compress::dynamic) const fn index(self) -> usize {
        self.0 as usize
    }

    pub(in crate::payload::compress::dynamic) const fn into_u32(self) -> u32 {
        self.0
    }

    pub(in crate::payload::compress::dynamic) const fn distance(
        self,
        end: Self,
        capacity: u32,
    ) -> u32 {
        if end.0 >= self.0 {
            end.0 - self.0
        } else {
            capacity - self.0 + end.0
        }
    }
}

impl<'a> Ring<'a> {
    pub(in crate::payload::compress::dynamic) const fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub(in crate::payload::compress::dynamic) const fn len(self) -> usize {
        self.0.len()
    }

    pub(in crate::payload::compress::dynamic) fn span(
        self,
        start: Cursor,
        len: usize,
    ) -> Option<Span<'a>> {
        if len != 0 && (start.index() >= self.0.len() || len > self.0.len()) {
            return None;
        }
        Some(self.stored(start, len))
    }

    pub(in crate::payload::compress::dynamic) fn stored(
        self,
        start: Cursor,
        len: usize,
    ) -> Span<'a> {
        if len == 0 {
            return Span::from_slice(&[]);
        }
        let start = start.index();
        let first = len.min(self.0.len() - start);
        // SAFETY: span checks these bounds; other callers retain this ring's live
        // Slot/Size or a checked shrink of an admitted insertion. Nonempty spans
        // have start < arena length and len <= arena length, bounding both segments.
        unsafe {
            Span::new(
                self.0.get_unchecked(start..start + first),
                self.0.get_unchecked(..len - first),
            )
        }
    }

    pub(in crate::payload::compress::dynamic) const fn advance(
        self,
        start: Cursor,
        len: usize,
    ) -> Cursor {
        if self.0.is_empty() {
            return Cursor::ZERO;
        }
        let start = start.index();
        let tail = self.0.len() - start;
        let next = if len >= tail { len - tail } else { start + len };
        Cursor(next as u32)
    }
}

const _: () = assert!(core::mem::size_of::<Ring<'_>>() == 2 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Cursor>() == core::mem::size_of::<u32>());

pub(in crate::payload::compress::dynamic) struct Buf<'a>(&'a mut [u8]);

impl<'a> Buf<'a> {
    pub(in crate::payload::compress::dynamic) const fn new(bytes: &'a mut [u8]) -> Self {
        Self(bytes)
    }

    pub(in crate::payload::compress::dynamic) fn write(
        &mut self,
        start: Cursor,
        bytes: &[u8],
    ) -> Cursor {
        if bytes.is_empty() {
            return start;
        }
        let first = bytes.len().min(self.0.len() - start.index());
        // SAFETY: callers hold the staged-entry size proof. It guarantees a
        // live ring cursor and at most one arena's bytes; `copy` permits the
        // source to be another span of this same allocation.
        unsafe {
            ptr::copy(
                bytes.as_ptr(),
                self.0.as_mut_ptr().add(start.index()),
                first,
            );
            ptr::copy(
                bytes.as_ptr().add(first),
                self.0.as_mut_ptr(),
                bytes.len() - first,
            );
        }
        Ring::new(self.0).advance(start, bytes.len())
    }

    pub(in crate::payload::compress::dynamic) fn copy(
        &mut self,
        source: Cursor,
        target: Cursor,
        len: usize,
    ) -> Cursor {
        if len == 0 {
            return target;
        }
        let mut source = source;
        let mut target = target;
        let mut left = len;
        while left != 0 {
            let chunk = left
                .min(self.0.len() - source.index())
                .min(self.0.len() - target.index());
            // SAFETY: the source slot and target draft are both owner-bound
            // cursors, and the transfer plan bounds `len` by this arena.
            unsafe {
                ptr::copy(
                    self.0.as_ptr().add(source.index()),
                    self.0.as_mut_ptr().add(target.index()),
                    chunk,
                )
            };
            left -= chunk;
            source = Ring::new(self.0).advance(source, chunk);
            target = Ring::new(self.0).advance(target, chunk);
        }
        target
    }
}

const _: () = assert!(core::mem::size_of::<Buf<'_>>() == 2 * core::mem::size_of::<usize>());
