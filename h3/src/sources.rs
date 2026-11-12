use std::{array, ops::ControlFlow};

use sark_http::field::metadata::integer;

/// Cloneable physical traversal behind one logical byte source. Cloning is a
/// cursor snapshot (indices and borrows only); it never retains payload
/// owners.
pub(crate) trait Chunks<'a>: Clone + Iterator<Item = &'a [u8]> {}

impl<'a, I> Chunks<'a> for I where I: Clone + Iterator<Item = &'a [u8]> {}

pub(crate) enum Form<'a> {
    Contiguous(&'a [u8]),
    Segmented,
}

/// Monotonic logical cursor. Every owner boundary is crossed at most once;
/// snapshots used by literals contain only the physical iterator state.
#[derive(Clone)]
pub(crate) struct Cursor<'a, I: Chunks<'a>> {
    chunks: I,
    current: &'a [u8],
    remaining: usize,
}

impl<'a, I: Chunks<'a>> Cursor<'a, I> {
    fn new(chunks: I, left: usize) -> Self {
        Self {
            chunks,
            current: &[],
            remaining: left,
        }
    }

    fn fill(&mut self) {
        while self.current.is_empty() && self.remaining != 0 {
            let Some(chunk) = self.chunks.next() else {
                self.remaining = 0;
                return;
            };
            self.current = chunk;
        }
    }

    pub(crate) const fn remaining(&self) -> usize {
        self.remaining
    }

    pub(crate) fn peek(&mut self) -> Option<u8> {
        self.fill();
        self.current.first().copied()
    }

    fn advance(&mut self, mut amount: usize) -> usize {
        let total = amount;
        if amount > self.remaining {
            return 0;
        }
        while amount != 0 {
            self.fill();
            if self.current.is_empty() {
                return total - amount;
            }
            let consumed = amount.min(self.current.len());
            let (_, tail) = self.current.split_at(consumed);
            self.current = tail;
            self.remaining -= consumed;
            amount -= consumed;
        }
        total
    }

    pub(crate) fn copy_into(&mut self, mut output: &mut [u8]) -> usize {
        let required = output.len();
        if required > self.remaining {
            return 0;
        }
        while !output.is_empty() {
            self.fill();
            if self.current.is_empty() {
                return required - output.len();
            }
            let copied = output.len().min(self.current.len());
            let (target, rest) = output.split_at_mut(copied);
            let (source, tail) = self.current.split_at(copied);
            target.copy_from_slice(source);
            output = rest;
            self.current = tail;
            self.remaining -= copied;
        }
        required
    }

    pub(crate) fn take(&mut self, len: usize) -> Option<Span<'a, I>> {
        if len > self.remaining {
            return None;
        }
        let cursor = self.clone();
        if self.advance(len) != len {
            return None;
        }
        Some(Span { cursor, len })
    }

    pub(crate) fn into_span(self, len: usize) -> Option<Span<'a, I>> {
        if len > self.remaining {
            return None;
        }
        Some(Span { cursor: self, len })
    }

    pub(crate) fn prefixed<const BITS: u8>(
        &mut self,
    ) -> Result<integer::PrefixedInt<BITS>, integer::Error>
    where
        integer::PrefixedInt<BITS>: integer::Width,
    {
        let Some(first) = self.peek() else {
            return Err(integer::Error::NeedMore);
        };
        self.advance(1);
        let max = <integer::PrefixedInt<BITS> as integer::Width>::MAX_PREFIX;
        let prefix = u64::from(first & max as u8);
        if prefix < max {
            return Ok(integer::PrefixedInt::new(prefix));
        }

        let mut number = integer::Continuation::new(max);
        loop {
            let Some(byte) = self.peek() else {
                return Err(integer::Error::NeedMore);
            };
            self.advance(1);
            if let ControlFlow::Break(value) = number.try_push(byte)? {
                return Ok(integer::PrefixedInt::new(value));
            }
        }
    }
}

/// Reusable snapshot of a bounded logical range.
#[derive(Clone)]
pub(crate) struct Span<'a, I: Chunks<'a>> {
    cursor: Cursor<'a, I>,
    len: usize,
}

impl<'a, I: Chunks<'a>> Span<'a, I> {
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn form(&self) -> Form<'a> {
        let mut cursor = self.cursor.clone();
        cursor.fill();
        match cursor.current.get(..self.len) {
            Some(bytes) => Form::Contiguous(bytes),
            None => Form::Segmented,
        }
    }

    #[inline(always)]
    pub(crate) fn as_slices(&self) -> Option<(&'a [u8], &'a [u8])> {
        let mut cursor = self.cursor.clone();
        cursor.fill();
        let first = cursor.current;
        if let Some(bytes) = first.get(..self.len) {
            return Some((bytes, &[]));
        }
        cursor.current = &[];
        cursor.remaining -= first.len();
        cursor.fill();
        Some((first, cursor.current.get(..self.len - first.len())?))
    }

    pub(crate) fn chunks(&self) -> Bounded<'a, I> {
        let mut cursor = self.cursor.clone();
        cursor.remaining = self.len;
        Bounded { cursor }
    }

    /// Retains the original span's admission proof for its complete immutable suffix.
    pub(crate) fn suffix(self, offset: usize) -> Option<Self> {
        let len = self.len.checked_sub(offset)?;
        let mut cursor = self.cursor;
        (cursor.advance(offset) == offset).then_some(())?;
        Some(Self { cursor, len })
    }
}

#[derive(Clone)]
pub(crate) struct Bounded<'a, I: Chunks<'a>> {
    cursor: Cursor<'a, I>,
}

impl<'a, I: Chunks<'a>> Iterator for Bounded<'a, I> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.remaining == 0 {
            return None;
        }
        self.cursor.fill();
        if self.cursor.current.is_empty() {
            return None;
        }
        let len = self.cursor.remaining.min(self.cursor.current.len());
        let (chunk, tail) = self.cursor.current.split_at(len);
        self.cursor.current = tail;
        self.cursor.remaining -= len;
        Some(chunk)
    }
}

/// Immutable logical bytes whose `len` exactly matches the bytes yielded by `chunks`.
/// The source borrow preserves both; payload and literal ranges are visited in place.
pub(crate) trait Source {
    type Chunks<'a>: Chunks<'a>
    where
        Self: 'a;

    fn len(&self) -> usize;

    fn chunks(&self) -> Self::Chunks<'_>;

    fn form(&self) -> Form<'_> {
        Form::Segmented
    }

    fn cursor(&self) -> Cursor<'_, Self::Chunks<'_>> {
        Cursor::new(self.chunks(), self.len())
    }
}

impl Source for [u8] {
    type Chunks<'a> = array::IntoIter<&'a [u8], 1>;

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }

    fn chunks(&self) -> Self::Chunks<'_> {
        [self].into_iter()
    }

    fn form(&self) -> Form<'_> {
        Form::Contiguous(self)
    }
}
