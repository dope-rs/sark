//! Borrowed metadata records and match results.

use core::ops;

/// Borrowed bytes held in one or two physical segments.
#[derive(Clone, Copy)]
pub struct Span<'a> {
    first: &'a [u8],
    second: &'a [u8],
}

impl<'a> Span<'a> {
    pub const fn new(first: &'a [u8], second: &'a [u8]) -> Self {
        Self { first, second }
    }

    pub const fn from_slice(bytes: &'a [u8]) -> Self {
        Self::new(bytes, &[])
    }

    pub const fn len(self) -> usize {
        self.first.len() + self.second.len()
    }

    pub const fn is_empty(self) -> bool {
        self.first.is_empty() && self.second.is_empty()
    }

    pub const fn parts(self) -> (&'a [u8], &'a [u8]) {
        (self.first, self.second)
    }

    pub const fn as_slice(&self) -> Option<&'a [u8]> {
        if self.second.is_empty() {
            Some(self.first)
        } else {
            None
        }
    }

    pub fn get(self, index: usize) -> Option<u8> {
        if index < self.first.len() {
            self.first.get(index).copied()
        } else {
            self.second.get(index - self.first.len()).copied()
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &'a u8> + Clone {
        self.first.iter().chain(self.second)
    }

    pub fn slice(self, range: ops::Range<usize>) -> Option<Self> {
        let split = self.first.len();
        if range.end <= split {
            return self.first.get(range).map(Self::from_slice);
        }
        if range.start >= split {
            return self
                .second
                .get(range.start - split..range.end - split)
                .map(Self::from_slice);
        }
        Some(Self::new(
            self.first.get(range.start..)?,
            self.second.get(..range.end - split)?,
        ))
    }

    pub fn is_equal(self, bytes: &[u8]) -> bool {
        let split = self.first.len();
        bytes.len() == self.len()
            && bytes.get(..split) == Some(self.first)
            && bytes.get(split..) == Some(self.second)
    }

    pub fn starts_with(self, prefix: &[u8]) -> bool {
        let (first, second) = prefix.split_at(prefix.len().min(self.first.len()));
        self.first.starts_with(first) && self.second.starts_with(second)
    }

    pub fn ends_with(self, suffix: &[u8]) -> bool {
        let (first, second) = suffix.split_at(suffix.len().saturating_sub(self.second.len()));
        self.first.ends_with(first) && self.second.ends_with(second)
    }

    pub fn eq_ascii(self, bytes: &[u8]) -> bool {
        bytes.len() == self.len()
            && self
                .iter()
                .copied()
                .zip(bytes.iter().copied())
                .all(|(left, right)| left.eq_ignore_ascii_case(&right))
    }

    pub fn extend(self, out: &mut impl Extend<u8>) {
        out.extend(self.first.iter().copied());
        out.extend(self.second.iter().copied());
    }
}

impl PartialEq for Span<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.first
            .iter()
            .chain(self.second)
            .eq(other.first.iter().chain(other.second))
    }
}

impl Eq for Span<'_> {}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Entry<'a> {
    pub name: &'a [u8],
    pub value: &'a [u8],
    pub indexing: crate::field::Indexing,
}

impl<'a> Entry<'a> {
    pub const fn new(name: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            name,
            value,
            indexing: crate::field::Indexing::Allowed,
        }
    }

    pub const fn with_index(mut self, indexing: crate::field::Indexing) -> Self {
        self.indexing = indexing;
        self
    }
}

impl<'field> From<&Entry<'field>> for Entry<'field> {
    fn from(field: &Entry<'field>) -> Self {
        *field
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct View<'a> {
    pub name: Span<'a>,
    pub value: Span<'a>,
    pub indexing: crate::field::Indexing,
}

impl<'a> View<'a> {
    pub const fn new(name: Span<'a>, value: Span<'a>) -> Self {
        Self {
            name,
            value,
            indexing: crate::field::Indexing::Allowed,
        }
    }

    pub const fn with_index(mut self, indexing: crate::field::Indexing) -> Self {
        self.indexing = indexing;
        self
    }
}

impl<'a> From<Entry<'a>> for View<'a> {
    fn from(field: Entry<'a>) -> Self {
        Self::new(Span::from_slice(field.name), Span::from_slice(field.value))
            .with_index(field.indexing)
    }
}

#[doc(hidden)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Match<I, N = Option<I>> {
    pub exact: Option<I>,
    pub name: N,
}

impl<I> Match<I> {
    #[doc(hidden)]
    pub const fn none() -> Self {
        Self {
            exact: None,
            name: None,
        }
    }
}
