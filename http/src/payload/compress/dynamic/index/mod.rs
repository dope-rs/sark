use o3::depot::map::{compact, compact::Access as _};

use crate::payload::compress::dynamic::hash;

pub(super) mod raw;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error(compact::Error);

pub(super) struct Search<Q: compact::raw::Query>(Q);

impl<Q: compact::raw::Query> Search<Q> {
    pub(super) const fn new(owner: Q) -> Self {
        Self(owner)
    }

    pub(super) fn find<I: Copy>(
        self,
        hash: u64,
        mut accept: impl FnMut(Q::Item) -> Option<(usize, I)>,
    ) -> Option<I> {
        let mut best: Option<(usize, I)> = None;
        self.0.scan(hash, |item| {
            if let Some(found) = accept(item)
                && best.is_none_or(|current| found.0 < current.0)
            {
                best = Some(found);
            }
        });
        best.map(|found| found.1)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::payload::compress::dynamic) enum Key {
    Name,
    Exact,
}

impl Key {
    /// The key domain selected by a two-phase adapter's compile-time stage.
    pub(in crate::payload::compress::dynamic) const fn staged<const N: usize>() -> Self {
        if N == 0 { Self::Name } else { Self::Exact }
    }

    /// This domain's component of one record digest.
    pub(in crate::payload::compress::dynamic) fn of(self, digest: hash::Digest) -> u64 {
        match self {
            Self::Name => digest.name(),
            Self::Exact => digest.exact(),
        }
    }
}

/// The name and exact-field directories of one bounded arena.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct State {
    name: compact::Layout,
    exact: compact::Layout,
}

impl State {
    pub(super) fn new(entries: usize, start: usize) -> Result<(Self, usize), Error> {
        let (name, start) = compact::Layout::try_new(entries, start).map_err(Error)?;
        let (exact, end) = compact::Layout::try_new(entries, start).map_err(Error)?;
        Ok((Self { name, exact }, end))
    }

    pub(super) const fn layout(self, key: Key) -> compact::Layout {
        match key {
            Key::Name => self.name,
            Key::Exact => self.exact,
        }
    }

    pub(super) fn byte(self, offset: usize) -> u8 {
        self.name.byte(offset).max(self.exact.byte(offset))
    }
}
