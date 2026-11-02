use std::num;

use crate::payload::compress::dynamic::{ENTRY_OVERHEAD, mode, plan::layout, table::ring::Cursor};

pub(in crate::payload::compress::dynamic::table) mod raw;

/// Retained ring bounds are `start < count` and `offset <= count`.
pub(in crate::payload::compress::dynamic::table) const fn position(
    count: num::NonZero<u32>,
    start: u32,
    offset: u32,
) -> u32 {
    let suffix = count.get() - start;
    if offset < suffix {
        start + offset
    } else {
        offset - suffix
    }
}

pub(in crate::payload::compress::dynamic::table) enum Admission<'a, M: mode::Kind> {
    Evict(raw::Pop<'a, M>),
    Ready(raw::Ready<'a, M>),
}

pub(in crate::payload::compress::dynamic) struct State<M: mode::Kind> {
    pub(in crate::payload::compress::dynamic) layout: layout::Layout<M>,
    pub(in crate::payload::compress::dynamic) entries: Entries,
    pub(in crate::payload::compress::dynamic) hash: M::Hash,
    pub(in crate::payload::compress::dynamic) insert: M::Insert,
}

pub(in crate::payload::compress::dynamic) struct Entries {
    front: u32,
    len: u32,
    tail: Cursor,
    size: u32,
    limit: u32,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::payload::compress::dynamic) struct Size(u32);

impl<M: mode::Kind> State<M> {
    pub(in crate::payload::compress::dynamic) fn new(
        layout: layout::Layout<M>,
        limit: usize,
        hash: M::Hash,
        insert: M::Insert,
    ) -> Self {
        let limit = limit.min(layout.capacity()) as u32;
        Self {
            layout,
            entries: Entries {
                front: 0,
                len: 0,
                tail: Cursor::ZERO,
                size: 0,
                limit,
            },
            hash,
            insert,
        }
    }

    pub(in crate::payload::compress::dynamic::table) fn clear(&mut self) {
        self.entries.clear();
    }
}

impl Entries {
    pub(in crate::payload::compress::dynamic::table) const fn front(&self) -> u32 {
        self.front
    }

    pub(in crate::payload::compress::dynamic) const fn len(&self) -> usize {
        self.len as usize
    }

    pub(in crate::payload::compress::dynamic) const fn size(&self) -> u32 {
        self.size
    }

    pub(in crate::payload::compress::dynamic) const fn limit(&self) -> usize {
        self.limit as usize
    }

    pub(in crate::payload::compress::dynamic) const fn tail(&self) -> Cursor {
        self.tail
    }

    pub(in crate::payload::compress::dynamic) const fn is_single(&self) -> bool {
        self.len == 1
    }

    pub(in crate::payload::compress::dynamic) const fn index(
        &self,
        count: num::NonZero<u32>,
    ) -> Index<'_> {
        Index {
            entries: self,
            count,
        }
    }

    fn reset(&mut self) {
        if self.len == 0 {
            self.front = 0;
            self.tail = Cursor::ZERO;
        }
    }

    pub(in crate::payload::compress::dynamic) fn set_limit(
        &mut self,
        limit: usize,
        capacity: usize,
    ) {
        self.limit = limit.min(capacity) as u32;
    }

    pub(in crate::payload::compress::dynamic) fn clear(&mut self) {
        self.front = 0;
        self.len = 0;
        self.tail = Cursor::ZERO;
        self.size = 0;
    }
}

pub(in crate::payload::compress::dynamic) struct Index<'a> {
    entries: &'a Entries,
    count: num::NonZero<u32>,
}

impl Index<'_> {
    pub(in crate::payload::compress::dynamic) fn physical(&self, index: usize) -> Option<usize> {
        if index >= self.entries.len() {
            return None;
        }
        Some(position(self.count, self.entries.front, index as u32) as usize)
    }
}

impl Size {
    pub(in crate::payload::compress::dynamic) const fn from_data(len: u32) -> Self {
        Self(len)
    }

    pub(in crate::payload::compress::dynamic) const fn from_total(total: u32) -> Self {
        Self(total - ENTRY_OVERHEAD as u32)
    }

    pub(in crate::payload::compress::dynamic) const fn into_u32(self) -> u32 {
        self.0
    }

    pub(in crate::payload::compress::dynamic) const fn data_len(self) -> usize {
        self.into_u32() as usize
    }

    pub(in crate::payload::compress::dynamic) const fn get(self) -> usize {
        self.data_len() + ENTRY_OVERHEAD
    }
}

const _: () = {
    assert!(core::mem::size_of::<Size>() == core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<<mode::Staged as mode::Private>::Hash>() == 0);
};
