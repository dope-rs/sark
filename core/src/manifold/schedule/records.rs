use lanes::entry;
use o3::{
    alloc,
    depot::{capacity, queue::indexed::lanes},
};

use crate::manifold::{
    self,
    schedule::{ALL, Insert, LANES},
};

/// Fixed resident values and their one canonical queue membership.
pub struct Records<K: manifold::raw::Key, T> {
    pub(super) slots: lanes::Table<K, T, LANES>,
    pub(super) limit: capacity::Count,
}

impl<K: manifold::raw::Key, T> Records<K, T> {
    pub(super) fn try_new(
        cap: capacity::Count,
        limit: capacity::Count,
    ) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: lanes::Table::try_new(cap)?,
            limit: limit.min(cap),
        })
    }

    pub fn capacity(&self) -> usize {
        self.slots.capacity()
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.len() == 0
    }

    pub fn contains(&self, stream: K) -> bool {
        self.slots.get(stream).is_some()
    }

    pub fn key_at(&self, index: usize) -> Option<K> {
        self.slots.key_at(index)
    }

    pub fn entry_at(&self, index: usize) -> Option<(K, &T)> {
        self.slots.entry_at(index)
    }

    pub fn has_work(&self) -> bool {
        ALL.iter().any(|&queue| !self.slots.is_vacant(queue))
    }

    pub(super) fn try_vacant<I>(
        &mut self,
        stream: K,
        value: I,
    ) -> Result<(entry::Vacancy<'_, K, T, LANES>, I), Insert<K, I>> {
        let used = self.slots.len();
        let limit = self.limit.get();
        let vacant = match self.slots.try_vacant(stream) {
            Ok(vacant) => vacant,
            Err(error) => return Err(Insert::from_direct(error, value)),
        };
        if used == limit {
            return Err(Insert::Full {
                stream,
                value,
                used,
                limit,
            });
        }
        Ok((vacant, value))
    }

    pub fn get(&self, stream: K) -> Option<&T> {
        self.slots.get(stream)
    }

    pub fn get_mut(&mut self, stream: K) -> Option<&mut T> {
        self.slots.entry(stream).map(entry::Slot::into_mut)
    }

    pub fn remove(&mut self, stream: K) -> Option<T> {
        self.slots.entry(stream).map(entry::Slot::remove)
    }
}
