pub(in crate::payload::compress::dynamic::table) mod input;
mod raw;
pub(in crate::payload::compress::dynamic::table) mod remove;
use mode::records;
use table::{Table, ring::Cursor, slot::Slot, state};

use crate::payload::compress::dynamic::{
    ENTRY_OVERHEAD,
    insert::{self, Name},
    mode, table,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Length { name: usize, value: usize },
    Name { actual: usize, limit: usize },
    Capacity { required: usize, limit: usize },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Change {
    Inserted,
    Cleared(Error),
}

pub struct Edit<'a, M: mode::Kind> {
    table: &'a mut Table<M>,
}

impl<'a, M: mode::Kind> Edit<'a, M> {
    pub(in crate::payload::compress::dynamic) const fn new(table: &'a mut Table<M>) -> Self {
        Self { table }
    }
    fn limit(&mut self, limit: usize) {
        let state = self.table.storage.state_mut();
        let capacity = state.layout.capacity();
        state.entries.set_limit(limit, capacity);
    }
}

impl Edit<'_, mode::Direct> {
    pub fn set_limit(&mut self, limit: usize) {
        self.limit(limit);
        self.trim();
    }

    pub fn insert(&mut self, name: &[u8], value: &[u8]) -> Change {
        match table::Input::try_new(self.table, name, value) {
            Ok(input) => {
                input.insert();
                Change::Inserted
            }
            Err(error) => Change::Cleared(error),
        }
    }

    fn trim(&mut self) {
        while let Some(oldest) = state::raw::Pop::oldest(self.table.storage.parts_mut()) {
            table::Remove::new(oldest).remove();
        }
    }
}

impl Edit<'_, mode::Staged> {
    pub fn set_limit(&mut self, limit: usize) {
        self.limit(limit);
        self.invalidate();
        self.trim();
    }

    pub fn reject(&mut self) {
        self.invalidate();
        self.table.storage.state_mut().clear();
    }

    pub fn duplicate(&mut self, index: usize) -> Result<(), insert::Error> {
        let limit = self.table.max_size();
        if limit < ENTRY_OVERHEAD {
            return Err(insert::Error::Capacity {
                required: ENTRY_OVERHEAD,
                limit,
            });
        }
        let len = self.table.entry_count();
        let view = self.table.view();
        let entry = match view.try_entry(index) {
            Some(entry) => entry,
            None => return Err(insert::Error::Index { index, len }),
        };
        let slot = entry.record().slot();
        let size = entry.size();
        let state = self.table.storage.state_mut();
        state.insert.begin(state.entries.tail());
        Name::new(self.table).duplicate(slot, size);
        Ok(())
    }

    pub(in crate::payload::compress::dynamic) fn publish(
        &mut self,
        slot: Slot,
        size: state::Size,
        tail: Cursor,
    ) {
        self.invalidate();
        let ready = self.evict(size);
        ready.finish(records::Staged::new(slot), tail).commit();
    }

    fn evict(&mut self, incoming: state::Size) -> state::raw::Ready<'_, mode::Staged> {
        let mut parts = self.table.storage.parts_mut();
        loop {
            match state::raw::Ready::admit(parts, incoming) {
                state::Admission::Evict(oldest) => {
                    parts = oldest.commit();
                }
                state::Admission::Ready(ready) => return ready,
            }
        }
    }

    fn trim(&mut self) {
        let mut parts = self.table.storage.parts_mut();
        while let Some(oldest) = state::raw::Pop::oldest(parts) {
            parts = oldest.commit();
        }
    }

    fn invalidate(&mut self) {
        let state = self.table.storage.state_mut();
        state.insert.invalidate(state.entries.tail());
    }
}

const _: () = {
    assert!(core::mem::size_of::<Error>() <= 3 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Change>() <= 3 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Edit<'static, mode::Direct>>() == core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Edit<'static, mode::Staged>>() == core::mem::size_of::<usize>());
};
