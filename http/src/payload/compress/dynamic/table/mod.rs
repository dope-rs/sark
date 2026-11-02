use crate::{
    field::metadata::map::record,
    payload::compress::dynamic::{ENTRY_OVERHEAD, Field, hash, insert, mode},
};

pub mod delta;
pub mod edit;
pub(in crate::payload::compress::dynamic) mod ring;
mod search;
pub(super) mod slot;
pub(super) mod state;
mod storage;
mod view;

use edit::{
    input::{Input, Insert},
    remove::Remove,
};
pub use search::Search;
pub(in crate::payload::compress::dynamic) use storage::{Memory, Parts, PartsMut, Storage};
pub use view::View;
use view::entry::Entry;

pub struct Table<M: mode::Kind = mode::Staged> {
    pub(super) storage: Storage<M>,
}

impl<M: mode::Kind> Table<M> {
    pub fn entry_count(&self) -> usize {
        self.storage.state().entries.len()
    }

    pub fn current_size(&self) -> usize {
        self.storage.state().entries.size() as usize
    }

    pub fn max_size(&self) -> usize {
        self.storage.state().entries.limit()
    }

    pub fn capacity(&self) -> usize {
        self.storage.state().layout.capacity()
    }

    pub fn field(&self, index: usize) -> Option<Field<'_>> {
        self.view().try_entry(index).map(Entry::field)
    }

    pub fn view(&self) -> View<'_, M> {
        View::new(self.storage.memory())
    }

    pub fn edit(&mut self) -> edit::Edit<'_, M> {
        edit::Edit::new(self)
    }
}

impl Table<mode::Direct> {
    pub fn probe<'a>(&self, field: record::Entry<'a>) -> hash::Probe<'a> {
        self.storage.state().hash.probe(field)
    }

    pub fn needle<'a>(&self, name: &'a [u8]) -> hash::Needle<'a> {
        self.storage.state().hash.needle(name)
    }

    /// Returns this table's owner-bound hash seed.
    pub fn seed(&self) -> &hash::Seed {
        &self.storage.state().hash
    }
}

impl Table<mode::Staged> {
    pub fn try_begin(&mut self) -> Result<insert::Name<'_>, insert::Capacity> {
        let state = self.storage.state();
        let limit = state.entries.limit();
        if limit < ENTRY_OVERHEAD {
            return Err(insert::Capacity {
                required: ENTRY_OVERHEAD,
                limit,
            });
        }
        let state = self.storage.state_mut();
        state.insert.begin(state.entries.tail());
        Ok(insert::Name::new(self))
    }

    pub(in crate::payload::compress::dynamic) fn name(&mut self) -> insert::Name<'_> {
        insert::Name::new(self)
    }

    pub(in crate::payload::compress::dynamic) fn value(&mut self) -> insert::Value<'_> {
        insert::Value::new(self)
    }

    pub fn rollback(&mut self) {
        let state = self.storage.state_mut();
        state.insert.invalidate(state.entries.tail());
    }
}

const _: () = {
    assert!(core::mem::size_of::<Table<mode::Direct>>() == core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Table<mode::Staged>>() == core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Option<Table<mode::Direct>>>() == core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Option<Table<mode::Staged>>>() == core::mem::size_of::<usize>());
};
