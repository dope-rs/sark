use core::ops;

use insert::{Draft, Value, state::Length, write};
use o3::mem;
use table::{
    ring::{Buf, Cursor},
    slot::Slot,
    state,
};

use crate::{
    field::metadata::block,
    payload::compress::dynamic::{ENTRY_OVERHEAD, field::NAME_LEN_MASK, insert, mode, table},
};

/// Exclusive name-writing phase of one dynamic-table insertion.
pub struct Name<'a>(Draft<'a>);

impl<'a> Name<'a> {
    pub(in crate::payload::compress::dynamic) fn new(
        table: &'a mut table::Table<mode::Staged>,
    ) -> Self {
        Self(Draft::new(table))
    }

    pub fn into_writer(self) -> insert::Writer<'a> {
        insert::Writer::new(self.0.table)
    }

    pub fn writer(&mut self) -> insert::Writer<'_> {
        self.0.writer()
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<(), write::Error> {
        self.0.push(bytes)
    }

    pub fn finish(self) -> Result<Value<'a>, mem::Rejected<Self, insert::Error>> {
        match self.0.table.storage.state_mut().insert.finish() {
            Ok(()) => Ok(Value::from(self.0)),
            Err(error) => Err(mem::Rejected::new(self, error)),
        }
    }

    /// Replaces the pending name with a complete classified table name.
    pub fn copy(mut self, index: usize) -> Result<Value<'a>, mem::Rejected<Self, insert::Error>> {
        match self.copy_name(index) {
            Ok(()) => Ok(Value::from(self.0)),
            Err(error) => Err(mem::Rejected::new(self, error)),
        }
    }

    pub(super) fn copy_name(&mut self, index: usize) -> Result<(), insert::Error> {
        let len = self.0.table.entry_count();
        let slot = match self.0.table.view().slot(index) {
            Some(slot) => slot,
            None => return Err(insert::Error::Index { index, len }),
        };
        // The live slot's name fits its entry and the table's current limit.
        let length = Length::from_size(state::Size::from_data(slot.name_len() as u32));
        let state = self.0.table.storage.state_mut();
        state.insert.tail = state.entries.tail();
        self.apply(slot.start(), length);
        self.0.table.storage.state_mut().insert.finish_copy(slot);
        Ok(())
    }

    pub(super) fn replace_name(&mut self, name: block::Named<'_>) -> Result<(), insert::Error> {
        let bytes = name.span();
        if bytes.len() > NAME_LEN_MASK as usize {
            return Err(insert::Error::Name {
                actual: bytes.len(),
                limit: NAME_LEN_MASK as usize,
            });
        }
        // NAME_LEN_MASK leaves room for the entry overhead on supported targets.
        let required = bytes.len() + ENTRY_OVERHEAD;
        let limit = self.0.table.max_size();
        if required > limit {
            return Err(insert::Error::Capacity { required, limit });
        }
        let start = self.0.table.storage.state().entries.tail();
        let parts = self.0.table.storage.parts_mut();
        let (first, second) = bytes.parts();
        let mut arena = Buf::new(parts.arena);
        let tail = arena.write(start, first);
        parts.state.insert.tail = arena.write(tail, second);
        parts.state.insert.len = Length::from_size(state::Size::from_data(bytes.len() as u32));
        parts.state.insert.admit(name.proof());
        Ok(())
    }

    pub(in crate::payload::compress::dynamic) fn duplicate(
        mut self,
        slot: Slot,
        size: state::Size,
    ) {
        self.apply(slot.start(), Length::from_size(size));
        let current = self.0.table.storage.state();
        let copied = Slot::pack(
            current.entries.tail(),
            slot.name_len(),
            slot.validation(),
        );
        let tail = current.insert.tail;
        self.0.table.edit().publish(copied, size, tail);
    }

    fn apply(&mut self, source: Cursor, length: Length) {
        let parts = self.0.table.storage.parts_mut();
        let target = parts.state.insert.tail;
        let tail = Buf::new(parts.arena).copy(source, target, length.get() as usize);
        parts.state.insert.tail = tail;
        parts.state.insert.len = length;
    }
}

impl<'a> ops::Deref for Name<'a> {
    type Target = Draft<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

const _: () = assert!(core::mem::size_of::<Name<'_>>() == core::mem::size_of::<usize>());
