use core::{mem, ops};

use insert::{Draft, write};
use table::{ring::Ring, slot::Slot, state};

use crate::{
    field::metadata::{block, map::record::Span},
    payload::compress::dynamic::{Whole, insert, mode, table},
};

/// Exclusive value-writing phase of one dynamic-table insertion.
pub struct Value<'a>(Draft<'a>);

impl<'a> Value<'a> {
    pub(in crate::payload::compress::dynamic) fn new(
        table: &'a mut table::Table<mode::Staged>,
    ) -> Self {
        Self(Draft::new(table))
    }

    pub(super) const fn from(draft: Draft<'a>) -> Self {
        Self(draft)
    }

    pub fn truncate(&mut self, len: usize) -> Result<(), insert::Error> {
        let parts = self.0.table.storage.parts_mut();
        let state = parts.state;
        let start = state.insert.name_len();
        let end = state.insert.len.get();
        if len < start as usize {
            return Err(insert::Error::Range(insert::Range::new(len, start, end)));
        }
        let Some(next) = state.insert.len.shrink(len) else {
            return Err(insert::Error::Range(insert::Range::new(len, start, end)));
        };
        if len < end as usize {
            let ring = Ring::new(parts.arena);
            let cursor = ring.advance(state.insert.checkpoint, start as usize);
            let value = ring.stored(cursor, len - start as usize);
            state.insert.value = block::Value::from_span(value);
            state.insert.len = next;
            state.insert.tail = ring.advance(state.insert.checkpoint, len);
        }
        Ok(())
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

    pub fn value(&self) -> Result<Span<'_>, insert::Error> {
        project(self.0.table)
    }

    pub fn commit(self) {
        let current = self.0.table.storage.state();
        let size = state::Size::from_data(current.insert.len.get());
        let slot = Slot::pack(
            current.insert.checkpoint,
            current.insert.name_len() as usize,
            Whole::from_value(current.insert.proof(), current.insert.value),
        );
        let tail = current.insert.tail;
        self.0.table.edit().publish(slot, size, tail);
    }
}

pub(super) fn project(table: &table::Table<mode::Staged>) -> Result<Span<'_>, insert::Error> {
    let parts = table.storage.memory().parts();
    let state = parts.state;
    let name_len = state.insert.name_len();
    let total = state.insert.len.get();
    if total < name_len {
        return Err(insert::Error::Range(insert::Range::new(
            total as usize,
            name_len,
            total,
        )));
    }
    let ring = Ring::new(parts.arena);
    let start = ring.advance(state.insert.checkpoint, name_len as usize);
    let len = total - name_len;
    match ring.span(start, len as usize) {
        Some(value) => Ok(value),
        None => Err(insert::Error::Span(insert::Span::new(
            start.into_u32(),
            len,
            state.layout.arena_len,
        ))),
    }
}

impl<'a> ops::Deref for Value<'a> {
    type Target = Draft<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

const _: () = assert!(mem::size_of::<Value<'_>>() == mem::size_of::<usize>());
