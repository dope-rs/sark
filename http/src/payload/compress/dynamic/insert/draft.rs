use core::mem;

use o3::buffer::write::Sink;
use table::ring::Ring;

use crate::{
    field::metadata::map::record::Span,
    payload::compress::dynamic::{insert, insert::write, mode, table},
};

/// Common byte-writing capability shared by the name and value phases.
pub struct Draft<'a> {
    pub(super) table: &'a mut table::Table<mode::Staged>,
}

impl<'a> Draft<'a> {
    pub(super) fn new(table: &'a mut table::Table<mode::Staged>) -> Self {
        Self { table }
    }

    pub fn written(&self) -> usize {
        self.table.storage.state().insert.len.get() as usize
    }

    pub fn writer(&mut self) -> insert::Writer<'_> {
        insert::Writer::new(self.table)
    }

    pub(super) fn push(&mut self, bytes: &[u8]) -> Result<(), write::Error> {
        self.writer().write_slice(bytes)
    }

    pub fn name(&self) -> Result<Span<'_>, insert::Error> {
        let state = self.table.storage.state();
        let len = if state.insert.is_finished() {
            state.insert.name_len()
        } else {
            state.insert.len.get()
        };
        span(self.table, len)
    }
}

pub(super) fn span(
    table: &table::Table<mode::Staged>,
    len: u32,
) -> Result<Span<'_>, insert::Error> {
    let state = table.storage.state();
    let parts = table.storage.memory().parts();
    let start = state.insert.checkpoint;
    match Ring::new(parts.arena).span(start, len as usize) {
        Some(name) => Ok(name),
        None => Err(insert::Error::Span(insert::Span::new(
            start.into_u32(),
            len,
            state.layout.arena_len,
        ))),
    }
}

const _: () = assert!(mem::size_of::<Draft<'_>>() == mem::size_of::<usize>());
