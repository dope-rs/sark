use core::{mem, slice};

use o3::buffer::write::Sink;
use table::{
    ring::{Buf, Cursor},
    state::{Size, State},
};

use crate::{
    field::metadata::block,
    payload::compress::dynamic::{
        ENTRY_OVERHEAD,
        insert::{state::Length, write::Error},
        mode, table,
    },
};

pub struct Writer<'a> {
    state: &'a mut State<mode::Staged>,
    arena: Buf<'a>,
    mark: Mark,
}

struct Mark {
    tail: Cursor,
    len: Length,
    name: block::Scan,
    value: block::Value,
}

impl<'a> Writer<'a> {
    pub(super) fn new(table: &'a mut table::Table<mode::Staged>) -> Self {
        let parts = table.storage.parts_mut();
        let state = parts.state;
        let arena = Buf::new(parts.arena);
        let mark = Mark {
            tail: state.insert.tail,
            len: state.insert.len,
            name: state.insert.name,
            value: state.insert.value,
        };
        Self { state, arena, mark }
    }

    /// Discards this writer's bytes and restores the classification of its prefix.
    pub fn rollback(self) {
        self.state.insert.tail = self.mark.tail;
        self.state.insert.len = self.mark.len;
        self.state.insert.name = self.mark.name;
        self.state.insert.value = self.mark.value;
    }

    pub fn written(&self) -> usize {
        self.state.insert.len.get() as usize
    }

    fn validate(&self, extra: usize) -> Result<Length, Error> {
        let len = self.state.insert.len.get() as usize;
        let data = match len.checked_add(extra) {
            Some(data) => data,
            None => {
                return Err(Error::Length {
                    len,
                    additional: extra,
                });
            }
        };
        let required = match data.checked_add(ENTRY_OVERHEAD) {
            Some(required) => required,
            None => {
                return Err(Error::Length {
                    len: data,
                    additional: ENTRY_OVERHEAD,
                });
            }
        };
        let limit = self.state.entries.limit();
        if required > limit {
            return Err(Error::Capacity { required, limit });
        }
        Ok(Length::from_size(Size::from_data(data as u32)))
    }

    fn extend(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let start = self.state.insert.tail;
        self.state.insert.tail = self.arena.write(start, bytes);
        if self.state.insert.is_finished() {
            self.state.insert.value.extend(bytes);
        } else {
            for &byte in bytes {
                self.state.insert.name.push(byte);
            }
        }
    }
}

impl Sink for Writer<'_> {
    type Error = Error;

    fn write_byte(&mut self, byte: u8) -> Result<(), Self::Error> {
        let len = self.validate(1)?;
        self.extend(slice::from_ref(&byte));
        self.state.insert.len = len;
        Ok(())
    }

    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        let len = self.validate(bytes.len())?;
        self.extend(bytes);
        self.state.insert.len = len;
        Ok(())
    }

    fn write_slices<const N: usize>(&mut self, slices: [&[u8]; N]) -> Result<(), Self::Error> {
        let mut extra = 0usize;
        for bytes in slices {
            let next = match extra.checked_add(bytes.len()) {
                Some(next) => next,
                None => {
                    return Err(Error::Length {
                        len: extra,
                        additional: bytes.len(),
                    });
                }
            };
            extra = next;
        }
        let len = self.validate(extra)?;
        for bytes in slices {
            self.extend(bytes);
        }
        self.state.insert.len = len;
        Ok(())
    }
}

const _: () = assert!(
    mem::size_of::<Writer<'_>>() <= 3 * mem::size_of::<usize>() + 4 * mem::size_of::<u32>()
);
