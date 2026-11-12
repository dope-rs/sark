use o3::buffer::write::Slice;
use o3::{buffer::storage, mem::rc};

use crate::wire::response::Context;

mod entry;

pub use entry::Entry;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Date { offset: usize, available: usize },
    Body { length: usize, available: usize },
}

pub struct Cache<'a> {
    entry: Option<&'a mut Option<Entry>>,
}

pub enum Cached {
    Fixed { body: usize },
    Shared { body: storage::Shared },
}

impl<'a> Cache<'a> {
    pub const fn new(entry: &'a mut Option<Entry>) -> Self {
        Self { entry: Some(entry) }
    }

    pub const fn empty() -> Self {
        Self { entry: None }
    }

    pub fn try_write(
        &self,
        write: Slice<'_, &mut usize>,
        date: &[u8; 29],
        response: Context,
    ) -> Result<Option<Cached>, rc::Error> {
        let Some(entry) = self.entry.as_deref().and_then(Option::as_ref) else {
            return Ok(None);
        };
        if !entry.matches(response) {
            return Ok(None);
        }
        entry.try_write(write, date)
    }

    pub fn insert(self, entry: Entry) {
        if let Some(slot) = self.entry {
            *slot = Some(entry);
        }
    }
}
