use std::cell::OnceCell;

use sark_core::http::FixedResponseInner;

pub enum Entry {
    Fixed {
        template: Vec<u8>,
        date_offset: Option<usize>,
    },
    Static {
        head_template: Vec<u8>,
        date_offset: Option<usize>,
        body: &'static [u8],
    },
}

pub struct Cache<'a> {
    entry: Option<&'a OnceCell<Entry>>,
}

pub enum Cached {
    Fixed {
        written: usize,
    },
    Static {
        hdr_written: usize,
        body: &'static [u8],
    },
}

impl<'a> Cache<'a> {
    pub const fn new(entry: &'a OnceCell<Entry>) -> Self {
        Self { entry: Some(entry) }
    }

    pub const fn empty() -> Self {
        Self { entry: None }
    }

    pub fn write(&self, write: &mut [u8], date: &[u8; 29]) -> Option<Cached> {
        match self.entry.and_then(OnceCell::get) {
            Some(Entry::Fixed {
                template,
                date_offset,
            }) => FixedResponseInner::write_preserialized(write, template, *date_offset, date)
                .map(|written| Cached::Fixed { written }),
            Some(Entry::Static {
                head_template,
                date_offset,
                body,
            }) => FixedResponseInner::write_preserialized(write, head_template, *date_offset, date)
                .map(|hdr_written| Cached::Static { hdr_written, body }),
            None => None,
        }
    }

    pub fn insert_fixed(&self, template: Vec<u8>, date_offset: Option<usize>) {
        if let Some(entry) = self.entry {
            let _ = entry.set(Entry::Fixed {
                template,
                date_offset,
            });
        }
    }

    pub fn insert_static(
        &self,
        head_template: Vec<u8>,
        date_offset: Option<usize>,
        body: &'static [u8],
    ) {
        if let Some(entry) = self.entry {
            let _ = entry.set(Entry::Static {
                head_template,
                date_offset,
                body,
            });
        }
    }
}
