use std::cell::OnceCell;

use sark_core::http::FixedResponseInner;

pub enum Content {
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

pub struct Slot<'a> {
    slot: &'a OnceCell<Content>,
}

pub enum Hit {
    Fixed {
        written: usize,
    },
    Static {
        hdr_written: usize,
        body: &'static [u8],
    },
}

impl<'a> Slot<'a> {
    pub const fn new(slot: &'a OnceCell<Content>) -> Self {
        Self { slot }
    }

    pub fn try_hit(&self, write: &mut [u8], date: &[u8; 29]) -> Option<Hit> {
        match self.slot.get() {
            Some(Content::Fixed {
                template,
                date_offset,
            }) => FixedResponseInner::write_preserialized(write, template, *date_offset, date)
                .map(|written| Hit::Fixed { written }),
            Some(Content::Static {
                head_template,
                date_offset,
                body,
            }) => FixedResponseInner::write_preserialized(write, head_template, *date_offset, date)
                .map(|hdr_written| Hit::Static { hdr_written, body }),
            None => None,
        }
    }

    pub fn store_fixed(&self, template: Vec<u8>, date_offset: Option<usize>) {
        let _ = self.slot.set(Content::Fixed {
            template,
            date_offset,
        });
    }

    pub fn store_static(
        &self,
        head_template: Vec<u8>,
        date_offset: Option<usize>,
        body: &'static [u8],
    ) {
        let _ = self.slot.set(Content::Static {
            head_template,
            date_offset,
            body,
        });
    }
}
