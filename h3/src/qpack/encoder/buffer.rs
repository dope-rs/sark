use std::mem;

use delta::buffer;
use sark_http::payload::compress::dynamic::table::delta;

use crate::qpack::encoder;

pub(crate) struct Buffer {
    pending: encoder::Wire,
    section: Vec<u8>,
    staged: Option<buffer::Storage>,
}

pub(super) struct Cache<'a>(&'a mut Vec<u8>);

pub(super) struct Parts<'a> {
    pub(super) pending: &'a mut encoder::Wire,
    pub(super) section: Cache<'a>,
    pub(super) staged: &'a mut Option<buffer::Storage>,
}

impl Cache<'_> {
    pub(super) fn take(&mut self) -> Vec<u8> {
        mem::take(self.0)
    }

    pub(super) fn recycle(&mut self, mut lines: Vec<u8>) {
        lines.clear();
        if lines.capacity() > self.0.capacity() {
            *self.0 = lines;
        }
    }
}

impl Buffer {
    pub(super) fn new() -> Self {
        Self {
            pending: encoder::Wire::new(),
            section: Vec::new(),
            staged: None,
        }
    }

    pub(super) fn from(capacity: &encoder::Capacity) -> Self {
        Self {
            pending: encoder::Wire::from(capacity),
            section: Vec::new(),
            staged: None,
        }
    }

    pub(crate) fn take_pending(&mut self) -> encoder::Wire {
        mem::replace(&mut self.pending, encoder::Wire::new())
    }

    pub(super) fn parts(&mut self) -> Parts<'_> {
        Parts {
            pending: &mut self.pending,
            section: Cache(&mut self.section),
            staged: &mut self.staged,
        }
    }
}
