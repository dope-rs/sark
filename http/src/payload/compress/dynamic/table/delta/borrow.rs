use table::delta::buffer;

use crate::payload::compress::dynamic::{mode, table};

pub struct Borrow<'a> {
    pub(super) table: &'a mut table::Table<mode::Direct>,
    pub(super) pending: &'a mut buffer::Storage,
}

impl<'a> Borrow<'a> {
    pub fn new(
        table: &'a mut table::Table<mode::Direct>,
        pending: &'a mut buffer::Storage,
    ) -> Self {
        Self { table, pending }
    }
}
