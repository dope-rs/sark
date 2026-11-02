mod journal;
mod query;

use std::num::NonZero;

use delta::buffer::Storage;
pub(crate) use journal::Journal;
use query::Query;
use sark_http::{
    field::metadata::map::record::Entry,
    payload::compress::dynamic::{self, mode, table},
    {self},
};
use table::delta;

use crate::qpack::{self, dynamics, encoder, ledger};

pub(crate) struct Delta<'a> {
    base: u64,
    next: u64,
    count: &'a mut u64,
    count_range: u64,
    floor: u64,
    wire_len: usize,
    inner: delta::Draft<delta::Borrow<'a>>,
}

impl<'a> Delta<'a> {
    pub(super) fn try_new(
        table: &'a mut dynamics::Table<mode::Direct>,
        floor: u64,
        buffer: &'a mut Option<Storage>,
    ) -> Result<Self, encoder::Error> {
        let bytes = table.view().capacity();
        let entries = bytes / dynamic::ENTRY_OVERHEAD;
        let staged = match buffer.take() {
            Some(staged) if staged.limits() == (bytes, entries) => staged,
            _ => Storage::try_new(bytes, entries)?,
        };
        let buffer = buffer.insert(staged);
        let base = table.view().insert_count();
        let (entries, count, maximum) = table.parts();
        Ok(Self {
            base,
            next: base,
            count,
            count_range: (maximum / 32) * 2,
            floor,
            wire_len: 0,
            inner: delta::Draft::new(delta::Borrow::new(entries, buffer)),
        })
    }

    pub(super) fn query<'b, 'field>(&'b mut self, field: Entry<'field>) -> Query<'b, 'a, 'field> {
        Query::new(
            self.base,
            &mut self.next,
            &mut self.floor,
            &mut self.wire_len,
            self.inner.query(field),
        )
    }

    pub(super) const fn base(&self) -> u64 {
        self.base
    }

    pub(super) fn needle(
        &self,
        name: &[u8],
        permit: &mut ledger::Admission<'_>,
    ) -> Option<qpack::Reference> {
        permit.select(|permit| {
            self.inner.needle(name, |position| {
                let index = Self::reference(self.base, position)?;
                permit.accept(index)
            })
        })
    }

    pub(super) fn protect(&mut self, index: qpack::Reference) {
        self.floor = self.floor.min(index.absolute());
    }

    pub(super) fn finish(self) -> Journal<'a> {
        Journal::new(self.count, self.next, self.wire_len, self.inner.finish())
    }

    pub(super) fn rollback(self) {
        self.inner.rollback();
    }

    pub(super) const fn encode_count(&self, required: u64) -> u64 {
        if required == 0 || self.count_range == 0 {
            return 0;
        }
        required % self.count_range + 1
    }

    fn reference(base: u64, position: delta::Position) -> Option<qpack::Reference> {
        match position {
            delta::Position::Base(relative) => {
                NonZero::new(base.checked_sub(relative as u64)?).map(qpack::Reference::from_count)
            }
            delta::Position::Staged(ordinal) => {
                qpack::Reference::new(base.checked_add(ordinal as u64)?)
            }
        }
    }
}

const _: () = assert!(usize::BITS <= u64::BITS);
