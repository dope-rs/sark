use delta::buffer;

use crate::{
    field::metadata::map::record::{Entry, Match},
    payload::compress::dynamic::{
        hash,
        table::delta::{self, Draft},
    },
};

pub struct Query<'a, 'field, T: delta::raw::Source> {
    draft: &'a mut Draft<T>,
    probe: hash::Probe<'field>,
}

impl<'a, 'field, T: delta::raw::Source> Query<'a, 'field, T> {
    pub(super) fn new(draft: &'a mut Draft<T>, field: Entry<'field>) -> Self {
        Self {
            probe: draft.owner.parts().0.probe(field),
            draft,
        }
    }

    pub fn both<I: Copy>(&self, map: impl FnMut(delta::Position) -> Option<I>) -> Match<I> {
        let (table, pending) = self.draft.owner.parts();
        self.draft.change.both(table, pending, self.probe, map)
    }

    pub fn exact<I: Copy>(&self, map: impl FnMut(delta::Position) -> Option<I>) -> Option<I> {
        let (table, pending) = self.draft.owner.parts();
        self.draft.change.exact(table, pending, self.probe, map)
    }

    pub fn name<I: Copy>(&self, map: impl FnMut(delta::Position) -> Option<I>) -> Option<I> {
        let (table, pending) = self.draft.owner.parts();
        self.draft
            .change
            .needle(table, pending, self.probe.needle(), map)
    }

    pub fn both_index(&self) -> Match<usize> {
        let change = &self.draft.change;
        let (table, pending) = self.draft.owner.parts();
        change.both(table, pending, self.probe, |position| {
            change.relative(pending, position)
        })
    }

    pub fn exact_index(&self) -> Option<usize> {
        let change = &self.draft.change;
        let (table, pending) = self.draft.owner.parts();
        change.exact(table, pending, self.probe, |position| {
            change.relative(pending, position)
        })
    }

    pub fn try_insert(&mut self) -> Result<(), delta::Failure> {
        let (table, pending) = self.draft.owner.parts_mut();
        let insert = self.draft.change.try_insert(table, pending, self.probe)?;
        let append: buffer::Append<'_> = pending.append();
        let ready = append.try_prepare(self.probe, insert.size, insert.proof)?;
        insert.commit(ready);
        Ok(())
    }

    pub fn try_admit(
        &mut self,
        accept: impl FnMut(delta::Position) -> bool,
    ) -> Result<(), delta::Error> {
        let (table, pending) = self.draft.owner.parts_mut();
        let insert = self
            .draft
            .change
            .try_admit(table, pending, self.probe, accept)?;
        let append: buffer::Append<'_> = pending.append();
        let ready = append.try_prepare(self.probe, insert.size, insert.proof)?;
        insert.commit(ready);
        Ok(())
    }

    pub const fn field(&self) -> Entry<'field> {
        self.probe.field()
    }
}
