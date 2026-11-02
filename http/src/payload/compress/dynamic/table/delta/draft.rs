use crate::{
    field::metadata::map::record::Entry,
    payload::compress::dynamic::table::delta::{self},
};

/// Retains the original table and journal through the transaction.
///
///
///
pub struct Draft<T: delta::raw::Source> {
    pub(super) owner: T,
    pub(super) change: delta::Change,
}

const _: () = assert!(size_of::<Draft<delta::Borrow<'_>>>() == 4 * size_of::<usize>());

impl<T: delta::raw::Source> Draft<T> {
    pub fn new(mut owner: T) -> Self {
        let (table, pending) = owner.parts_mut();
        let change = delta::Change::new(table, pending);
        Self { owner, change }
    }

    pub fn query<'a, 'field>(&'a mut self, field: Entry<'field>) -> delta::Query<'a, 'field, T> {
        delta::Query::new(self, field)
    }

    pub fn needle<I: Copy>(
        &self,
        name: &[u8],
        map: impl FnMut(delta::Position) -> Option<I>,
    ) -> Option<I> {
        let (table, pending) = self.owner.parts();
        self.change.needle(table, pending, table.needle(name), map)
    }

    pub fn needle_index(&self, name: &[u8]) -> Option<usize> {
        let (table, pending) = self.owner.parts();
        self.change
            .needle(table, pending, table.needle(name), |position| {
                self.change.relative(pending, position)
            })
    }

    pub fn max_size(&self) -> usize {
        self.owner.parts().0.max_size()
    }

    pub fn finish(self) -> delta::Commit<T> {
        delta::Commit { owner: self.owner }
    }

    pub fn rollback(mut self) -> T {
        self.owner.parts_mut().1.clear();
        self.owner
    }
}
