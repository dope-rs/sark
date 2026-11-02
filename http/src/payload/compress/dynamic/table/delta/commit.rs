use crate::payload::compress::dynamic::{
    Field,
    table::{self, delta},
};

/// Retains the original transaction owner until commit or rollback.
pub struct Commit<T: delta::raw::Source> {
    pub(super) owner: T,
}

const _: () = assert!(size_of::<Commit<delta::Borrow<'_>>>() == 2 * size_of::<usize>());

impl<T: delta::raw::Source> Commit<T> {
    pub fn fields(&self) -> impl DoubleEndedIterator<Item = Field<'_>> {
        self.owner.parts().1.iter().map(|item| item.0)
    }

    pub fn apply(mut self) -> T {
        let (table, pending) = self.owner.parts_mut();
        for item in pending.iter() {
            table::Input {
                table,
                field: item.0,
                digest: item.1,
                size: item.2,
            }
            .insert();
        }
        self.owner
    }

    pub fn rollback(mut self) -> T {
        self.owner.parts_mut().1.clear();
        self.owner
    }
}
