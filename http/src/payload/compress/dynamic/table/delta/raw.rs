use table::delta::{Borrow, buffer};

use crate::payload::compress::dynamic::{mode, table};

/// Owns one stable table/journal pair for the entire transaction.
/// # Safety
/// Implementations must return the same pair, with no other access while owned.
pub unsafe trait Source {
    fn parts(&self) -> (&table::Table<mode::Direct>, &buffer::Storage);
    fn parts_mut(&mut self) -> (&mut table::Table<mode::Direct>, &mut buffer::Storage);
}

unsafe impl Source for Borrow<'_> {
    fn parts(&self) -> (&table::Table<mode::Direct>, &buffer::Storage) {
        (self.table, self.pending)
    }

    fn parts_mut(&mut self) -> (&mut table::Table<mode::Direct>, &mut buffer::Storage) {
        (self.table, self.pending)
    }
}
