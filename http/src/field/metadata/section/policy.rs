//! Field-section policy lifecycle shared by concrete input contracts.

use crate::field::metadata::section::storage;

pub trait Policy: Sized {
    type Selection;
    type Block: storage::Block;
    fn finish(self) -> Self::Selection;
}
