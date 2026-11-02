use crate::field::metadata::map::record;
pub mod block;
#[doc(hidden)]
pub mod huffman;
#[doc(hidden)]
pub mod integer;
#[doc(hidden)]
pub mod lower;
pub mod map;
pub mod packed;
#[doc(hidden)]
pub mod section;
pub mod strings;

pub trait Metadata {
    type Iter<'a>: Clone + Iterator<Item = record::Entry<'a>>
    where
        Self: 'a;

    fn fields(&self) -> Self::Iter<'_>;
}
