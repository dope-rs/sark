use std::{mem, ops};

use crate::field::{Indexing, metadata::map::record};

mod decoded;
pub mod prefix;

pub use decoded::Decoded;
use prefix::Prefix;
use record::Entry;

#[derive(Clone, Copy)]
pub struct Iter<'a> {
    current: &'a [u8],
}

pub struct RangeIter<'a> {
    current: Iter<'a>,
    offset: usize,
}

impl<'a> Iter<'a> {
    pub(in crate::field::metadata) const fn new(current: &'a [u8]) -> Self {
        Self { current }
    }
}

impl<'a> RangeIter<'a> {
    pub(in crate::field::metadata) fn new(current: &'a [u8], prefix: usize) -> Self {
        Self {
            current: Iter::new(&current[prefix..]),
            offset: prefix,
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_empty() {
            return None;
        }
        // SAFETY: only finished packed owners and transactional map writers create
        // this iterator. Every nonempty suffix retains a complete canonical prefix.
        let prefix = unsafe { &*self.current.as_ptr().cast::<[u8; Prefix::LEN]>() };
        let prefix = Prefix::decode(prefix, Indexing::from_never(prefix[8] != 0));
        let name_end = Prefix::LEN + prefix.name_len() as usize;
        let end = name_end + prefix.value_len() as usize;
        // SAFETY: the same admission proves both payload lengths within this
        // immutable suffix. Advancing by the whole record preserves that proof.
        let field = unsafe {
            let field = Entry::new(
                self.current.get_unchecked(Prefix::LEN..name_end),
                self.current.get_unchecked(name_end..end),
            )
            .with_index(prefix.indexing());
            self.current = self.current.get_unchecked(end..);
            field
        };
        Some(field)
    }
}

impl<'a> Iterator for RangeIter<'a> {
    type Item = (Entry<'a>, ops::Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let field = self.current.next()?;
        let start = self.offset + Prefix::LEN + field.name.len();
        self.offset = start + field.value.len();
        Some((field, start..self.offset))
    }
}

const _: () = assert!(mem::size_of::<Iter<'static>>() == mem::size_of::<&[u8]>());
