use crate::{
    field::metadata::block,
    payload::compress::dynamic::{Whole, field, table::ring::Cursor},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Slot {
    start: Cursor,
    packed_name: Packed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct Packed(u32);

impl Slot {
    pub(in crate::payload::compress::dynamic) const BYTES: usize = 8;
    pub(in crate::payload::compress::dynamic) const fn pack(
        start: Cursor,
        name_len: usize,
        check: Whole,
    ) -> Self {
        Self {
            start,
            packed_name: Packed::new(name_len, check),
        }
    }

    pub(in crate::payload::compress::dynamic) const fn start(self) -> Cursor {
        self.start
    }

    pub(in crate::payload::compress::dynamic) const fn name_len(self) -> usize {
        self.packed_name.len()
    }

    pub(in crate::payload::compress::dynamic) fn proof(self) -> block::Name {
        self.validation().name()
    }

    pub(in crate::payload::compress::dynamic) const fn validation(self) -> Whole {
        self.packed_name.validation()
    }
}

impl Packed {
    const fn new(name_len: usize, check: Whole) -> Self {
        Self(name_len as u32 | (check.compact() as u32) << field::NAME_LEN_BITS)
    }

    const fn len(self) -> usize {
        (self.0 & field::NAME_LEN_MASK) as usize
    }

    const fn validation(self) -> Whole {
        Whole::from_packed(self.0)
    }
}

const _: () = assert!(core::mem::size_of::<Slot>() == Slot::BYTES);
