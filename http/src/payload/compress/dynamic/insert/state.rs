use crate::{
    field::metadata::block,
    payload::compress::dynamic::{
        Whole, field, insert,
        table::{ring::Cursor, slot::Slot, state::Size},
    },
};

pub struct State {
    pub(super) tail: Cursor,
    pub(super) len: Length,
    pub(super) checkpoint: Cursor,
    packed_name: Packed,
    pub(super) name: block::Scan,
    pub(super) value: block::Value,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(super) struct Length(u32);

#[derive(Clone, Copy)]
#[repr(transparent)]
struct Packed(u32);

impl State {
    pub(in crate::payload::compress::dynamic) fn begin(&mut self, tail: Cursor) {
        self.tail = tail;
        self.len = Length::ZERO;
        self.checkpoint = tail;
        self.packed_name = Packed::UNFINISHED;
        self.name = block::Scan::new();
        self.value = block::Value::new();
    }

    pub(in crate::payload::compress::dynamic) fn invalidate(&mut self, tail: Cursor) {
        self.tail = tail;
        self.len = Length::INACTIVE;
    }

    pub(super) const fn is_finished(&self) -> bool {
        self.packed_name.is_finished()
    }

    pub(super) const fn name_len(&self) -> u32 {
        self.packed_name.len()
    }

    pub(super) fn proof(&self) -> block::Name {
        self.packed_name.proof()
    }

    pub(super) fn finish(&mut self) -> Result<(), insert::Error> {
        let proof = self.name.finish();
        let len = self.len.get();
        if len > field::NAME_LEN_MASK {
            return Err(insert::Error::Name {
                actual: len as usize,
                limit: field::NAME_LEN_MASK as usize,
            });
        }
        self.pack(len, proof);
        Ok(())
    }

    pub(super) fn finish_copy(&mut self, slot: Slot) {
        self.pack(slot.name_len() as u32, slot.proof());
    }

    pub(super) fn admit(&mut self, proof: block::Name) {
        self.pack(self.len.get(), proof);
    }

    fn pack(&mut self, name_len: u32, proof: block::Name) {
        self.packed_name = Packed::new(name_len, proof);
    }
}

impl Length {
    const INACTIVE: Self = Self(u32::MAX);
    const ZERO: Self = Self(0);

    pub(super) const fn get(self) -> u32 {
        self.0
    }

    pub(super) fn shrink(self, data: usize) -> Option<Self> {
        (data <= self.0 as usize).then_some(Self(data as u32))
    }

    pub(super) const fn from_size(size: Size) -> Self {
        Self(size.into_u32())
    }
}

impl Packed {
    const UNFINISHED: Self = Self(31 << field::NAME_LEN_BITS);

    const fn new(name_len: u32, proof: block::Name) -> Self {
        Self(name_len | (proof.compact() as u32) << field::NAME_LEN_BITS)
    }

    const fn is_finished(self) -> bool {
        self.0 >> field::NAME_LEN_BITS != 31
    }

    const fn len(self) -> u32 {
        self.0 & field::NAME_LEN_MASK
    }

    /// Reads an admitted name code (0..=17), or INVALID for unfinished code 31.
    fn proof(self) -> block::Name {
        Whole::from_packed(self.0).name()
    }
}

impl Default for State {
    fn default() -> Self {
        Self {
            tail: Cursor::ZERO,
            len: Length::INACTIVE,
            checkpoint: Cursor::ZERO,
            packed_name: Packed::UNFINISHED,
            name: block::Scan::new(),
            value: block::Value::new(),
        }
    }
}

const _: () = assert!(core::mem::size_of::<State>() == 5 * core::mem::size_of::<u32>());
