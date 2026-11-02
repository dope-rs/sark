use o3::{base::num::bounded, depot::queue::links};

use crate::stream::{Id, Nonzero};

/// ABA-safe active stream identity in one machine word.
/// The high word is the never-reused wire id; the low word is its stable
/// connection-local slot.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key {
    pub(in crate::stream) id: Nonzero,
    pub(in crate::stream) slot: bounded::U32<0, { u32::MAX - 1 }>,
}

impl Key {
    pub const fn id(self) -> Id {
        self.id.get()
    }

    pub(crate) const fn nonzero(self) -> Nonzero {
        self.id
    }

    #[doc(hidden)]
    pub const fn identity(self) -> u64 {
        (self.id().into_u32() as u64) << 32 | self.slot.get() as u64
    }

    pub(crate) const fn from_parts(id: Nonzero, index: links::Index) -> Self {
        Self {
            id,
            slot: bounded::U32::from_one(index.position()),
        }
    }

    pub(crate) const fn slot(self) -> u32 {
        self.slot.get()
    }

    pub(crate) const fn index(self) -> usize {
        self.slot.into_usize()
    }
}

impl From<Key> for links::Index {
    fn from(key: Key) -> Self {
        key.slot.into()
    }
}

impl PartialEq<Id> for Key {
    fn eq(&self, other: &Id) -> bool {
        self.id() == *other
    }
}

impl PartialEq<Key> for Id {
    fn eq(&self, other: &Key) -> bool {
        *self == other.id()
    }
}

const _: () = assert!(core::mem::size_of::<Key>() == core::mem::size_of::<u64>());
const _: () = assert!(core::mem::size_of::<Option<Key>>() == core::mem::size_of::<u64>());
