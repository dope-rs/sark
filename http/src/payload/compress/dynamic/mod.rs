//! Bounded storage shared by HPACK and QPACK dynamic tables.

use crate::field::metadata::block;

mod field;
pub mod hash;
pub mod index;
pub mod insert;
pub mod mode;
pub mod plan;
pub mod table;

pub use field::Field;

pub const ENTRY_OVERHEAD: usize = 32;

/// Name classification and context-independent rejection of one complete field.
/// This proof cannot be reused for a new value that only shares the name.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct Whole(u8);

struct Codes {
    malformed: [u8; 18],
    names: [block::Name; 32],
    rejected: u32,
}

const CODES: Codes = {
    let mut codes = Codes {
        malformed: [0; 18],
        names: [block::Name::INVALID; 32],
        rejected: 0,
    };
    let mut index = 0;
    let mut next = 18;
    while index < 18 {
        let name = block::Name::COMPACT[index];
        codes.names[index] = name;
        if name.is_forbidden() {
            codes.malformed[index] = index as u8;
            codes.rejected |= 1 << index;
        } else {
            codes.malformed[index] = next as u8;
            codes.names[next] = name;
            codes.rejected |= 1 << next;
            next += 1;
        }
        index += 1;
    }
    assert!(next == 30);
    codes.rejected |= (1 << 30) | (1 << 31);
    codes
};

impl Whole {
    /// Combines a name proof with independently established valid value bytes.
    pub const fn valid(name: block::Name) -> Self {
        Self(name.compact())
    }

    pub fn from_value(name: block::Name, value: block::Value) -> Self {
        if value.is_valid() {
            Self::valid(name)
        } else {
            Self(CODES.malformed[name.compact() as usize])
        }
    }

    pub const fn name(self) -> block::Name {
        // SAFETY: Whole's private constructors use an admitted compact name (0..18),
        // the const-built malformed codes (0..30), or five packed bits (0..32).
        // Copying and storing Whole preserve that bound for the 32-entry table.
        unsafe { CODES.names.as_ptr().add(self.0 as usize).read() }
    }

    /// Returns only this complete field's context-independent rejection result.
    pub const fn validation(self) -> block::Validation {
        if CODES.rejected & (1 << self.0) != 0 {
            block::Validation::Malformed
        } else {
            block::Validation::Valid
        }
    }

    const fn compact(self) -> u8 {
        self.0
    }

    const fn from_packed(packed: u32) -> Self {
        Self((packed >> field::NAME_LEN_BITS) as u8)
    }
}

const _: () = assert!(core::mem::size_of::<Whole>() == 1);
const _: () = assert!(CODES.names.len() == 1 << (u32::BITS - field::NAME_LEN_BITS));
