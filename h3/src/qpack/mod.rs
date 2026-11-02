use core::{fmt, num};

use decoder::state;
use o3::{
    alloc,
    buffer::{pool, write::Sink},
    depot::map,
};
use sark_http::{field::metadata::integer, payload::compress::dynamic::plan};

pub mod decoder;
pub mod dynamics;
pub mod encoder;
pub(crate) mod identity;
pub(crate) mod ledger;
mod literal;
mod statics;
pub mod stream;

pub(crate) const FEEDBACK_CAPACITY: usize = decoder::FEEDBACK_CAPACITY;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Insertion {
    #[default]
    None,
    Committed,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeltaSign {
    Positive,
    Negative,
}

impl fmt::Display for DeltaSign {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Progress {
    pub(crate) consumed: usize,
    pub(crate) insertion: Insertion,
    pub(crate) feedback: state::Status,
}

#[derive(Clone, Copy)]
pub(crate) struct Growth(usize);

impl Growth {
    /// The exact bytes one admitted writer committed into the feedback wire.
    pub(in crate::qpack) const fn new(bytes: usize) -> Self {
        Self(bytes)
    }

    /// Adds one committed writer's exact growth to this running total.
    pub(in crate::qpack) const fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }

    pub(crate) const fn bytes(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Reference(num::NonZero<u64>);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Requirement {
    Static,
    Dynamic(Reference),
}

impl Reference {
    pub(crate) const fn new(absolute: u64) -> Option<Self> {
        let Some(required) = num::NonZero::<u64>::MIN.checked_add(absolute) else {
            return None;
        };
        Some(Self::from_count(required))
    }

    pub(in crate::qpack) const fn from_count(required: num::NonZero<u64>) -> Self {
        Self(required)
    }

    pub(crate) const fn absolute(self) -> u64 {
        self.0.get() - 1
    }

    pub const fn required(self) -> num::NonZero<u64> {
        self.0
    }

    pub(crate) const fn is_received(self, received: u64) -> bool {
        self.required().get() <= received
    }
}

impl Requirement {
    pub(crate) const fn count(self) -> u64 {
        match self {
            Self::Static => 0,
            Self::Dynamic(required) => required.required().get(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Create {
    Fields(pool::LayoutError),
    Table(plan::Error),
    Storage(Storage),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Storage {
    Table(alloc::Error),
    Pool(alloc::Exhausted),
}

#[derive(Clone, Copy)]
pub(crate) struct StreamHash;

impl<T> map::Hasher<T> for StreamHash
where
    T: Copy + Into<u64>,
{
    fn hash(&self, value: &T) -> u64 {
        let mut value = (*value).into();
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}

const _: () = {
    assert!(core::mem::size_of::<Reference>() == core::mem::size_of::<u64>());
    assert!(core::mem::size_of::<Option<Reference>>() == core::mem::size_of::<u64>());
    assert!(core::mem::size_of::<Requirement>() == core::mem::size_of::<u64>());
};

#[derive(PartialEq, Eq)]
pub struct Section {
    encoded_required_insert_count: integer::PrefixedInt<8>,
    delta_base: integer::PrefixedInt<7>,
    delta_base_sign: DeltaSign,
    field_lines: Vec<u8>,
}

impl Section {
    pub(crate) fn new(encoded: u64, required: u64, base: u64, lines: Vec<u8>) -> Self {
        use sark_http::field::metadata::integer::PrefixedInt;
        let (delta, sign) = if base >= required {
            (base - required, DeltaSign::Positive)
        } else {
            (required - base - 1, DeltaSign::Negative)
        };
        Self {
            encoded_required_insert_count: PrefixedInt::new(encoded),
            delta_base: PrefixedInt::new(delta),
            delta_base_sign: sign,
            field_lines: lines,
        }
    }

    pub fn prefix_len(&self) -> usize {
        self.encoded_required_insert_count.encoded_len().get() + self.delta_base.encoded_len().get()
    }

    pub fn encoded_len(&self) -> usize {
        self.prefix_len() + self.field_lines.len()
    }

    pub fn write_prefix(&self, out: &mut impl Extend<u8>) {
        self.encoded_required_insert_count.encode(0, out);
        self.delta_base.encode(self.sign(), out);
    }

    pub(crate) fn try_prefix<W: Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        self.encoded_required_insert_count.try_encode(0, out)?;
        self.delta_base.try_encode(self.sign(), out)
    }

    fn sign(&self) -> u8 {
        match self.delta_base_sign {
            DeltaSign::Positive => 0,
            DeltaSign::Negative => 0x80,
        }
    }

    pub fn lines(&self) -> &[u8] {
        &self.field_lines
    }

    pub fn into_lines(self) -> Vec<u8> {
        self.field_lines
    }
}
