mod private;
#[doc(hidden)]
pub mod records;

pub(crate) use private::Private;
use slot::Slot;

use crate::payload::compress::dynamic::{hash, index, insert, table::slot};

/// A statically selected dynamic-table layout.
pub trait Kind: Private {}

/// Complete fields committed directly by HPACK and QPACK encoders.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Direct {}

/// Fragmented fields retained transactionally by HPACK and QPACK decoders.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Staged {}

impl Private for Direct {
    type Hash = hash::Seed;
    type Index = index::State;
    type Insert = ();
    type Record = records::Direct;

    const ARENAS: usize = 1;

    fn slot(record: &Self::Record) -> Slot {
        record.slot
    }
}

impl Kind for Direct {}

impl Private for Staged {
    type Hash = ();
    type Index = ();
    type Insert = insert::State;
    type Record = records::Staged;

    const ARENAS: usize = 2;

    fn slot(record: &Self::Record) -> Slot {
        record.0
    }
}

impl Kind for Staged {}
