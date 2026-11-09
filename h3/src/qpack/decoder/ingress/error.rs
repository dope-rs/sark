use dynamic::insert::{self, write};
use sark_http::payload::compress::dynamic;

use crate::qpack::{dynamics, statics, stream};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Overflow {
    pub position: usize,
    pub value: u64,
    pub shift: u32,
    pub byte: u8,
}

/// Exact rejection observed while consuming the peer's QPACK encoder stream.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    Position {
        position: usize,
        additional: usize,
        available: usize,
    },
    Integer(Overflow),
    CapacityRange {
        value: u64,
    },
    NameRange {
        value: u64,
    },
    ValueRange {
        value: u64,
    },
    StaticReference {
        index: u64,
    },
    DynamicReference {
        relative: u64,
        resident: usize,
    },
    InsertCount {
        current: u64,
    },
    Capacity(dynamics::Capacity),
    TableCapacity(insert::Capacity),
    TableInsert(insert::Error),
    NameEncoding,
    ValueEncoding,
    LiteralWrite(write::Error),
    FeedbackBlocked(stream::Capacity),
    FeedbackIncrement {
        pending: u64,
        additional: u64,
    },
}

impl From<dynamics::Capacity> for Error {
    fn from(error: dynamics::Capacity) -> Self {
        Self::Capacity(error)
    }
}

impl From<statics::Error> for Error {
    fn from(error: statics::Error) -> Self {
        match error {
            statics::Error::Reference { index } => Self::StaticReference { index },
        }
    }
}

impl From<dynamics::Error> for Error {
    fn from(error: dynamics::Error) -> Self {
        match error {
            dynamics::Error::Reference { relative, len } => Self::DynamicReference {
                relative,
                resident: len,
            },
            dynamics::Error::Count { current } => Self::InsertCount { current },
            dynamics::Error::Insert(error) => Self::TableInsert(error),
        }
    }
}

const _: () = assert!(core::mem::size_of::<Error>() <= 4 * core::mem::size_of::<usize>());
