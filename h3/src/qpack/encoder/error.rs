use core::mem;

use o3::alloc;
use sark_http::{
    field::metadata::huffman::source,
    payload::compress::dynamic::table::delta::{self, buffer},
};

use crate::{frame, qpack::dynamics};

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Error {
    Allocation(alloc::Error),
    Huffman(source::Overflow),
    Buffer(buffer::Error),
    Delta(delta::Fault),
    Capacity {
        left: usize,
        right: usize,
    },
    FieldCapacity {
        required: usize,
        limit: usize,
    },
    FieldSection {
        remaining: u64,
        name: usize,
        value: usize,
    },
    ConnectDisabled,
    Table(dynamics::Capacity),
    WireWrite {
        expected: usize,
        written: usize,
        additional: usize,
    },
    WireLength {
        expected: usize,
        actual: usize,
    },
}

impl From<dynamics::Capacity> for Error {
    fn from(error: dynamics::Capacity) -> Self {
        Self::Table(error)
    }
}

impl From<alloc::Error> for Error {
    fn from(error: alloc::Error) -> Self {
        Self::Allocation(error)
    }
}

impl From<source::Overflow> for Error {
    fn from(error: source::Overflow) -> Self {
        Self::Huffman(error)
    }
}

impl From<buffer::Error> for Error {
    fn from(error: buffer::Error) -> Self {
        Self::Buffer(error)
    }
}

impl From<frame::WriteError> for Error {
    fn from(error: frame::WriteError) -> Self {
        match error {
            frame::WriteError::Write {
                expected,
                written,
                additional: added,
            } => Self::WireWrite {
                expected,
                written,
                additional: added,
            },
            frame::WriteError::Length { expected, actual } => Self::WireLength { expected, actual },
        }
    }
}

const _: () = assert!(mem::size_of::<Error>() == 4 * mem::size_of::<usize>());
