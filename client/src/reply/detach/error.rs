use std::mem;

use buffer::{segments, write::consume};
use o3::{alloc, buffer, mem::rc};

use crate::reply::detach::{Part, Parts};

/// Exact failure while detaching a response from its driver lifetime.
#[derive(Debug)]
pub enum Error {
    Retain {
        part: Part,
        cause: rc::Error,
    },
    Overflow {
        part: Part,
        current: usize,
        added: usize,
    },
    StorageCapacity {
        parts: Parts,
        cause: consume::Error,
    },
    StorageExhausted {
        parts: Parts,
        requested: usize,
        cause: alloc::Exhausted,
    },
    Metadata {
        segments: segments::Count,
        cause: alloc::Error,
    },
    Partition {
        part: Part,
        offset: usize,
        requested: usize,
        capacity: usize,
    },
    Remainder {
        consumed: usize,
        total: usize,
    },
    BodyCount {
        expected: segments::Count,
        actual: segments::Count,
    },
    MetadataCommit {
        segment: segments::Count,
        cause: segments::Error,
    },
}

const _: () = assert!(mem::size_of::<Error>() == 4 * mem::size_of::<usize>());
