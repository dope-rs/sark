use std::alloc::Layout;

use o3::alloc;

use crate::payload::compress::dynamic::index;

mod append;
mod insert;
mod plan;
mod record;
mod search;
mod storage;
mod view;

pub(in crate::payload::compress::dynamic::table::delta) use append::{Append, Ready};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Bytes {
        actual: usize,
        maximum: usize,
    },
    Entries {
        actual: usize,
        maximum: usize,
    },
    Index {
        entries: usize,
        cause: index::Error,
    },
    Records {
        entries: usize,
        width: usize,
    },
    IndexEnd {
        start: usize,
        len: usize,
    },
    DataEnd {
        start: usize,
        len: usize,
    },
    Resident {
        bytes: usize,
        cause: alloc::Overflow,
    },
    Allocation {
        layout: Layout,
        cause: alloc::Error,
    },
}
pub(super) use search::Search;
pub use storage::Storage;

const _: () = assert!(core::mem::size_of::<Storage>() == core::mem::size_of::<usize>());
