use std::{mem, result};

use o3::{
    alloc,
    buffer::{segments, storage, write::consume},
    mem::rc,
};

mod bytes;
pub mod input;
mod nesting;
pub mod parsing;
pub mod segment;
mod source;
mod token;
pub mod writing;

pub use bytes::{
    Bytes,
    proofs::{Backing, Plain, PlainError, Raw, Text, TextValue},
};
pub(crate) use nesting::Nesting;
#[doc(hidden)]
pub use o3;
/// Derives decoding/encoding on nongeneric structs; `encode`/`try_encode` also accept generics.
/// `seq, nested` accepts `&Owner: IntoIterator` of owned/borrowed encoders; decoding requires `Vec<T>`.
/// Exact writes are checked independently of sizing, even if the owner changes between traversals.
pub use sark_gen::json;
#[doc(hidden)]
pub use sark_http;
pub use source::Source;
pub use token::Token;

#[derive(Debug)]
pub enum Error {
    Invalid,
    Depth,
    Token(consume::Error),
    Storage(Storage),
    Allocation(alloc::Error),
    Exhausted(alloc::Exhausted),
    Retain(rc::Error),
    InputLength {
        current: usize,
        add: usize,
    },
    InputSegments {
        current: segments::Count,
        add: segments::Count,
    },
    OutputLength(consume::Error),
}

#[derive(Debug)]
pub enum Storage {
    Capacity(consume::Error),
    Exhausted(alloc::Exhausted),
}

impl From<storage::AllocError> for Error {
    fn from(error: storage::AllocError) -> Self {
        Self::Storage(match error {
            storage::AllocError::Capacity(error) => Storage::Capacity(error),
            storage::AllocError::Exhausted(error) => Storage::Exhausted(error),
        })
    }
}

type Result<T> = result::Result<T, Error>;

const _: () = assert!(mem::size_of::<Error>() == mem::size_of::<usize>() * 3);
