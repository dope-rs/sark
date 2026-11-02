//! Request frame projection failure.

use std::{convert, mem};

use o3::mem::rc;

#[doc(hidden)]
pub mod proof;
pub mod raw;
mod seal;

pub(super) use seal::Seal;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error<R = convert::Infallible> {
    Order {
        start: usize,
        end: usize,
        capacity: usize,
    },
    Bounds {
        start: usize,
        end: usize,
        capacity: usize,
    },
    Retain(R),
}

const _: () = {
    assert!(mem::size_of::<Error>() == mem::size_of::<usize>() * 4);
    assert!(mem::size_of::<Option<Error>>() == mem::size_of::<Error>());
    assert!(mem::size_of::<Result<(), Error>>() == mem::size_of::<Error>());
    assert!(!mem::needs_drop::<Error>());
    assert!(mem::size_of::<Error<rc::Error>>() == mem::size_of::<Error>());
};
