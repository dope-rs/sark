use std::{convert, mem};

use crate::{request::field, route};

/// Exact failure while decoding generated route fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error<F: Copy> {
    Header {
        field: F,
        cause: field::Error,
    },
    Path {
        field: F,
        cause: field::Error,
    },
    Query {
        field: F,
        cause: field::Error,
    },
    QueryOrder {
        start: usize,
        end: usize,
        capacity: usize,
    },
    QueryBounds {
        start: usize,
        end: usize,
        capacity: usize,
    },
    QueryExhausted {
        state: u8,
        fields: usize,
        start: usize,
        end: usize,
    },
}

impl<F: Copy> Error<F> {
    pub const fn reject(self) -> route::Reject {
        route::Reject::BadRequest
    }
}

const _: () = {
    assert!(!mem::needs_drop::<Error<convert::Infallible>>());
};
