use core::mem;

use crate::h1::chunk::{axis::Axis, ban, syntax::Syntax};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    Syntax {
        offset: usize,
        syntax: Syntax,
        actual: u8,
    },
    Forbidden {
        offset: usize,
        field: ban::Field,
    },
    Limit {
        axis: Axis,
        actual: usize,
        maximum: usize,
    },
    Overflow {
        axis: Axis,
        value: usize,
        added: usize,
    },
}

const _: () = {
    assert!(mem::size_of::<Error>() <= 3 * mem::size_of::<usize>());
    assert!(!mem::needs_drop::<Error>());
};
