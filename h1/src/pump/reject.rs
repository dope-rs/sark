use std::mem;

use o3::{alloc, buffer::write::consume, mem::rc};
use sark_http::h1::switch::Kind;

use crate::{
    pump::{Cause, Operation},
    switch,
};

/// Exact reason retained ingress could not accept one transport transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reject {
    Capacity {
        operation: Operation,
        cause: consume::Error,
    },
    Consume {
        operation: Operation,
        cause: consume::Error,
    },
    Range {
        operation: Operation,
        offset: usize,
        capacity: usize,
    },
    Exhausted {
        operation: Operation,
        cause: alloc::Exhausted,
    },
    Retain {
        operation: Operation,
        cause: rc::Error,
    },
    Raw(Kind),
    Switch {
        actual: switch::Mode,
        attempted: Kind,
    },
}

impl Reject {
    pub(crate) const fn new(op: Operation, cause: Cause) -> Self {
        match cause {
            Cause::Capacity(cause) => Self::Capacity {
                operation: op,
                cause,
            },
            Cause::Consume(cause) => Self::Consume {
                operation: op,
                cause,
            },
            Cause::Range { offset, capacity } => Self::Range {
                operation: op,
                offset,
                capacity,
            },
            Cause::Exhausted(cause) => Self::Exhausted {
                operation: op,
                cause,
            },
            Cause::Retain(cause) => Self::Retain {
                operation: op,
                cause,
            },
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Reject>() <= 3 * mem::size_of::<usize>());
    assert!(!mem::needs_drop::<Reject>());
};
