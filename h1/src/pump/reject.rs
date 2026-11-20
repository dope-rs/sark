use std::mem;

use o3::{alloc, buffer::write::consume, mem::rc};
use sark_http::h1::switch::Kind;

use crate::{pump::Operation, switch};

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

const _: () = {
    assert!(mem::size_of::<Reject>() <= 3 * mem::size_of::<usize>());
    assert!(!mem::needs_drop::<Reject>());
};
