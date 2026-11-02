use core::num::NonZero;

use sark_core::manifold::task::schedule;

use crate::server::mode::Limit;

/// Failure to construct a bounded server mode.

#[derive(Debug)]
pub enum Error {
    Capacity {
        requested: usize,
        cause: Limit,
    },
    Task {
        requested: NonZero<u32>,
        cause: schedule::Error,
    },
}
