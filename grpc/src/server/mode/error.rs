use core::num::NonZero;

use crate::server::{mode::Limit, runtime::schedule};

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
