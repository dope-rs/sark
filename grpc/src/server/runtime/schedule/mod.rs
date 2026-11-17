//! Context and exact errors for scoped native task scheduling.

mod context;
pub mod start;
pub mod wake;

pub use context::Context;
pub use dope_fiber::task::storage::set::Error;
