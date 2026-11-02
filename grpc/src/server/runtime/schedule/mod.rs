//! Context and exact errors for scoped native task scheduling.

mod context;
mod error;
pub mod start;
pub mod wake;

pub use context::Context;
pub use error::Error;
