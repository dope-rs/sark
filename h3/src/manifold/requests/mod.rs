//! HTTP/3 request admission and execution-neutral lifecycle state.

pub(in crate::manifold) mod book;
pub(in crate::manifold) mod duplex;
pub(in crate::manifold) mod entries;
mod error;
pub(in crate::manifold) mod exchange;
mod id;
pub(in crate::manifold) mod task;

pub(in crate::manifold) use error::Error;
pub(in crate::manifold) use id::{Id, Identity};
