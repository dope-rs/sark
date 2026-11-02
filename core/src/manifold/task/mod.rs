//! Protocol-neutral task capabilities owned by a multiplexed application.

use crate::manifold;

pub mod app;
mod error;
pub mod request;
pub mod response;

pub use error::Error;

/// A protocol-minted target accepted by multiplexed task storage.
#[doc(hidden)]
pub trait Target: manifold::raw::Target + Copy + Eq {}
