//! Generation-bound, driver-local gRPC client capabilities.

pub mod api;
pub mod channel;
#[doc(hidden)]
pub mod conn;
mod driver;
mod error;
pub mod handle;
mod ops;
pub mod settings;

pub use driver::Driver;
pub use error::Error;
