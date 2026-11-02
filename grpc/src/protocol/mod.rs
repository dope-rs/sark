//! gRPC request, response, and metadata field protocol.

mod block;
pub(crate) mod fields;
pub mod request;
pub mod response;
#[doc(hidden)]
pub mod section;
mod sinks;
pub mod trailer;

pub use block::Block;
