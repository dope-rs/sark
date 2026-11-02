//! Fiber-free HTTP/1 framing, batching, and transport-independent decisions.

use sark_http::payload::fixed;

mod input;
pub mod pump;
pub mod route;
pub mod server;
pub mod session;
pub mod switch;
pub mod wire;

pub use input::Input;
use sark_http::h1;

/// Canonical zero-copy interim response emitted only when an admitted request
/// actually suspends for body input.
pub const CONTINUE_100: fixed::Body = fixed::Body::new(h1::CONTINUE_100);

/// Maximum non-adjacent payload ranges retained by one chunked request.
pub const MAX_CHUNK_PAYLOAD_RANGES: usize = 1024;
