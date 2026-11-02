//! Allocation-free HTTP/1 syntax, framing, and admission.

pub mod chunk;
pub mod codec;
pub mod field;
pub mod framing;
pub mod head;
pub mod switch;
pub mod target;

/// Canonical response for an admitted `100-continue` expectation.
pub const CONTINUE_100: &[u8; 25] = b"HTTP/1.1 100 Continue\r\n\r\n";

/// Canonical bodyless response establishing a classic CONNECT tunnel.
pub const CONNECT_ESTABLISHED: &[u8] = b"HTTP/1.1 200 Connection Established\r\n\r\n";
