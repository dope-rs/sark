//! Printable field text encoding and in-place percent decoding.
//! Encoding escapes percent and bytes outside printable ASCII. Decoding keeps
//! malformed escapes literal and replaces each invalid UTF-8 byte with `?`.

pub mod decoder;
mod error;
mod length;
mod octet;
pub mod raw;
mod scan;
mod utf8;

pub use error::Error;
pub use length::Length;
