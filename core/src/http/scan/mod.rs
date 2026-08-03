//! Safe byte-scanning primitives for HTTP syntax.
//!
//! Architecture-specific vector code stays behind this semantic facade so
//! callers do not depend on its implementation strategy or safety proof.

mod scalar;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod aarch64;
#[cfg(target_arch = "x86_64")]
mod x86_64;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod backend {
    pub(super) use super::aarch64::{request_target_is_valid, scan_header_name, scan_header_value};
}
#[cfg(not(any(
    all(target_arch = "aarch64", target_feature = "neon"),
    target_arch = "x86_64"
)))]
mod backend {
    pub(super) use super::scalar::{request_target_is_valid, scan_header_name, scan_header_value};
}
#[cfg(target_arch = "x86_64")]
mod backend {
    pub(super) use super::x86_64::{request_target_is_valid, scan_header_name, scan_header_value};
}

#[derive(Debug, PartialEq, Eq)]
pub enum HeaderNameOutcome {
    Found { pos: usize, byte: u8 },
    Invalid,
    None,
}

#[derive(Debug, PartialEq, Eq)]
pub enum HeaderValueOutcome {
    Found { pos: usize },
    Invalid,
    None,
}

#[inline]
pub fn scan_header_name(bytes: &[u8], start: usize) -> HeaderNameOutcome {
    if start >= bytes.len() {
        return HeaderNameOutcome::None;
    }
    backend::scan_header_name(bytes, start)
}

#[inline]
pub fn scan_header_value(bytes: &[u8], start: usize) -> HeaderValueOutcome {
    if start >= bytes.len() {
        return HeaderValueOutcome::None;
    }
    backend::scan_header_value(bytes, start)
}

#[inline]
pub fn request_target_is_valid(bytes: &[u8]) -> bool {
    backend::request_target_is_valid(bytes)
}
