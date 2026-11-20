//! HTTP/1 endpoint routing.

use std::mem;

use sark_http::h1::head::response::{Date, Server};

pub mod duplex;
mod pending;
mod stream;
pub mod sync;
pub mod task;

pub use pending::Pending;

/// HTTP/1-only response policy selected after routing.
#[derive(Clone, Copy)]
pub struct Policy {
    pub mode: Mode,
    pub date: Date,
    pub server: Server,
}

impl Policy {
    pub const fn new(mode: Mode, date: Date, server: Server) -> Self {
        Self { mode, date, server }
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum Mode {
    Plain,
    Gzip,
    RequiredGzip,
}

const _: () = assert!(mem::size_of::<Policy>() == 3 * mem::size_of::<bool>());

#[doc(hidden)]
pub trait PolicyOf {
    const POLICY: Policy;
}
