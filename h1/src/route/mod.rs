//! HTTP/1 endpoint routing.

use std::mem;

use sark_http::h1::head::{
    request::Coding,
    response::{Date, Server},
};

pub mod duplex;
mod pending;
pub mod stream;
pub mod sync;
pub mod task;

/// HTTP/1-only response policy selected after routing.
#[derive(Clone, Copy)]
pub struct Policy {
    pub coding: Coding,
    pub date: Date,
    pub server: Server,
}

impl Policy {
    pub const fn new(coding: Coding, date: Date, server: Server) -> Self {
        Self {
            coding,
            date,
            server,
        }
    }
}

const _: () = assert!(mem::size_of::<Policy>() == 3 * mem::size_of::<bool>());

#[doc(hidden)]
pub trait PolicyOf {
    const POLICY: Policy;
}
