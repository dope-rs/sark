//! HTTP/1 endpoint routing.

use std::mem;

use sark_core::route::table;
use sark_http::{
    h1::head::{
        request::Coding,
        response::{Date, Server},
    },
    payload::fixed,
};

use crate::wire::Failure;

mod driver;
pub mod duplex;
mod pending;
pub mod set;
pub mod stream;
pub mod sync;
pub mod task;

pub use driver::Driver;

/// Response-cache action selected for one HTTP/1 route.
#[derive(Clone, Copy)]
#[repr(u8)]
pub enum Cache {
    Bypass,
    Store,
}

/// HTTP/1-only response policy selected after routing.
#[derive(Clone, Copy)]
pub struct Policy {
    pub cache: Cache,
    pub coding: Coding,
    pub date: Date,
    pub server: Server,
}

impl Policy {
    pub const fn new(cache: Cache, coding: Coding, date: Date, server: Server) -> Self {
        Self {
            cache,
            coding,
            date,
            server,
        }
    }
}

const _: () = assert!(mem::size_of::<Policy>() == 4 * mem::size_of::<bool>());
const _: () = assert!(mem::size_of::<Cache>() == mem::size_of::<bool>());

/// Supplies protocol policy without adding HTTP/1 state to a route type.
pub trait Router: table::Router<Table: set::Table<Self::State>> {
    /// Selects the terminal consumer of exact HTTP/1 failures.
    fn failure<'d>(&self, cause: &Failure) -> fixed::Body<'d> {
        cause.body()
    }
}

#[doc(hidden)]
pub trait PolicyOf {
    const POLICY: Policy;
}
