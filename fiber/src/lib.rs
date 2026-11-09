//! Fiber application, transport, and scheduling over Sark protocol crates.

#[doc(hidden)]
pub use dope;
#[doc(hidden)]
pub use dope_fiber;
#[doc(hidden)]
pub use dope_gen;
#[doc(hidden)]
pub use o3;
#[doc(hidden)]
pub extern crate pin_project;

#[doc(hidden)]
pub use sark_core;
pub use sark_core::{manifold::EmptyState, request};
#[doc(hidden)]
pub use sark_h1;
#[doc(hidden)]
pub use sark_http;

pub mod client;
mod config;
pub mod fiber;
pub mod manifold;
pub mod server;

pub use config::Config;
pub use dope::{
    core::driver::{
        self,
        route::{Identity, KeyTag},
    },
    manifold::timing::{Balanced, Policy, Throughput},
    net::{tcp, tcp::Tcp},
    runtime::executor::Executor,
};
pub use dope_fiber::{abi, abi::Fiber, context};
pub use sark_gen::{
    define_cleartext_route, define_route, define_router, define_task_route, duplex, fiber, handler,
    header_name, request, response,
};
