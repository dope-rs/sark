//! Fiber-backed HTTP client.
//!
//! Requests borrow their inputs until completion. Responses retain the installed
//! application's metadata pool and the original driver-bound receive owners.

use dope::core::driver::route::{Identity, KeyTag};
use dope_fiber::abi;
use sark_client::reply;
use sark_http::method::Method;

pub mod channel;
pub mod config;
pub mod conn;
pub mod dynamic;
pub(in crate::client) mod exchange;
pub mod process;
pub mod request;
mod stream;

pub use stream::Stream;

type Covariant<T> = std::marker::PhantomData<fn() -> T>;

/// Configured authority retaining the original provider borrow.
pub trait Host<'app> {
    fn host(&self) -> &'app str;
}

/// HTTP requests retaining the provider's exact response and failure owners.
/// Caller headers and bodies are bound to the configured origin. Dynamic
/// connectors admit other origins only for requests without either.
pub trait Request<'app, 'd: 'app, ID: Identity = KeyTag<0>> {
    type Error;

    fn send_with<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        headers: &'r [(&'r str, &'r str)],
        body: &'r [u8],
    ) -> impl abi::Fiber<'d, Output = Result<reply::Scoped<'app, 'd>, Self::Error>> + 'r;
}

/// Requests that return the first decoded response without following redirects.
/// Transport retries retain the provider's ordinary request safety policy.
pub trait Once<'app, 'd: 'app, ID: Identity = KeyTag<0>>: Request<'app, 'd, ID> {
    fn send_once<'r>(
        &'r self,
        method: Method,
        path: &'r str,
        headers: &'r [(&'r str, &'r str)],
        body: &'r [u8],
    ) -> impl abi::Fiber<'d, Output = Result<reply::Scoped<'app, 'd>, Self::Error>> + 'r;
}

/// Readiness of an installed connector and its exact registration failure.
pub trait Ready<'d> {
    type Error;

    fn connections(&self) -> usize;

    fn wait_active<'r>(&'r self) -> impl abi::Fiber<'d, Output = Result<(), Self::Error>> + 'r;
}
