//! Application processing within one collected response's decode deadline.

use dope::core::driver::route::{Identity, KeyTag};
use dope_fiber::{abi, task::time::budget};
use sark_client::reply;
use sark_http::method::Method;

use crate::client::Request;

/// Exact application failure or cooperative deadline expiration.
#[derive(Debug)]
pub enum Error<E> {
    Work(E),
    Elapsed(budget::Elapsed),
}

impl<E> Error<E> {
    /// Maps the application failure while retaining the exact expiration.
    pub fn map_work<T>(self, map: impl FnOnce(E) -> T) -> Error<T> {
        match self {
            Self::Work(error) => Error::Work(map(error)),
            Self::Elapsed(elapsed) => Error::Elapsed(elapsed),
        }
    }
}

impl<E> From<budget::Elapsed> for Error<E> {
    fn from(elapsed: budget::Elapsed) -> Self {
        Self::Elapsed(elapsed)
    }
}

/// Processes the collected response under the same deadline as content decoding.
pub trait Process<'app, 'd: 'app, ID: Identity = KeyTag<0>>: Request<'app, 'd, ID> {
    /// Application failures remain inner errors; successful work is checked before return.
    fn process<'r, T, E, F>(
        &'r self,
        method: Method,
        path: &'r str,
        headers: &'r [(&'r str, &'r str)],
        body: &'r [u8],
        process: F,
    ) -> impl abi::Fiber<'d, Output = Result<Result<T, E>, Self::Error>> + 'r
    where
        F: FnOnce(reply::Scoped<'app, 'd>, budget::Check<'d>) -> Result<T, Error<E>> + 'r,
        T: 'r,
        E: 'r;
}
