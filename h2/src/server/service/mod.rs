//! Synchronous handlers compiled into the canonical HTTP/2 router path.

mod bounded;
mod handler;
mod prepared;
mod request;
mod route;
mod selected;

pub use bounded::Bounded;
pub use handler::Handler;
#[doc(hidden)]
pub use prepared::Prepared;
pub use request::Request;
pub use route::Route;
use sark_http::payload::response::dispatch;
#[doc(hidden)]
pub(in crate::server) use selected::Selected;

/// A synchronous function whose response supports every request and storage lifetime.
/// Use [`Handler`] directly when the response borrows from its request or handler.
#[derive(Debug)]
pub struct FromFn<F> {
    function: F,
}

impl<F> FromFn<F> {
    pub const fn new(function: F) -> Self {
        Self { function }
    }
}

impl<F, O> Handler for FromFn<F>
where
    F: for<'req, 'd, 'id> Fn(Request<'req, 'd, 'id>) -> O,
    O: for<'req, 'd> dispatch::Immediate<'req, 'd>,
{
    type Output<'req, 'd>
        = O
    where
        Self: 'req,
        'd: 'req;

    type Egress<'d> = <O as dispatch::Immediate<'d, 'd>>::Bound;

    fn handle<'req, 'd, 'id>(&'req self, request: Request<'req, 'd, 'id>) -> O
    where
        'd: 'req,
    {
        (self.function)(request)
    }
}
