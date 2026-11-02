use o3::buffer::resident::bound;
use sark_http::payload::response::dispatch;

use crate::server::service::{Bounded, Request};

/// Synchronous handler whose output remains in the request's lifetime domain.
pub trait Handler {
    type Output<'req, 'd>: dispatch::Immediate<'req, 'd>
    where
        Self: 'req,
        'd: 'req;

    /// Maximum resident backing bytes retained by one response owner.
    type Egress<'d>: bound::Bound + 'd;

    fn handle<'req, 'd, 'id>(&'req self, request: Request<'req, 'd, 'id>) -> Self::Output<'req, 'd>
    where
        'd: 'req;

    /// Selects the resident bound enforced by the server's egress admission.
    fn with_bound<const N: usize>(self) -> Bounded<Self, N>
    where
        Self: Sized,
    {
        Bounded::new(self)
    }
}
