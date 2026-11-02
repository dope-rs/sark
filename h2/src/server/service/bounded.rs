use o3::buffer::resident::bound;

use crate::server::service::{Handler, Request};

/// A handler with an explicit resident response bound.
///
/// The output keeps its original lifetime domain. The server's existing egress
/// admission rejects owners exceeding the bound before transmission.
#[derive(Debug)]
#[repr(transparent)]
pub struct Bounded<H, const N: usize> {
    handler: H,
}

impl<H, const N: usize> Bounded<H, N> {
    pub(super) const fn new(handler: H) -> Self {
        Self { handler }
    }
}

impl<H: Handler, const N: usize> Handler for Bounded<H, N> {
    type Output<'req, 'd>
        = H::Output<'req, 'd>
    where
        Self: 'req,
        'd: 'req;

    type Egress<'d> = bound::AtMost<N>;

    fn handle<'req, 'd, 'id>(&'req self, request: Request<'req, 'd, 'id>) -> Self::Output<'req, 'd>
    where
        'd: 'req,
    {
        self.handler.handle(request)
    }
}
