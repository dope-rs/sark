use std::cell::Cell;

use o3::buffer::resident::bound;
use sark_h2::server::service::{Handler, Request};
use sark_http::payload::response::{fixed, status::Code};

pub const BODY_LEN: usize = 64 << 10;

pub struct Oversized<'a, const LIMIT: usize>(pub &'a Cell<usize>);

impl<const LIMIT: usize> Handler for Oversized<'_, LIMIT> {
    type Egress<'d> = bound::AtMost<LIMIT>;
    type Output<'req, 'd>
        = fixed::Direct<(), Vec<u8>>
    where
        Self: 'req,
        'd: 'req;

    fn handle<'req, 'd, 'id>(&'req self, _: Request<'req, 'd, 'id>) -> Self::Output<'req, 'd>
    where
        'd: 'req,
    {
        self.0.set(self.0.get() + 1);
        fixed::Direct::new(Code::OK, (), vec![b'x'; BODY_LEN])
    }
}
