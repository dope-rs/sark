use o3::{buffer::storage, mem::rc};
use sark_h2::server::service::{Handler, Request};
use sark_http::{
    o3::buffer::resident::bound,
    payload::{
        fixed::Body,
        response::{Fixed, Static, dispatch, headers::Entry, status::Code},
    },
};

use crate::common::headers::TEXT;

pub struct Repeater(pub storage::Shared);

impl Repeater {
    pub fn try_reply<'a>(&self) -> Result<Fixed<[Entry<'a>; 1], storage::Shared>, rc::Error> {
        Ok(Fixed::new(Code::OK, [TEXT], self.0.try_clone()?))
    }
}

impl Handler for Repeater {
    type Egress<'d> = bound::Unbounded;
    type Output<'req, 'd>
        = dispatch::Either<Fixed<[Entry<'req>; 1], storage::Shared>, Static<'d, [Entry<'req>; 0]>>
    where
        Self: 'req,
        'd: 'req;

    fn handle<'req, 'd, 'id>(&'req self, _: Request<'req, 'd, 'id>) -> Self::Output<'req, 'd>
    where
        'd: 'req,
    {
        match self.try_reply() {
            Ok(reply) => dispatch::Either::Left(reply),
            Err(rc::Error::Exhausted) => {
                dispatch::Either::Right(Static::new(Code::SERVICE_UNAVAILABLE, [], Body::new(b"")))
            }
        }
    }
}
