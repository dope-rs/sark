use o3::{buffer::storage, mem::rc};
use sark_h2::server::service::{Handler, Request};
use sark_http::{
    o3::buffer::resident::bound,
    payload::{
        fixed::Body,
        response::{dispatch, fixed, headers::Entry, statics, status::Code},
    },
};

use crate::common::headers::TEXT;

pub struct Repeater(pub storage::Shared);

impl Repeater {
    pub fn try_reply<'a>(
        &self,
    ) -> Result<fixed::Direct<[Entry<'a>; 1], storage::Shared>, rc::Error> {
        Ok(fixed::Direct::new(Code::OK, [TEXT], self.0.try_clone()?))
    }
}

impl Handler for Repeater {
    type Egress<'d> = bound::Unbounded;
    type Output<'req, 'd>
        = dispatch::Either<
        fixed::Direct<[Entry<'req>; 1], storage::Shared>,
        statics::Direct<'d, [Entry<'req>; 0]>,
    >
    where
        Self: 'req,
        'd: 'req;

    fn handle<'req, 'd, 'id>(&'req self, _: Request<'req, 'd, 'id>) -> Self::Output<'req, 'd>
    where
        'd: 'req,
    {
        match self.try_reply() {
            Ok(reply) => dispatch::Either::Left(reply),
            Err(rc::Error::Exhausted) => dispatch::Either::Right(statics::Direct::new(
                Code::SERVICE_UNAVAILABLE,
                [],
                Body::new(b""),
            )),
        }
    }
}
