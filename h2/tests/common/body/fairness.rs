use std::cell::Cell;

use sark_h2::server::service::{Handler, Request};
use sark_http::{
    o3::buffer::resident::bound,
    payload::response::{fixed, headers::Entry, status::Code},
    request::body,
};

use crate::common::headers::TEXT;

pub const BURST: usize = 256;

#[derive(Default)]
pub struct Trace {
    pub flood: Cell<usize>,
    pub seen: Cell<Option<usize>>,
    pub mismatch: Cell<Option<(Option<usize>, Option<usize>)>>,
}

pub struct Fairness<'a, F> {
    pub trace: &'a Trace,
    pub first: F,
}

impl<F: Fn()> Handler for Fairness<'_, F> {
    type Egress<'d> = bound::Unbounded;
    type Output<'req, 'd>
        = fixed::Direct<[Entry<'req>; 1], body::Ref<'req, 'd>>
    where
        Self: 'req,
        'd: 'req;

    fn handle<'req, 'd, 'id>(&'req self, request: Request<'req, 'd, 'id>) -> Self::Output<'req, 'd>
    where
        'd: 'req,
    {
        let body = request.body();
        let pointer = body.as_slice().map(|bytes| bytes.as_ptr().addr());
        if request.path() == Some(&b"/ingress-probe"[..]) {
            self.trace.seen.set(Some(self.trace.flood.get()));
        } else {
            self.trace.flood.set(self.trace.flood.get() + 1);
            if self.trace.flood.get() == 1 {
                (self.first)();
            }
        }
        let response = fixed::Direct::new(Code::OK, [TEXT], body);
        let returned = response
            .body()
            .as_slice()
            .map(|bytes| bytes.as_ptr().addr());
        if (pointer.is_none() || returned != pointer) && self.trace.mismatch.get().is_none() {
            self.trace.mismatch.set(Some((pointer, returned)));
        }
        response
    }
}
