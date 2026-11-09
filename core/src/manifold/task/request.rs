use crate::{request, route};

/// Protocol-neutral input entering one generated task handler.
#[doc(hidden)]
pub enum Input<'d, R: route::Spec<Request: route::Own>> {
    Buffered(
        request::raw::Parsed<
            request::Retained<'d, <R::Request as route::Schema>::Frame>,
            R::Request,
        >,
    ),
    Ready {
        request: route::Built<'d, <R::Request as route::Own>::Task<'d>>,
    },
}
