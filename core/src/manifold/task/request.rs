use crate::{request, route};

/// Protocol-neutral input entering one generated task handler.
#[doc(hidden)]
pub enum Input<'d, R: route::Own> {
    Buffered(request::raw::Parsed<request::Retained<'d, R::Frame>, R>),
    Ready {
        request: route::Built<'d, R::Task<'d>>,
    },
}

impl<'d, R: route::Own> From<route::Built<'d, R::Task<'d>>> for Input<'d, R> {
    fn from(request: route::Built<'d, R::Task<'d>>) -> Self {
        Self::Ready { request }
    }
}

impl<'d, R: route::Own> From<request::raw::Parsed<request::Retained<'d, R::Frame>, R>>
    for Input<'d, R>
{
    fn from(request: request::raw::Parsed<request::Retained<'d, R::Frame>, R>) -> Self {
        Self::Buffered(request)
    }
}
