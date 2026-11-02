use core::{convert, num::NonZero};

use route::{Push as _, TaskBuild as _};
use sark_http::payload::owned;

use crate::{body, request, route};

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

/// Route-specific bridge into cooperative request construction.
#[doc(hidden)]
#[repr(transparent)]
pub struct Build<'d, R: route::Spec<Request: route::Own>> {
    builder: <R::Request as route::Own>::TaskBuilder<'d>,
}

impl<'d, R: route::Spec<Request: route::Own>> Build<'d, R> {
    pub fn new(builder: <R::Request as route::Own>::TaskBuilder<'d>) -> Self {
        Self { builder }
    }
}

impl<'d, R: route::Spec<Request: route::Own>> crate::body::Build<'d> for Build<'d, R>
where
    <R::Request as route::Own>::TaskBuilder<'d>: route::TaskBuild<'d>,
{
    type Output = route::Built<'d, <R::Request as route::Own>::Task<'d>>;
    type StepError =
        <<R::Request as route::Own>::TaskBuilder<'d> as route::TaskBuild<'d>>::StepError;

    fn finish_input(self, body_len: usize) -> Self {
        Self::new(self.builder.finish_body(body_len))
    }

    fn step(
        self,
        budget: NonZero<usize>,
    ) -> crate::body::Step<Self, Self::Output, Self::StepError> {
        self.builder
            .step(budget)
            .map(Self::new, convert::identity, convert::identity)
    }
}

impl<'d, R: route::Spec<Request: route::Own>> body::Push<'d> for Build<'d, R>
where
    <R::Request as route::Own>::TaskBuilder<'d>: route::Push<'d>,
{
    type Error = <<R::Request as route::Own>::TaskBuilder<'d> as route::Push<'d>>::Error;

    fn observe<C: Copy, S: Copy, O: crate::life::Observer<C, S>>(
        error: &Self::Error,
        observer: &mut O,
    ) {
        <<R::Request as route::Own>::TaskBuilder<'d> as route::Push<'d>>::observe(error, observer);
    }

    fn push(self, segment: owned::Segment<'d>) -> body::Transfer<Self, Self::Error> {
        self.builder.push_body(segment).map(Self::new)
    }
}
