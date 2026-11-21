//! Synchronous HTTP/1 route execution over borrowed protocol storage.
use core::ops::ControlFlow;

use o3::buffer::write::Slice;

use sark_core::{body, body::credit, manifold, route, route::Project as _};
use sark_http::{
    owner::handoff,
    payload::{
        compress,
        response::{Body as _, dispatch},
    },
};

use crate::{
    route::{Pending, PolicyOf},
    wire::{Failure, head, output, parsing::Rejection},
};

impl<'d, R: route::Spec> Pending<'d, R> {
    pub fn respond<S, E>(
        self,
        body_len: usize,
        state: &S,
        write: Slice<'_, &mut usize>,
        date: &[u8; 29],
        gzip: &mut impl compress::Compression,
        mut proof: E,
    ) -> Result<(output::Complete<'d>, head::Connection), Rejection<'d>>
    where
        E: credit::Evidence,
        R: manifold::Route<S> + route::Spec<Kind = dispatch::Sync> + PolicyOf,
    {
        let Self {
            head,
            response,
            content_length,
            body: mut owner,
        } = self;
        let mut body = Self::admit(&mut owner, body_len, content_length, &mut proof)?;
        let ready = <<R::Request as route::Schema>::Body as body::Project>::try_project(
            &mut body,
            (
                head.borrow(),
                state,
                output::Egress::new(write, date, response),
                gzip,
            ),
            |scope| {
                let ((request, state, output, gzip), view) = scope.into_parts();
                let input = view.body();
                let request =
                    R::Request::try_view(&request, view).map_err(Failure::route::<R::Request>)?;
                output.try_stage(
                    R::invoke(request, state),
                    handoff::Slot::new(input),
                    gzip,
                    R::POLICY,
                )
            },
        )
        .map_err(|failure| Rejection::Failed(Failure::Projection(failure.discard())))?
        .map_err(Rejection::Failed)?;
        let ready = match ready {
            ControlFlow::Continue(ready) => {
                drop(owner);
                ready
            }
            ControlFlow::Break(declared) => {
                let body = owner;
                let actual = body.len();
                if actual != declared {
                    return Err(Rejection::Failed(Failure::Admission(
                        body::Error::LengthMismatch { actual, declared },
                    )));
                }
                output::Complete::Linked { body }
            }
        };
        Ok((ready, response.connection()))
    }
}
