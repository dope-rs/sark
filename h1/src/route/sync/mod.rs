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
    session,
    wire::{Failure, output},
};

impl<'d, R: route::Spec> Pending<'d, R> {
    pub fn respond<S, E, D>(
        self,
        done: session::Complete,
        state: &S,
        write: Slice<'_, &mut usize>,
        date: &[u8; 29],
        gzip: &mut impl compress::Compression,
        mut proof: E,
    ) -> session::Outcome<'d, D>
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
        let mut body = match Self::admit(&mut owner, done.body_len, content_length, &mut proof) {
            Ok(body) => body,
            Err(error) => return error.into(),
        };
        let ready = match <<R::Request as route::Schema>::Body as body::Project>::try_project(
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
        ) {
            Ok(Ok(ready)) => ready,
            Ok(Err(error)) => return session::Outcome::Failed(error),
            Err(failure) => {
                return session::Outcome::Failed(Failure::Projection(failure.discard()));
            }
        };
        let ready = match ready {
            ControlFlow::Continue(ready) => {
                drop(owner);
                ready
            }
            ControlFlow::Break(declared) => {
                let body = owner;
                let actual = body.len();
                if actual != declared {
                    return session::Outcome::Failed(Failure::Admission(
                        body::Error::LengthMismatch { actual, declared },
                    ));
                }
                output::Complete::Linked { body }
            }
        };
        ready.into_consume(
            session::Consumption::Buffered(done.consumed),
            response.connection(),
        )
    }
}
