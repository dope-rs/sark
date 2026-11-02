//! Retained state for an incomplete synchronous request.
use o3::buffer::{segments::assembly, storage};
use sark_core::{
    body,
    body::{admit::Length, credit},
    decode::Call,
    manifold, route,
};
use sark_http::{
    h1::{codec::canned, head::request::Coding},
    owner::handoff,
    payload::{compress, owned, response::dispatch},
};

use crate::{
    route::{PolicyOf, pending, sync::Egress},
    session,
    wire::{Failure, head, output, parsing::framing, response},
};

/// Route-specific state retained while the complete body frame is unavailable.
pub struct Pending<'d, R: route::Spec> {
    head: framing::Parsed<storage::Shared, R>,
    connection: head::Connection,
    response: response::Context,
    coding: Coding,
    body: pending::Body<'d>,
    content_length: Length,
}

impl<'d, R: route::Spec> Pending<'d, R> {
    pub(crate) fn from_frame(frame: framing::Pending<R, Coding>) -> Self {
        Self {
            head: frame.head,
            connection: frame.connection,
            response: frame.response,
            coding: frame.coding,
            body: pending::Body::new(),
            content_length: frame.declared_body_len,
        }
    }

    pub const fn response(&self) -> response::Context {
        self.response
    }

    pub fn push_body<E: credit::Evidence>(
        &mut self,
        parts: owned::Segments<'d>,
        proof: E,
    ) -> Result<(), assembly::Error> {
        self.body.push(parts, proof)
    }

    /// Framing admission bounded this immutable head by HEAD_CAPACITY.
    pub fn head_len(&self) -> u32 {
        self.head.owner().as_ref().len() as u32
    }

    pub fn cancel<E: credit::Evidence>(self, proof: E) {
        self.body.discard(proof);
    }

    pub fn finish<S, E, D>(
        self,
        done: session::Complete,
        permit: session::Permit,
        state: &S,
        egress: Egress<'_, '_, impl compress::Compression>,
        mut proof: E,
    ) -> session::Outcome<'d, D>
    where
        E: credit::Evidence,
        R: manifold::Route<S> + route::Spec<Kind = dispatch::Sync> + PolicyOf,
    {
        let Self {
            head,
            connection: close,
            response,
            coding,
            body,
            content_length: want,
        } = self;
        let session::Complete {
            body_len: total,
            consumed: used,
        } = done;
        if !body.matches(<R::Request as route::Schema>::BODY_RETENTION, total) {
            body.discard(proof);
            return session::Outcome::Close(canned::BAD_REQUEST);
        }
        let Egress {
            output,
            mut cache,
            gzip,
        } = egress;
        let mut output = output.with_context(response);
        if let Some(ready) = output.cached(&mut cache, R::POLICY.cache) {
            body.discard(proof);
            return ready.into_consume(
                permit,
                session::Consumption::Buffered(session::Buffered::new(used)),
                close,
            );
        }
        let mut owner = body;
        let mut body = match owner.try_admit::<R, E>(total, want, &mut proof) {
            Ok(body) => body,
            Err(error) => return session::Outcome::Failed(Failure::reject(error)),
        };
        let ready = match <<R::Request as route::Schema>::Body as body::Project>::try_project(
            &mut body,
            (head.borrow(), state, output, cache, gzip, coding),
            |scope| -> Result<output::Outcome<'d, usize>, Failure> {
                let ((request, state, output, cache, gzip, coding), view) = scope.into_parts();
                let input = view.body();
                let call = Call::new(request, view);
                let reply = call.try_invoke::<R, S>(state).map_err(|error| {
                    Failure::Route(<R::Request as route::Project>::reject(error))
                })?;
                Ok(output.route(
                    reply,
                    handoff::Slot::new(input),
                    cache,
                    gzip,
                    coding,
                    R::POLICY,
                ))
            },
        ) {
            Ok(Ok(ready)) => ready,
            Ok(Err(error)) => return session::Outcome::Failed(error),
            Err(failure) => {
                return session::Outcome::Failed(Failure::Projection(failure.discard()));
            }
        };
        let ready = ready.link(owner.finish());
        ready.into_consume(
            permit,
            session::Consumption::Buffered(session::Buffered::new(used)),
            close,
        )
    }
}
