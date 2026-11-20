use core::mem;

use o3::buffer::{segments::assembly, storage};
use sark_core::{
    body::{self, admit, credit},
    route,
};
use sark_http::{
    h1::codec::canned,
    payload::{owned, response::Body as _},
};

use crate::{
    session,
    wire::{
        Failure,
        body::View,
        parsing::{self, framing},
        response,
    },
};

pub struct Pending<'d, R: route::Spec> {
    pub(super) head: framing::Parsed<storage::Shared, R>,
    pub(super) response: response::Context,
    pub(super) content_length: admit::Length,
    pub(super) body: owned::Segments<'d>,
}

impl<'d, R: route::Spec> Pending<'d, R> {
    pub(super) fn start<D, E: credit::Evidence>(
        frame: framing::Pending<R>,
        body: owned::Segments<'d>,
        permit: session::Permit,
        mut proof: E,
    ) -> session::RouteOutcome<'d, Self, D> {
        proof.reserved(E::credit(body.len(), body.count()));
        permit.commit();
        session::RouteOutcome::AwaitBody {
            state: frame.state,
            expectation: frame.expectation,
            pending: Self {
                head: frame.head,
                response: frame.response,
                content_length: frame.declared_body_len,
                body,
            },
        }
    }

    pub const fn response(&self) -> response::Context {
        self.response
    }

    /// Framing admission bounded this immutable head by HEAD_CAPACITY.
    pub fn head_len(&self) -> u32 {
        self.head.owner().as_ref().len() as u32
    }

    pub fn push_body<E: credit::Evidence>(
        &mut self,
        parts: owned::Segments<'d>,
        mut proof: E,
    ) -> Result<(), assembly::Error> {
        let credit = E::credit(parts.len(), parts.count());
        let (body, result) = match mem::take(&mut self.body).try_extend(parts) {
            Ok(body) => {
                proof.reserved(credit);
                (body, Ok(()))
            }
            Err(rejected) => {
                let ((body, _), error) = rejected.into_parts();
                (body, Err(error))
            }
        };
        self.body = body;
        result
    }

    pub(super) fn admit<'a, E: credit::Evidence>(
        body: &'a mut owned::Segments<'d>,
        total: usize,
        content_length: admit::Length,
        proof: &mut E,
    ) -> Result<body::Body<View<'a, 'd>>, parsing::Rejection<'d>> {
        let len = body.len();
        let expected = match <R::Request as route::Schema>::BODY_RETENTION {
            admit::Retention::Discarded => 0,
            admit::Retention::Retained => total,
        };
        let credit = E::credit(len, body.count());
        if len != expected {
            drop(mem::take(body));
            proof.released(credit, credit::Disposition::Discarded);
            return Err(parsing::Rejection::Close(canned::BAD_REQUEST));
        }
        let result = View::Owned(body).finish::<R>(total, content_length);
        proof.released(
            credit,
            match &result {
                Ok(_) => credit::Disposition::Admitted,
                Err(_) => credit::Disposition::Discarded,
            },
        );
        result.map_err(|error| parsing::Rejection::Failed(Failure::reject(error)))
    }

    pub fn cancel<E: credit::Evidence>(self, mut proof: E) {
        let credit = E::credit(self.body.len(), self.body.count());
        drop(self.body);
        proof.released(credit, credit::Disposition::Discarded);
    }
}
