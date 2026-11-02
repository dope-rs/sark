use o3::buffer::storage;
use sark_core::{body::admit::Length, route};
use sark_http::h1::field::expect;

use crate::{
    session,
    wire::{
        head, parsing,
        parsing::{framing, framing::Parsed},
        response,
    },
};

pub(crate) struct Pending<R: route::Spec, C = framing::Plain> {
    pub(crate) head: Parsed<storage::Shared, R>,
    pub(crate) state: session::BodyAwait,
    pub(crate) expectation: expect::Decision,
    pub(crate) declared_body_len: Length,
    pub(crate) connection: head::Connection,
    pub(crate) response: response::Context,
    pub(crate) coding: C,
}

impl<R: route::Spec, C> Pending<R, C> {
    pub(in crate::wire::parsing::framing) fn new(
        base: framing::Head<'_, '_, R, C>,
        consumed: usize,
        length: Length,
    ) -> Result<Self, parsing::Request<R::Request>> {
        let head = base.fields.retain()?;
        Ok(Self {
            head,
            state: session::BodyAwait::new(consumed),
            expectation: base.expectation,
            declared_body_len: length,
            connection: base.connection,
            response: base.response,
            coding: base.coding,
        })
    }
}
