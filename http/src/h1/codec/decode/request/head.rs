use core::convert::Infallible;
use std::num::NonZero;

use crate::h1::{
    codec::decode::fields,
    field, framing,
    head::{Limit, ParseError, line, request},
};

/// Complete request head borrowing the exact scanned wire owner.
pub struct Head<'input, F> {
    pub method: &'input [u8],
    pub target: &'input [u8],
    pub version: u8,
    pub admission: framing::Admission,
    pub connection: field::Connection,
    pub fields: F,
    pub header_len: NonZero<u32>,
}

impl<'input> Head<'input, ()> {
    pub fn scan(input: &'input [u8], bound: usize) -> Result<Option<Self>, ParseError<Infallible>> {
        let Some(plan) = request::Parts::scan(input, Limit::DEFAULT.with_bytes(bound))? else {
            return Ok(None);
        };
        Ok(Some(Self::from(plan, ())))
    }
}

impl<'input, F: fields::Sink<'input>> Head<'input, F> {
    pub fn parse(
        input: &'input [u8],
        bound: usize,
        mut fields: F,
    ) -> Result<Option<Self>, ParseError<F::Error>> {
        let Some(plan) = request::Parts::parse(input, Limit::DEFAULT.with_bytes(bound), |field| {
            fields.entry(field)
        })?
        else {
            return Ok(None);
        };
        Ok(Some(Self::from(plan, fields)))
    }
}

impl<'input, F> Head<'input, F> {
    fn from(plan: request::Parts<&'input [u8]>, fields: F) -> Self {
        let (method, target) = plan.line();
        let version = match plan.version() {
            line::Version::Ten => 0,
            line::Version::Eleven => 1,
        };
        Self {
            method,
            target,
            version,
            admission: plan.admission(),
            connection: plan.connection(),
            fields,
            header_len: plan.wire_len(),
        }
    }
}
