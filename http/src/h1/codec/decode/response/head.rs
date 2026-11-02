use std::num::NonZero;

use o3::alloc::{self, BoxSliceExt as _};

use crate::{
    h1::{framing, head::response},
    payload::response::{Response, headers::List, status},
};

use super::Collected;

pub struct Head<F> {
    pub version: u8,
    pub status: status::Code,
    pub fields: F,
    pub header_len: NonZero<u32>,
    pub payload: framing::ResponseBody,
    pub control: framing::Control,
}

impl<F> Head<F> {
    pub(super) fn from(head: response::Admitted, fields: F) -> Self {
        Self {
            version: head.version(),
            status: head.status(),
            fields,
            header_len: head.wire_len(),
            payload: head.body(),
            control: head.control(),
        }
    }
}

impl<'field> Head<List<'field>> {
    pub(super) fn try_copy(self, body: &[u8]) -> Result<Collected<'field>, alloc::Exhausted> {
        Ok(self.complete(Box::try_copy(body)?.into_vec(), List::new()))
    }

    pub(super) fn complete<B>(self, body: B, trailers: List<'field>) -> Collected<'field, B> {
        Response {
            status: self.status,
            headers: self.fields,
            trailers,
            body,
        }
    }
}
