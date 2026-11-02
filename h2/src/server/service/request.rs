use o3::buffer::{bytes::scope, pool, pool::owners};
use sark_core::request::Head;
use sark_http::{
    field::metadata::packed,
    request::{Form, body::Ref},
};

type Fields<'id> = packed::Decoded<
    scope::Owner<'id, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>,
>;

/// Borrowed HTTP/2 request view tied to its retained head and body owners.
pub struct Request<'req, 'd: 'req, 'id> {
    head: &'req Head<Fields<'id>>,
    body: Ref<'req, 'd>,
}

impl<'req, 'd: 'req, 'id> Request<'req, 'd, 'id> {
    pub(super) const fn new(head: &'req Head<Fields<'id>>, body: Ref<'req, 'd>) -> Self {
        Self { head, body }
    }

    pub fn header(&self, name: &[u8]) -> Option<&'req [u8]> {
        self.head
            .owner()
            .iter()
            .find(|field| field.name == name)
            .map(|field| field.value)
    }

    pub fn path(&self) -> Option<&'req [u8]> {
        match self.head.form() {
            Form::Authority => None,
            _ => Some(self.head.borrow().path()),
        }
    }

    pub fn authority(&self) -> Option<&'req [u8]> {
        self.head.borrow().authority()
    }

    pub const fn body(&self) -> Ref<'req, 'd> {
        self.body
    }

    pub const fn body_len(&self) -> usize {
        self.body.len()
    }
}
