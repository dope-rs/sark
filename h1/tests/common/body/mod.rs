pub mod chunks;
mod credits;
pub use credits::Credits;
pub mod replies;
mod retain;
use std::cell::Cell;

use o3::buffer::{
    resident::bound,
    write::{Sink, consume},
};
pub use retain::Retain;
use sark_http::{
    field,
    payload::response::{
        Encode, bounded, dispatch, exact,
        headers::{self, head::Template},
        status::Code,
    },
};

pub const COMPRESSIBLE: &[u8] = &[b'x'; 512];

pub static TEMPLATE: Template = Template::new(&[headers::Entry::new(
    field::literal!(headers::Name::try_bytes(b"content-type")),
    field::literal!(field::Value::try_bytes(b"application/json")),
)]);

pub struct Counted<'a> {
    pub bytes: &'a [u8],
    pub encodes: &'a Cell<usize>,
}

impl exact::Body for Counted<'_> {
    fn encoded_len(&self) -> usize {
        self.bytes.len()
    }
}

impl Encode for Counted<'_> {
    fn encode<W>(self, out: &mut W) -> Result<(), consume::Error>
    where
        W: Sink<Error = consume::Error>,
    {
        self.encodes.set(self.encodes.get() + 1);
        out.write_slice(self.bytes)
    }
}

impl bounded::Body for Counted<'_> {
    fn max_len(&self) -> usize {
        self.bytes.len().saturating_mul(2).max(1)
    }
}

pub struct Bounded<H, B> {
    pub fields: H,
    pub body: B,
}

impl<'req, 'd, H: headers::Source, B: bounded::Body> dispatch::Complete<'req, 'd>
    for Bounded<H, B>
{
    const BODY: sark_http::payload::Kind = sark_http::payload::Kind::Inline;
    type Bound = bound::Unbounded;

    fn complete<S: dispatch::Accept<'d>>(self, sink: S) -> Result<S::Output, S::Error> {
        sink.bounded(Code::OK, self.fields, self.body)
    }
}
