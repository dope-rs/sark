pub mod chunks;
mod credits;
pub use credits::Credits;
pub mod replies;
mod retain;
use std::cell::Cell;

use o3::buffer::write::{Sink, consume};
pub use retain::Retain;
use sark_http::{
    field,
    payload::response::{
        Bounded, Encode, Exact, bounded, exact,
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

impl<'a> Counted<'a> {
    pub fn exact<'h>(
        template: &'h Template<'h>,
        bytes: &'a [u8],
        encodes: &'a Cell<usize>,
    ) -> Exact<headers::head::Head<'h, 0>, Self> {
        Exact::new(
            Code::OK,
            headers::head::Head::new(template, headers::set::Set::new()),
            Self { bytes, encodes },
        )
    }

    pub fn bounded(
        bytes: &'a [u8],
        encodes: &'a Cell<usize>,
    ) -> Bounded<headers::head::Head<'static, 0>, Self> {
        let body = Self { bytes, encodes };
        Bounded::new(
            Code::OK,
            headers::head::Head::new(&TEMPLATE, headers::set::Set::new()),
            body,
        )
    }
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
