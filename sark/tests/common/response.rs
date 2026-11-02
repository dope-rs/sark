use std::{cell::Cell, ops::ControlFlow};

use o3::buffer::{
    storage::{self, Shared},
    write::{self, Sink as _, consume},
};
use sark::http::{
    field::{self, Value},
    payload::{
        fixed, response,
        response::{bounded, dispatch::Accept, exact, headers, status::Code},
    },
};
use sark_json::writing::{Failure, Size, TryEncode, Writer};

#[derive(Debug)]
pub enum Error {
    Fixed,
    Static,
    Bounded,
    Field(consume::Error),
    Size(exact::Overflow),
    Build(storage::BuildError<consume::Error>),
}

pub struct Fields<'out> {
    pub out: write::Staged<'out, usize>,
    pub result: Result<(), consume::Error>,
}

impl<'field> headers::Sink<'field> for Fields<'_> {
    fn entry(&mut self, field: headers::Entry<'field>) {
        if self.result.is_ok() {
            self.result =
                self.out
                    .write_slices([field.name(), b": ", field.value().as_bytes(), b"\r\n"]);
        }
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: headers::Name<&'field [u8]>,
        value: E,
        _: field::Indexing,
    ) {
        if self.result.is_ok() {
            self.result = (|| {
                self.out.write_slices([name.get(), b": "])?;
                value.try_write(|bytes| self.out.write_slice(bytes))?;
                self.out.write_slice(b"\r\n")
            })();
        }
    }
}

pub struct SliceSink<'a, 'out>(pub &'a mut [u8], pub Option<&'a mut Fields<'out>>);

impl<'d> Accept<'d> for SliceSink<'_, '_> {
    type Output = (Code, usize);
    type Error = Error;

    fn fixed<H: headers::Source, B: response::Body<'d>>(
        self,
        _: Code,
        _: H,
        _: B,
    ) -> Result<Self::Output, Self::Error> {
        Err(Error::Fixed)
    }
    fn statics<'body: 'd, H: headers::Source>(
        self,
        _: Code,
        _: H,
        _: fixed::Body<'body>,
    ) -> Result<Self::Output, Self::Error> {
        Err(Error::Static)
    }
    fn bounded<H: headers::Source, B: bounded::Body>(
        self,
        _: Code,
        _: H,
        _: B,
    ) -> Result<Self::Output, Self::Error> {
        Err(Error::Bounded)
    }
    fn exact<H: headers::Source, B: exact::TryBody>(
        self,
        status: Code,
        headers: H,
        body: B,
    ) -> Result<Self::Output, Self::Error> {
        if let Some(fields) = self.1 {
            headers.emit(fields);
            fields.result.map_err(Error::Field)?;
        }
        let plan =
            exact::Plan::try_new(body).map_err(|rejected| Error::Size(rejected.discard()))?;
        let len = plan.len();
        plan.encode(self.0).map_err(Error::Build)?;
        Ok((status, len))
    }
}

pub struct RawBody<'req> {
    pub key: &'req [u8],
    pub nonce: &'req [u8],
}

impl exact::Body for RawBody<'_> {
    fn encoded_len(&self) -> usize {
        self.key.len() + 1 + self.nonce.len()
    }
}

impl response::Encode for RawBody<'_> {
    fn encode<W>(self, out: &mut W) -> Result<(), consume::Error>
    where
        W: write::Sink<Error = consume::Error>,
    {
        let key = self.key;
        let nonce = self.nonce;
        write::Sink::write_slices(out, [key, b":", nonce])
    }
}

#[sark::response(exact)]
#[header("content-type", "text/plain")]
#[header_static("Cache-Control", "no-store")]
pub struct RawReply<'req> {
    pub status: Code,
    pub body: RawBody<'req>,
    #[header("x-key")]
    pub key: Value<&'req [u8]>,
    #[header("x-owned")]
    pub owned: Value<storage::Shared>,
}

#[sark_json::json(try_encode)]
pub struct Body {
    pub ok: bool,
    pub value: u64,
    pub shared: Shared,
    pub local: storage::Shared,
}

#[sark::response(json)]
#[header("content-type", "application/json")]
pub struct Reply {
    pub status: Code,
    pub body: Body,
}

#[sark::response(json)]
pub struct Unsigned {
    pub status: Code,
    pub body: usize,
}

pub struct Counted<'a>(pub &'a Cell<usize>);

impl Size for Counted<'_> {
    fn json_size(&self) -> usize {
        self.0.set(self.0.get() + 1);
        2
    }
}

impl TryEncode for Counted<'_> {
    fn try_write<'a, W: write::Sink>(
        &self,
        writer: Writer<'a, W>,
    ) -> ControlFlow<Failure<'a, W>, Writer<'a, W>> {
        writer.put(b"{}")
    }
}

#[sark::response(json)]
#[header("content-type", "application/json")]
pub struct CountedReply<'a> {
    pub status: Code,
    pub body: Counted<'a>,
}

#[sark::response(raw)]
pub struct StaticReply<'field> {
    pub status: Code,
    pub body: fixed::Body<'field>,
}

#[sark::response(raw)]
pub struct View<'req, 'd> {
    pub status: Code,
    pub body: fixed::Body<'d>,
    #[header("x-value")]
    pub value: Value<&'req [u8]>,
}
