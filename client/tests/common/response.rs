use o3::buffer::{
    resident,
    resident::staging,
    segments,
    storage::{self, Shared},
};
use sark_client::{
    protocol::{self, codec::output},
    reply,
};
use sark_http::payload::response::Method;

use crate::common::{codec::decoder, reply::try_sources};

type Body = &'static [u8];

pub const PLAIN: Body = b"payload";
pub const GZIP: Body = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\x2b\x48\xac\xcc\xc9\x4f\x4c\x01\x00\x15\x6a\x2c\x42\x07\x00\x00\x00";
pub const NESTED: Body = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\x93\xef\xe6\x60\x00\x01\xa6\xff\xda\x1e\x6b\xce\x9c\xf4\xf7\x61\x64\x10\xcd\xd2\x71\x62\x07\x0a\x01\x00\x1e\x2f\xef\xfa\x1b\x00\x00\x00";
pub const SINGLE: &str = "Content-Encoding: GZip\r\n";
pub const RESET_HEAD: &[u8] = b"HTTP/1.1 205 Reset Content\r\nTransfer-Encoding: chunked\r\n\r\n";
pub const RESET_CHUNKS: [(&[u8], &[u8]); 2] = [(b"0\r", b"\n\r\n"), (b"1\r", b"\nx\r\n0\r\n\r\n")];
pub const EMPTY_GZIP: [(Method, &[u8]); 3] = [
    (
        Method::Head,
        b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 27\r\n\r\n",
    ),
    (
        Method::Other,
        b"HTTP/1.1 304 Not Modified\r\nContent-Encoding: gzip\r\nContent-Length: 27\r\n\r\n",
    ),
    (
        Method::Other,
        b"HTTP/1.1 205 Reset Content\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n",
    ),
];
pub const MULTIPLE: [(&str, &[u8]); 2] = [
    (
        "Content-Encoding: gzip\r\nContent-Encoding: gzip\r\n",
        b"gzip",
    ),
    ("Content-Encoding: gzip, gzip\r\n", b"gzip, gzip"),
];
pub const TRANSFERS: [(&str, Body, Option<&[u8]>); 3] = [
    ("Transfer-Encoding: chunked\r\n", GZIP, None),
    (
        "Transfer-Encoding: gzip, chunked\r\n",
        NESTED,
        Some(b"gzip"),
    ),
    (
        "Transfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n",
        NESTED,
        Some(b"gzip"),
    ),
];

pub fn bad_crc() -> Vec<u8> {
    let mut encoded = GZIP.to_vec();
    let trailer = encoded.len() - 8;
    encoded[trailer] ^= 1;
    encoded
}

pub fn transfer_wire(fields: &str) -> Vec<u8> {
    let mut wire = format!(
        "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n{fields}\r\n{:x}\r\n",
        NESTED.len(),
    )
    .into_bytes();
    wire.extend_from_slice(NESTED);
    wire.extend_from_slice(b"\r\n0\r\n\r\n");
    wire
}

pub const SUFFIX: Body = &{
    let mut body = [0; 2 * output::DATA_LIMIT + 1];
    let end = body.len();
    body[end - 4] = b't';
    body[end - 3] = b'a';
    body[end - 2] = b'i';
    body[end - 1] = b'l';
    body
};

#[derive(Debug)]
pub enum Error {
    Storage(storage::AllocError),
    Staging(staging::Error),
    Protocol(protocol::Error),
    Incomplete,
    Assembly(sark_client::Error),
    Decode(decoder::Error),
}

pub trait Decode<'d> {
    fn try_body(
        &self,
        budget: resident::Budget<'d>,
        fields: &str,
        body: &[u8],
    ) -> Result<reply::Scoped<'_, 'd>, Error> {
        let mut wire = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n{fields}\r\n",
            body.len(),
        )
        .into_bytes();
        wire.extend_from_slice(body);
        self.try_wire(budget, Method::Other, &wire)
    }

    fn try_wire(
        &self,
        budget: resident::Budget<'d>,
        method: Method,
        wire: &[u8],
    ) -> Result<reply::Scoped<'_, 'd>, Error>;
}

impl<'d> Decode<'d> for segments::Pool<resident::Shared<'d>> {
    fn try_wire(
        &self,
        budget: resident::Budget<'d>,
        method: Method,
        wire: &[u8],
    ) -> Result<reply::Scoped<'_, 'd>, Error> {
        let wire = Shared::try_copy(wire).map_err(Error::Storage)?;
        try_sources(self, budget, method, &[wire])
    }
}
