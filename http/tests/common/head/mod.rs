pub mod budget;
pub mod method;
pub mod trace;

pub const ESCAPED_REQUEST_LINE: &[u8] =
    b"OPTIONS /a/target/longer/than/eight%2f?q=%41 HTTP/1.0\r\n";
pub const ESCAPED_PATH: &[u8] = b"/a/target/longer/than/eight%2f";
pub const CONNECT_ORIGIN: &[u8] = b"CONNECT / HTTP/1.1\r\n";
pub const INVALID_ESCAPE_PREFIX: &[u8] = b"GET /%z";
pub const RESPONSE_PREFIX: &[u8] = b"HTTP/1.1 200 OK\r\nX-Proof: a";
pub const RESPONSE_SUFFIX: &[u8] = b"b\r\n\r\n";
pub const CONNECT_RESPONSES: &[&[u8]] = &[
    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\nraw",
    b"HTTP/1.1 204 No Content\r\nContent-Length: x\r\nContent-Length: 7\r\nTransfer-Encoding: chunked, gzip\r\n\r\nraw",
    b"HTTP/1.0 299 Tunnel\r\nTransfer-Encoding: chunked\r\n\r\nraw",
];

use std::convert::Infallible;

use sark_http::h1::codec::decode::fields;

#[derive(Default)]
pub struct Capture<'input> {
    pub fields: Vec<(&'input [u8], &'input [u8])>,
}

impl<'input> fields::Sink<'input> for Capture<'input> {
    type Error = Infallible;

    fn push(&mut self, name: &'input [u8], value: &'input [u8]) -> Result<(), Self::Error> {
        self.fields.push((name, value));
        Ok(())
    }
}

pub trait Request {
    fn request(&self) -> Vec<u8>;
}

impl Request for [u8] {
    fn request(&self) -> Vec<u8> {
        let mut raw = b"GET ".to_vec();
        raw.extend_from_slice(self);
        raw.extend_from_slice(b" HTTP/1.1\r\n");
        raw
    }
}
