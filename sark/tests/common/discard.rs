use o3::alloc::{self, VecExt};
use sark_http::payload::{fixed, response::status::Code};

pub const DATE: &[u8; 29] = b"Mon, 01 Jan 2026 00:00:00 GMT";

pub const HEAD: &[u8] = b"POST /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\n\r\n";
pub const TAIL: &[u8] = b"bodyPOST /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n\r\n";

pub const BODY_POLICY: &[(&[u8], u16)] = &[
    (
        b"GET /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\nExpect: 100-continue\r\n\r\n",
        400,
    ),
    (
        b"HEAD /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\n\r\nx",
        400,
    ),
    (
        b"HEAD /discard HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n0\r\n\r\n",
        400,
    ),
    (
        b"GET /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 1048577\r\nExpect: 100-continue\r\n\r\n",
        413,
    ),
    (
        b"HEAD /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n\r\n",
        200,
    ),
];

#[sark::request]
pub struct Request {}

impl Request<'_> {
    pub fn pipeline() -> Result<Vec<u8>, alloc::Error> {
        let head = b"POST /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 131072\r\n\r\n";
        let tail = &PIPELINE[..PIPELINE.len() / 2];
        let mut wire = Vec::try_new(head.len() + 128 * 1024 + tail.len())?;
        wire.extend_from_slice(head);
        wire.resize(head.len() + 128 * 1024, b'x');
        wire.extend_from_slice(tail);
        Ok(wire)
    }
}

#[sark::response(raw)]
pub struct Response<'d> {
    status: Code,
    body: fixed::Body<'d>,
}

#[sark::handler(pub)]
fn reply<'d>(_: Request) -> Response<'d> {
    Response {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
    }
}

sark::define_router! {
    pub Router: () => {
        POST "/discard" => reply,
        GET "/discard" => reply,
        HEAD "/discard" => reply,
    }
}

pub const PREFIX: &[u8] = b"POST /discard HTTP/1.1\r";
pub const PIPELINE: &[u8] = b"POST /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n\r\nPOST /discard HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n\r\n";
pub const SUCCESS: &[u8] = b"HTTP/1.1 200 OK\r\n";
pub const MISSING_HEAD: &[u8] = b"POST /missing HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n";
pub const MISMATCHED_LENGTH: &[u8] = b"Content-Length: 1\r\n\r\n";
