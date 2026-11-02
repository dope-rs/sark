use std::cell::Cell;

use o3::buffer::segments::Count;
use sark_http::payload::{fixed, response::status::Code};

pub const WIRE: &[u8] = b"POST /echo HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n4\r\ndefg\r\n0\r\n\r\n";

#[derive(Default)]
pub struct State {
    pub one: Cell<Option<usize>>,
    pub many: Cell<Option<Count>>,
    pub invalid: Cell<bool>,
}

#[sark_fiber::request]
pub struct Request {
    #[raw_body]
    payload: Segments,
}

#[sark_fiber::response(raw)]
pub struct Response<'d> {
    status: Code,
    body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
fn echo<'d>(request: Request, state: &State) -> Response<'d> {
    if !request.payload.segments().flatten().eq(b"abcdefg") {
        state.invalid.set(true);
    }
    Response {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
    }
}

sark_fiber::define_router! {
    pub Router: State => {
        POST "/echo" => echo,
    }
}

#[sark_fiber::handler(pub)]
#[max_body(1)]
fn one<'d>(request: Request, state: &State) -> Response<'d> {
    if request.payload.len() != 1 {
        state.one.set(Some(request.payload.len()));
    }
    Response {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
    }
}

#[sark_fiber::handler(pub)]
#[max_body(1024)]
fn many<'d>(request: Request, state: &State) -> Response<'d> {
    if request.payload.count() != Count::new(1024) {
        state.many.set(Some(request.payload.count()));
    }
    Response {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
    }
}

sark_fiber::define_router! {
    pub CapacityRouter: State => {
        POST "/one" => one,
        POST "/many" => many,
    }
}

impl Request<'_> {
    pub fn wire(path: &[u8], chunks: usize) -> Vec<u8> {
        let mut wire = b"POST ".to_vec();
        wire.extend_from_slice(path);
        wire.extend_from_slice(b" HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n");
        for _ in 0..chunks {
            wire.extend_from_slice(b"1\r\nx\r\n");
        }
        wire.extend_from_slice(b"0\r\n\r\n");
        wire
    }
}

pub const PARTS: [&[u8; 3]; 2] = [b"abc", b"def"];
pub const LATE: &[u8; 4] = b"late";
pub const JOINED: &[u8; 6] = b"abcdef";
pub const SINGLE: &[u8; 6] = b"single";

pub const ONE: &[u8] = b"/one";
pub const MANY: &[u8] = b"/many";
pub const SPLIT: usize = b"POST /echo HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chu".len();
