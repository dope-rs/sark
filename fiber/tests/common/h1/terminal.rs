use std::cell::Cell;

use dope_test::fibers::Yield;
use o3::buffer::write::{Sink, consume};
use sark_http::payload::{
    fixed,
    response::{Encode, exact, status::Code},
};

pub const CASES: [(&[u8], &[u8], usize); 2] = [
    (
        b"GET /failed HTTP/1.1\r\nHost: terminal\r\n\r\nGET /next HTTP/1.1\r\nHost: terminal\r\nConnection: close\r\n\r\n",
        b"HTTP/1.1 500 ",
        1,
    ),
    (
        b"GET /success HTTP/1.1\r\nHost: terminal\r\n\r\nGET /next HTTP/1.1\r\nHost: terminal\r\nConnection: close\r\n\r\n",
        b"HTTP/1.1 200 ",
        2,
    ),
];

pub struct __W<'a>(pub &'a Cell<usize>);

pub struct Incomplete;

impl exact::Body for Incomplete {
    fn encoded_len(&self) -> usize {
        2
    }
}

impl Encode for Incomplete {
    fn encode<W: Sink<Error = consume::Error>>(self, out: &mut W) -> Result<(), consume::Error> {
        out.write_slice(b"x")
    }
}

#[sark_fiber::handler(pub)]
async fn failed() -> exact::Direct<(), Incomplete> {
    Yield::new().await;
    exact::Direct::new(Code::OK, (), Incomplete)
}

#[sark_fiber::response(raw)]
pub struct Reply<'d> {
    status: Code,
    body: fixed::Body<'d>,
}

#[sark_fiber::handler(pub)]
async fn success() -> Reply<'d> {
    Yield::new().await;
    Reply {
        status: Code::OK,
        body: fixed::Body::new(b"ok"),
    }
}

#[sark_fiber::handler(pub)]
fn next(state: &__W<'_>) -> Reply<'d> {
    state.0.set(state.0.get() + 1);
    Reply {
        status: Code::OK,
        body: fixed::Body::new(b"next"),
    }
}

sark_fiber::define_route! {
    pub App<'e>: __W<'e> => {
        GET "/failed" => async(capacity = 1, body = discarded) failed,
        GET "/success" => async(capacity = 1, body = discarded) success,
        GET "/next" => next,
    }
}
