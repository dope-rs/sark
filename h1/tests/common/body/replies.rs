use std::{cell::Cell, num::NonZero};

use o3::{
    alloc,
    buffer::{
        resident::bound,
        write::{Sink, consume},
    },
};
use sark_h1::route::{Cache, Policy};
use sark_http::{
    h1::head::{
        request::Coding,
        response::{Date, Server},
    },
    payload::{
        compress::{self, Compression},
        fixed,
        response::{Encode, Method, bounded, dispatch, exact, status::Code},
    },
};

pub const BYTES: &[u8] = b"omitted response content";
pub const CASES: &[(Method, Code)] = &[
    (Method::Head, Code::OK),
    (Method::Other, Code::NO_CONTENT),
    (Method::Other, Code::NOT_MODIFIED),
    (Method::Other, Code::RESET_CONTENT),
];
pub const POLICY: Policy = Policy::new(Cache::Bypass, Coding::Gzip, Date::Omit, Server::Omit);
pub const GET_TEN: &[u8] = b"GET / HTTP/1.0\r\nHost: local\r\n\r\n";
const GET_ELEVEN: &[u8] = b"GET / HTTP/1.1\r\nHost: local\r\n\r\n";
pub const CONNECTIONS: &[(&[u8], Option<&str>)] = &[
    (GET_TEN, None),
    (
        b"GET / HTTP/1.0\r\nHost: local\r\nConnection: keep-alive\r\n\r\n",
        Some("Connection: keep-alive"),
    ),
    (
        b"GET / HTTP/1.0\r\nHost: local\r\nConnection: keep-alive, close\r\n\r\n",
        None,
    ),
    (GET_ELEVEN, None),
    (
        b"GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        Some("Connection: close"),
    ),
    (GET_ELEVEN, None),
];
pub const HEADS: &[&[u8]] = &[
    b"HEAD / HTTP/1.0\r\nHost: local\r\n\r\n",
    b"HEAD / HTTP/1.1\r\nHost: local\r\n\r\n",
];
pub const STREAM_TEN: &[u8] = b"POST / HTTP/1.0\r\nHost: local\r\nContent-Length: 1\r\n\r\n";

pub struct Untouched<'a>(pub &'a Cell<usize>);

impl Encode for Untouched<'_> {
    fn encode<W: Sink<Error = consume::Error>>(self, out: &mut W) -> Result<(), consume::Error> {
        self.0.set(self.0.get() + 1);
        out.write_slice(BYTES)
    }
}

impl exact::TryBody for Untouched<'_> {
    fn try_len(&self) -> Result<usize, exact::Overflow> {
        self.0.set(self.0.get() + 1);
        Err(exact::Overflow {
            current: usize::MAX,
            added: NonZero::<usize>::MIN,
        })
    }
}

impl bounded::Body for Untouched<'_> {
    fn max_len(&self) -> usize {
        self.0.set(self.0.get() + 1);
        usize::MAX
    }
}

#[derive(Clone, Copy)]
pub enum Shape {
    Fixed,
    Static,
    Exact,
    Bounded,
}

pub struct Reply<'a> {
    pub status: Code,
    pub shape: Shape,
    pub calls: &'a Cell<usize>,
}

impl<'req, 'd> dispatch::Complete<'req, 'd> for Reply<'_> {
    const BODY: sark_http::payload::Kind = sark_http::payload::Kind::Inline;
    type Bound = bound::Unbounded;

    fn complete<S: dispatch::Accept<'d>>(self, sink: S) -> Result<S::Output, S::Error> {
        match self.shape {
            Shape::Fixed => sink.fixed(self.status, (), BYTES),
            Shape::Static => sink.statics(self.status, (), fixed::Body::new(BYTES)),
            Shape::Exact => sink.exact(self.status, (), Untouched(self.calls)),
            Shape::Bounded => sink.bounded(self.status, (), Untouched(self.calls)),
        }
    }
}

pub struct Compressor {
    pub calls: usize,
}

impl Compression for Compressor {
    fn try_new() -> Result<Self, alloc::Error> {
        Ok(Self { calls: 0 })
    }

    fn encode(&mut self, _: &[u8]) -> compress::Output {
        self.calls += 1;
        compress::Output::Plain
    }
}

pub struct Capture<'a> {
    pub state: &'a Cell<usize>,
    pub drops: &'a Cell<usize>,
}

impl Drop for Capture<'_> {
    fn drop(&mut self) {
        assert_eq!(self.state.get(), 1, "capture drops before route invocation");
        self.drops.set(self.drops.get() + 1);
    }
}
