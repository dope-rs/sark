use std::{convert::Infallible, str};

use sark_http::h1::head::ParseError;

mod from;

/// Exact request parsing, framing, or peer I/O failure.
#[derive(Debug)]
pub enum Error<E = dope_test::harness::peer::Error> {
    Line {
        actual: String,
        expected: String,
    },
    Submitted(u8),
    Boundary {
        wire: usize,
        head: usize,
    },
    Utf8(str::Utf8Error),
    Protocol(ParseError<Infallible>),
    Encoding,
    Frame {
        start: usize,
        head: usize,
        body: usize,
        max: usize,
    },
    Body {
        have: usize,
        need: usize,
    },
    Head {
        bytes: usize,
        max: usize,
    },
    HeadEnd {
        bytes: usize,
    },
    Request {
        bytes: usize,
        incoming: usize,
        max: usize,
    },
    UnexpectedEof,
    Io(E),
}
