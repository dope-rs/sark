use sark_http::{method::Method, payload::response::status, uri::path::Error};

pub const BASE: &str = "http://example.test/start";
pub const START: &str = "/start";
pub const PAYLOAD: &[u8] = b"effect";
pub const INITIAL_TARGETS: [&str; 2] = [START, BASE];
pub const REWRITE_STATUSES: [status::Code; 3] = [
    status::Code::MOVED_PERMANENTLY,
    status::Code::FOUND,
    status::Code::SEE_OTHER,
];
pub const PRESERVED_METHODS: [(status::Code, Method); 5] = [
    (status::Code::FOUND, Method::GET),
    (status::Code::SEE_OTHER, Method::GET),
    (status::Code::SEE_OTHER, Method::HEAD),
    (status::Code::TEMPORARY_REDIRECT, Method::POST),
    (status::Code::PERMANENT_REDIRECT, Method::POST),
];
pub const ALIAS: &[u8] = b"http://EXAMPLE.test:00080/start";
pub const SAME_TARGETS: [&[u8]; 4] = [
    b"/start",
    b"http://EXAMPLE.test/start",
    b"http://example.test:80/start",
    ALIAS,
];
pub const METHOD_CHAIN: [(status::Code, &[u8]); 3] = [
    (status::Code::TEMPORARY_REDIRECT, b"/middle"),
    (status::Code::SEE_OTHER, b"/start"),
    (status::Code::TEMPORARY_REDIRECT, b"/middle"),
];

pub const INVALID_UTF8: &[u8] = b"/next/\xff";

pub const INVALID: [(&str, Error); 4] = [
    ("/bad space/../ok", Error::Byte { at: 4, byte: b' ' }),
    ("bad space/../ok", Error::Byte { at: 3, byte: b' ' }),
    (
        "/%zz/../ok",
        Error::Escape {
            at: 1,
            first: Some(b'z'),
            second: Some(b'z'),
        },
    ),
    (
        "bad%zz/../ok",
        Error::Escape {
            at: 3,
            first: Some(b'z'),
            second: Some(b'z'),
        },
    ),
];
