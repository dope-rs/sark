pub mod common;

use std::iter;

use common::head::{self, Request as _, method};
use dope_test::alloc::Tracker;
use sark_http::{
    field,
    h1::{
        head::line::{Request, Version, error},
        target,
    },
    method::{Error, Known, Method},
    uri::path,
};

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn parses_complete_line() {
    let raw = b"GET /items?ignored=yes HTTP/1.1\r\nHost: example\r\n\r\n";
    let line = (Request::parse(raw))
        .expect("complete request line must parse")
        .expect("complete request line must parse");

    assert_eq!(line.method(), b"GET");
    assert_eq!(line.target(), b"/items?ignored=yes");
    assert_eq!(line.path(), Some(b"/items".as_slice()));
    assert_eq!(line.query(), Some(b"ignored=yes".as_slice()));
    assert!((line.version()) == (Version::Eleven));
    assert_eq!(&raw[line.end()..], b"Host: example\r\n\r\n");
}

#[test]
fn classifies_failures() {
    assert!(matches!(Request::parse(b"GET / HTTP/1.1\r"), Ok(None)));
    assert!(Request::parse(b"GET / HTTP/1.1\rx").is_err());
    assert!(Request::parse(b"GET / HTTP/2.0\r\n").is_err());
}

#[test]
fn prefixes_stay_partial() {
    let raw = head::ESCAPED_REQUEST_LINE;
    for end in 0..raw.len() {
        assert!(
            matches!(Request::parse(&raw[..end]), Ok(None)),
            "prefix {end} was classified as complete or malformed",
        );
    }
    let line = (Request::parse(raw))
        .expect("complete request line must parse")
        .expect("complete request line must parse");
    assert_eq!(line.method(), b"OPTIONS");
    assert_eq!(line.path(), Some(head::ESCAPED_PATH));
    assert!((line.version()) == (Version::Ten));
    assert_eq!(line.end(), raw.len());
}

#[test]
fn checks_separator_lanes() {
    for len in 1..=32 {
        let mut raw = b"GET ".to_vec();
        let start = raw.len();
        raw.push(b'/');
        raw.extend(iter::repeat_n(b'a', len - 1));
        raw.extend_from_slice(b" HTTP/1.1\r\n");

        let line = (Request::parse(&raw))
            .expect("complete request line must parse")
            .expect("complete request line must parse");
        assert_eq!(line.target(), &raw[start..start + len]);

        raw[start + len - 1] = b'\r';
        assert!(Request::parse(&raw).is_err());
    }
}

#[test]
fn keeps_absolute_spans() {
    let raw = b"GET https://origin.example/items/42?q=rust HTTP/1.1\r\n";
    let line = (Request::parse(raw))
        .expect("complete request line must parse")
        .expect("complete request line must parse");
    assert_eq!(line.path(), Some(b"/items/42".as_slice()));
    assert_eq!(line.authority(), Some(b"origin.example".as_slice()));
}

#[test]
fn rejects_bad_shapes() {
    for raw in [
        b" / HTTP/1.1\r\n".as_slice(),
        b"GET  HTTP/1.1\r\n",
        b"GET /\r\n",
        b"GET / HTTP/1.1 extra\r\n",
        b"GET / HTTP/1.2\r\n",
        b"GET / HTTP/1.1\n\r\n",
        b"G\rET / HTTP/1.1\r\n",
        b"GET /\rhidden HTTP/1.1\r\n",
        b"GET /bad/%zz HTTP/1.1\r\n",
        b"GET * HTTP/1.1\r\n",
        head::CONNECT_ORIGIN,
        head::INVALID_ESCAPE_PREFIX,
    ] {
        assert!(Request::parse(raw).is_err(), "{raw:?}");
    }
}

#[test]
fn admits_rfc3986_bytes() {
    for byte in u8::MIN..=u8::MAX {
        let ordinary = byte.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=:@/".contains(&byte);
        for context in *b"g?" {
            let short = [b'/', context, byte];
            let mut wide = [b'g'; 128];
            wide[0] = b'/';
            wide[1] = context;
            wide[65] = byte;
            for target in [short.as_slice(), wide.as_slice()] {
                assert_eq!(
                    matches!(Request::parse(&target.request()), Ok(Some(_))),
                    ordinary || byte == b'?',
                    "byte {byte:#04x}, context {context:#04x}, length {}",
                    target.len(),
                );
            }
        }
    }
}

#[test]
fn scans_all_simd_lanes() {
    for pad in 0..=80 {
        let mut target = vec![b'/'];
        target.extend(iter::repeat_n(b'a', pad));
        target.extend_from_slice(b"%2f?q=%41??tail");
        let raw = target.request();
        let line = (Request::parse(&raw))
            .expect("complete request line must parse")
            .expect("complete request line must parse");
        assert_eq!(line.target(), target);

        let percent = 1 + pad;
        target[percent + 2] = b'g';
        assert!(Request::parse(&target.request()).is_err());
    }
}

#[test]
fn stops_before_headers() {
    let raw =
        b"GET /%2f?q=?? HTTP/1.1\r\nX-Ignored: %zz#????????????????????????????????\xff\r\n\r\n";
    let line = Request::parse(raw)
        .expect("valid request line")
        .expect("complete request line");
    assert_eq!(line.path(), Some(b"/%2f".as_slice()));
    assert_eq!(line.query(), Some(b"q=??".as_slice()));
    assert_eq!(line.end(), 24);
}

#[test]
fn keeps_first_path_error() {
    for at in [15, 16, 17, 31, 32, 33, 63, 64, 65, 79, 80, 81] {
        let mut target = [b'a'; 128];
        target[0] = b'/';
        target[at..at + 4].copy_from_slice(b"#%zz");
        assert_eq!(
            Request::parse(&target.request()).err(),
            Some(error::Request::Target(target::Error::Path(
                path::Error::Byte { at, byte: b'#' }
            ))),
        );

        target[at..at + 4].copy_from_slice(b"%zz#");
        assert_eq!(
            Request::parse(&target.request()).err(),
            Some(error::Request::Target(target::Error::Path(
                path::Error::Escape {
                    at,
                    first: Some(b'z'),
                    second: Some(b'z'),
                }
            ))),
        );
    }
}

#[test]
fn filters_method_keys() {
    let get = (Request::parse_with(b"GET / HTTP/1.1\r\n", Known::Get.bit()))
        .expect("complete request line must parse")
        .expect("complete request line must parse");
    assert!((get.matched()) == (Some(Known::Get)));

    let post = (Request::parse_with(b"POST / HTTP/1.1\r\n", Known::Get.bit()))
        .expect("complete request line must parse")
        .expect("complete request line must parse");
    assert!(post.matched().is_none());

    let ext = (Request::parse_with(b"SYNC / HTTP/1.1\r\n", 0))
        .expect("complete request line must parse")
        .expect("complete request line must parse");
    assert_eq!(ext.method(), b"SYNC");
    assert!(ext.matched().is_none());

    for (raw, known, form) in [
        (
            b"CONNECT example.test:443 HTTP/1.1\r\n".as_slice(),
            Known::Connect,
            target::Form::Authority,
        ),
        (
            b"OPTIONS * HTTP/1.1\r\n".as_slice(),
            Known::Options,
            target::Form::Asterisk,
        ),
    ] {
        for (methods, matched) in [(0, None), (known.bit(), Some(known))] {
            let line = Request::parse_with(raw, methods)
                .expect("admitted method")
                .expect("complete line");
            assert_eq!(line.method().as_ptr(), raw.as_ptr());
            assert_eq!(line.method(), known.as_str().as_bytes());
            assert_eq!(line.matched(), matched);
            assert!(line.plan().form() == form);
        }
    }
    assert_eq!(
        Request::parse_with(b"G\xffT /%zz HTTP/1.1\r\n", 0).err(),
        Some(error::Request::Syntax),
    );
}

#[test]
fn keeps_method_identity() {
    assert_eq!(Method::from(Known::Get), Method::GET);
    assert_eq!(
        Method::from_bytes(method::TRACE).expect("trace"),
        Method::TRACE
    );
    assert_eq!(
        Method::from_bytes(method::QUERY).expect("query"),
        Method::QUERY
    );

    let (short, usage) = Tracker::usage(|| Method::from_bytes(method::INLINE15.as_bytes()));
    let short = short.expect("inline extension");
    assert_eq!(usage, (0, 0));
    let (long, usage) = Tracker::usage(|| Method::from_bytes(method::OWNED16.as_bytes()));
    let long = long.expect("owned extension");
    assert_eq!(usage, (1, method::OWNED16.len()));
    assert_eq!(short.as_str(), method::INLINE15);
    assert_eq!(long.as_bytes(), method::OWNED16.as_bytes());
    assert_ne!(
        Method::from_bytes(method::LOWERCASE).expect("lowercase token"),
        Method::GET
    );
}

#[test]
fn retains_token_failure() {
    assert_eq!(
        Method::from_bytes(method::EMPTY),
        Err(Error::Syntax(field::Error::Empty))
    );
    assert_eq!(
        Method::from_bytes(method::SPACE),
        Err(Error::Syntax(field::Error::InvalidByte {
            index: 3,
            byte: b' '
        })),
    );
    assert_eq!(
        Method::from_bytes(method::NON_ASCII),
        Err(Error::Syntax(field::Error::InvalidByte {
            index: 7,
            byte: 255
        })),
    );
}
