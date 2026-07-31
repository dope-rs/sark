use sark_core::http::codec::{MethodKey, RequestLine};

#[test]
fn parses_only_request_line_structure() {
    let raw = b"GET /items?ignored=\x01 HTTP/1.1\r\nHost: example\r\n\r\n";
    let line = RequestLine::parse(raw)
        .expect("structurally valid request line")
        .expect("complete request line");

    assert_eq!(line.method, b"GET");
    assert_eq!(line.target, b"/items?ignored=\x01");
    assert_eq!(line.version, b"HTTP/1.1");
    assert_eq!(&raw[line.headers_start..], b"Host: example\r\n\r\n");
}

#[test]
fn distinguishes_incomplete_from_malformed() {
    assert!(matches!(RequestLine::parse(b"GET / HTTP/1.1\r"), Ok(None)));
    assert!(matches!(RequestLine::parse(b"GET / HTTP/1.1\rx"), Err(())));
    assert!(matches!(RequestLine::parse(b"GET / HTTP/2.0\r\n"), Err(())));
}

#[test]
fn every_request_line_prefix_remains_incomplete() {
    let raw = b"OPTIONS /a/target/longer/than/eight HTTP/1.0\r\n";
    for end in 0..raw.len() {
        assert!(
            matches!(RequestLine::parse(&raw[..end]), Ok(None)),
            "prefix {end} was classified as complete or malformed",
        );
    }
    let line = RequestLine::parse(raw).unwrap().unwrap();
    assert_eq!(line.method, b"OPTIONS");
    assert_eq!(line.target, b"/a/target/longer/than/eight");
    assert_eq!(line.version, b"HTTP/1.0");
    assert_eq!(line.headers_start, raw.len());
}

#[test]
fn complete_bad_shapes_are_rejected() {
    for raw in [
        b" / HTTP/1.1\r\n".as_slice(),
        b"GET  HTTP/1.1\r\n",
        b"GET /\r\n",
        b"GET / HTTP/1.1 extra\r\n",
        b"GET / HTTP/1.2\r\n",
        b"GET / HTTP/1.1\n\r\n",
        b"G\rET / HTTP/1.1\r\n",
        b"GET /\rhidden HTTP/1.1\r\n",
    ] {
        assert!(matches!(RequestLine::parse(raw), Err(())), "{raw:?}");
    }
}

#[test]
fn route_mask_returns_only_configured_method_keys() {
    let mut get = None;
    RequestLine::parse_for::<{ MethodKey::Get.bit() }>(b"GET / HTTP/1.1\r\n", &mut get)
        .unwrap()
        .unwrap();
    assert_eq!(get, Some(MethodKey::Get));

    let mut post = None;
    RequestLine::parse_for::<{ MethodKey::Get.bit() }>(b"POST / HTTP/1.1\r\n", &mut post)
        .unwrap()
        .unwrap();
    assert_eq!(post, None);

    let mut key = None;
    let extension = RequestLine::parse_for::<0>(b"SYNC / HTTP/1.1\r\n", &mut key)
        .unwrap()
        .unwrap();
    assert_eq!(extension.method, b"SYNC");
    assert_eq!(key, None);
}
