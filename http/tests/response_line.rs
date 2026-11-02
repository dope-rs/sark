use sark_http::h1::head::line::{Response, Version};

#[test]
fn borrows_reason() {
    let raw = b"HTTP/1.1 204 No Content\r\nDate: now\r\n\r\n";
    let line = (Response::parse(raw))
        .expect("complete response line must parse")
        .expect("complete response line must parse");

    assert!((line.version()) == (Version::Eleven));
    assert_eq!(line.status().as_u16(), 204);
    assert_eq!(line.reason(), b"No Content");
    assert_eq!(&raw[line.end()..], b"Date: now\r\n\r\n");
}

#[test]
fn keeps_prefixes_pending() {
    let raw = b"HTTP/1.0 599 extension\tstatus\r\n";
    for end in 0..raw.len() {
        assert!(
            matches!(Response::parse(&raw[..end]), Ok(None)),
            "prefix {end}"
        );
    }

    let line = (Response::parse(raw))
        .expect("complete response line must parse")
        .expect("complete response line must parse");
    assert!((line.version()) == (Version::Ten));
    assert_eq!(line.status().as_u16(), 599);
    assert_eq!(line.reason(), b"extension\tstatus");
}

#[test]
fn rejects_malformed() {
    for raw in [
        b"HTTP/2.0 200 OK\r\n".as_slice(),
        b"HTTP/1.1 099 Low\r\n",
        b"HTTP/1.1 600 High\r\n",
        b"HTTP/1.1 abc Nope\r\n",
        b"HTTP/1.1 200\r\n",
        b"HTTP/1.1 200 Bad\x7fReason\r\n",
        b"HTTP/1.1 200 OK\rx",
    ] {
        assert!(Response::parse(raw).is_err(), "{raw:?}");
    }
}
