use sark_http::{
    h1::codec::decode::response::{Decoder, Progress},
    payload::response::{Method, status::Code},
};

#[test]
fn invalid_or_partial() {
    for raw in [b"\r\n\r\n".as_slice(), b"    \r\n\r\n"] {
        assert!(matches!(
            Decoder::new(Method::Other).decode(raw),
            Err(_) | Ok(Progress::More)
        ));
    }
}

#[test]
fn invalid_chunks() {
    for raw in [
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n-1\r\nX\r\n0\r\n\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\r\nhello\r\n0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n 5 \r\nhello\r\n0\r\n\r\n",
    ] {
        assert!(Decoder::new(Method::Other).decode(raw).is_err());
    }
}

#[test]
fn invalid_status() {
    for raw in [
        b"HTTP/1.1 000 Nothing\r\nContent-Length: 0\r\n\r\n".as_slice(),
        b"HTTP/1.1 099 Low\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 600 Custom\r\nContent-Length: 0\r\n\r\n",
    ] {
        assert!(Decoder::new(Method::Other).decode(raw).is_err());
    }
}

#[test]
fn invalid_heads() {
    for raw in [
        b"HTTP/1.1 200 OK\r\nBad Name: value\r\nContent-Length: 0\r\n\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\nContent-Length: +5\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nTransfer-Encoding: chunked\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 7\r\n\r\nhello!!",
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked, gzip\r\n\r\n",
    ] {
        assert!(Decoder::new(Method::Other).decode(raw).is_err());
    }
}

#[test]
fn binary_body() {
    let body: Vec<u8> = (0u8..=255).collect();
    let raw = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
    let mut buf = raw.into_bytes();
    buf.extend_from_slice(&body);

    let reply = match (Decoder::new(Method::Other).decode(&buf)).expect("response must decode") {
        Progress::Complete(value) => Some(value),
        Progress::More => None,
    }
    .expect("test input must be complete");
    assert_eq!(reply.body().as_slice(), &body[..]);
}

#[test]
fn maximum_length() {
    let max = usize::MAX.to_string();
    let raw = format!("HTTP/1.1 200 OK\r\nContent-Length: {max}\r\n\r\n");
    assert!(matches!(
        Decoder::new(Method::Other).decode(raw.as_bytes()),
        Ok(Progress::More)
    ));
}

#[test]
fn transfer_fields() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
    let reply = match (Decoder::new(Method::Other).decode(raw)).expect("response must decode") {
        Progress::Complete(value) => Some(value),
        Progress::More => None,
    }
    .expect("test input must be complete");
    assert_eq!(reply.body().as_slice(), b"hello");
}

#[test]
fn tab_value() {
    let raw = b"HTTP/1.1 200 OK\r\nX-Tab: val\tue\r\nContent-Length: 0\r\n\r\n";
    let reply = match (Decoder::new(Method::Other).decode(raw)).expect("response must decode") {
        Progress::Complete(value) => Some(value),
        Progress::More => None,
    }
    .expect("test input must be complete");
    let value = reply.headers().get("x-tab").expect("x-tab field");
    assert_eq!(value.as_bytes(), b"val\tue");
}

#[test]
fn eof_empty() {
    let result = Decoder::new(Method::Other).finish(b"");
    assert!(result.is_err());
}

#[test]
fn eof_head() {
    let reply = (Decoder::new(Method::Other).finish(b"HTTP/1.1 200 OK\r\n\r\n"))
        .expect("response must decode");
    assert!(reply.body().is_empty());
}

#[test]
fn colon_value() {
    let raw = b"HTTP/1.1 200 OK\r\nX-Data: a:b:c:d:e\r\nContent-Length: 0\r\n\r\n";
    let reply = match (Decoder::new(Method::Other).decode(raw)).expect("response must decode") {
        Progress::Complete(value) => Some(value),
        Progress::More => None,
    }
    .expect("test input must be complete");
    let value = reply.headers().get("x-data").expect("x-data field");
    assert_eq!(value.as_bytes(), b"a:b:c:d:e");
}

#[test]
fn empty_reason() {
    let raw = b"HTTP/1.1 200 \r\nContent-Length: 0\r\n\r\n";
    let reply = match (Decoder::new(Method::Other).decode(raw)).expect("response must decode") {
        Progress::Complete(value) => Some(value),
        Progress::More => None,
    }
    .expect("test input must be complete");
    assert!(reply.status() == Code::OK);
}

#[test]
fn cookie_value() {
    let raw =
        b"HTTP/1.1 200 OK\r\nSet-Cookie: id=abc; Path=/; HttpOnly\r\nContent-Length: 0\r\n\r\n";
    let reply = match (Decoder::new(Method::Other).decode(raw)).expect("response must decode") {
        Progress::Complete(value) => Some(value),
        Progress::More => None,
    }
    .expect("test input must be complete");
    let value = reply.headers().get("set-cookie").expect("set-cookie field");
    assert_eq!(value.as_bytes(), b"id=abc; Path=/; HttpOnly");
}

#[test]
fn incomplete_heads() {
    for raw in [b"HTTP/1.1".as_slice(), b"HTTP/1.1 200"] {
        assert!(matches!(
            Decoder::new(Method::Other).decode(raw),
            Ok(Progress::More)
        ));
    }
}
