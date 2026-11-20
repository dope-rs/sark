use sark_http::{
    field::Value,
    h1::codec::decode::{
        chunks,
        response::{Decoder, Error},
    },
    payload::response::{Method, status::Code},
};

const DECODER: Decoder = Decoder::new(Method::Other);

#[test]
fn partial_inputs() {
    for wire in [
        b"".as_slice(),
        b"H",
        b"hello world",
        b"HTTP/1.1 200 OK\r\nContent-Length: 5",
        b"HTTP/1.1",
        b"HTTP/1.1 200",
        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n",
        b"HTTP/1.1 200 OK\r\n\r\n",
    ] {
        assert!(matches!(DECODER.decode(wire), Ok(None)), "{wire:?}");
    }
    let wire = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", usize::MAX);
    assert!(matches!(DECODER.decode(wire.as_bytes()), Ok(None)));
}

#[test]
fn rejects_invalid_heads() {
    for wire in [
        b"\r\n\r\n".as_slice(),
        b"    \r\n\r\n",
        b"HTTP/1.1  OK\r\n\r\n",
        b"HTTP/1.1 999 Custom\r\n\r\n",
        b"HTTP/1.1 1000 Custom\r\n\r\n",
        b"HTTP/1.1 200\0OK\r\n\r\n",
        b"HTTP/1.1 000 Nothing\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 099 Low\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 600 Custom\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\n: value\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nX-Binary: \0\x01\x02\xff\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nX-Nu\0ll: value\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nX-Test: val\0ue\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nX-Long: first\r\n second\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: abc\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nContent-Length: -5\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nContent-Length: 99999999999999999999999999999\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 10\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nBad Name: value\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: +5\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nTransfer-Encoding: chunked\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 7\r\n\r\nhello!!",
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked, gzip\r\n\r\n",
    ] {
        assert!(matches!(DECODER.decode(wire), Err(Error::Head(_) | Error::Framing(_))), "{wire:?}");
    }
}

#[test]
fn rejects_invalid_chunks() {
    for wire in [
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nGGG\r\nhello\r\n0\r\n\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nFFFFFFFFFFFFFFFF\r\nhello\r\n0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n  5  \r\nhello\r\n0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n-1\r\nX\r\n0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\r\nhello\r\n0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n 5 \r\nhello\r\n0\r\n\r\n",
    ] {
        assert!(matches!(DECODER.decode(wire), Err(Error::Chunk(chunks::Error::Framing(_)))), "{wire:?}");
    }
}

#[test]
fn framed_bodies() {
    let responses: &[(&[u8], &[u8])] = &[
        (b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", b""),
        ("HTTP/1.1 200 OK™\r\nContent-Length: 0\r\n\r\n".as_bytes(), b""),
        (b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhelloHTTP/1.1 200 OK\r\n", b"hello"),
        (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5;ext1=val1;ext2=val2\r\nhello\r\n0\r\n\r\n", b"hello"),
        (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: CHUNKED\r\n\r\nf\r\n012345678901234\r\n0\r\n\r\n", b"012345678901234"),
        (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n", b"hello"),
    ];
    for &(wire, body) in responses {
        let reply = DECODER
            .decode(wire)
            .expect("valid response")
            .expect("complete");
        assert_eq!(reply.status(), Code::OK);
        assert_eq!(reply.body().as_slice(), body);
    }
}

#[test]
fn field_values() {
    let wire = b"HTTP/1.1 200 \r\nX-Empty:\r\nX-Tab: val\tue\r\nX-Data: a:b:c:d:e\r\nSet-Cookie: id=abc; Path=/; HttpOnly\r\nContent-Length: 0\r\n\r\n";
    let reply = DECODER
        .decode(wire)
        .expect("valid response")
        .expect("complete");
    assert_eq!(reply.status(), Code::OK);
    for (name, expected) in [
        ("x-empty", b"".as_slice()),
        ("x-tab", b"val\tue"),
        ("x-data", b"a:b:c:d:e"),
        ("set-cookie", b"id=abc; Path=/; HttpOnly"),
    ] {
        assert_eq!(
            reply.headers().get(name).map(Value::as_bytes),
            Some(expected),
            "{name}"
        );
    }

    let value = "x".repeat(10240);
    let wire = format!("HTTP/1.1 200 OK\r\nX-Long: {value}\r\nContent-Length: 0\r\n\r\n");
    let reply = DECODER
        .decode(wire.as_bytes())
        .expect("long field")
        .expect("complete");
    assert_eq!(
        reply.headers().get("x-long").map(Value::as_bytes),
        Some(value.as_bytes())
    );
}

#[test]
fn header_limit() {
    let mut wire = String::from("HTTP/1.1 200 OK\r\n");
    for index in 0..99 {
        wire.push_str(&format!("X-Header-{index}: value\r\n"));
    }
    let start = wire.len();
    wire.push_str("Content-Length: 0\r\n\r\n");
    let reply = DECODER
        .decode(wire.as_bytes())
        .expect("100 fields")
        .expect("complete");
    assert_eq!(reply.headers().len(), 100);

    wire.truncate(start);
    wire.push_str("X-Header-99: value\r\nX-Header-100: value\r\n\r\n");
    assert!(matches!(
        DECODER.decode(wire.as_bytes()),
        Err(Error::Head(_))
    ));
}

#[test]
fn fixed_bodies() {
    for name in ["Content-Length", "CONTENT-LENGTH"] {
        for value in ["0005", " 5 "] {
            let wire = format!("HTTP/1.1 200 OK\r\n{name}: {value}\r\n\r\nhello");
            let reply = DECODER
                .decode(wire.as_bytes())
                .expect("length")
                .expect("complete");
            assert_eq!(reply.body().as_slice(), b"hello");
        }
    }
    let body: Vec<u8> = (0..=255).collect();
    let mut wire =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    wire.extend_from_slice(&body);
    let reply = DECODER
        .decode(&wire)
        .expect("binary body")
        .expect("complete");
    assert_eq!(reply.body().as_slice(), body);
}

#[test]
fn eof_classifies_unfinished_responses() {
    assert!(matches!(DECODER.finish(b""), Err(Error::IncompleteHead)));
    let truncated = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhel";
    assert!(matches!(
        DECODER.finish(truncated),
        Err(Error::Chunk(chunks::Error::Incomplete))
    ));
    assert!(
        DECODER
            .finish(b"HTTP/1.1 200 OK\r\n\r\n")
            .expect("close-delimited")
            .body()
            .is_empty()
    );
}
