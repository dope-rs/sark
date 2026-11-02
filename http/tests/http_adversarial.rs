use sark_http::{
    h1::codec::decode::response::{Decoder, Progress},
    payload::response::{Method, status::Code},
};

#[test]
fn partial_inputs() {
    for raw in [
        b"".as_slice(),
        b"H",
        b"hello world",
        b"HTTP/1.1 200 OK\r\nContent-Length: 5",
    ] {
        assert!(
            matches!(Decoder::new(Method::Other).decode(raw), Ok(Progress::More)),
            "completed {raw:?}"
        );
    }
}

#[test]
fn body_presence() {
    let fixed = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    {
        let reply =
            match (Decoder::new(Method::Other).decode(fixed)).expect("response must decode") {
                Progress::Complete(value) => Some(value),
                Progress::More => None,
            }
            .expect("test input must be complete");
        assert!(reply.body().is_empty());
    };

    let eof = b"HTTP/1.1 200 OK\r\n\r\n";
    assert!(matches!(
        Decoder::new(Method::Other).decode(eof),
        Ok(Progress::More)
    ));
}

#[test]
fn incomplete_bodies() {
    for raw in [
        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nhello".as_slice(),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n",
    ] {
        assert!(
            matches!(Decoder::new(Method::Other).decode(raw), Ok(Progress::More)),
            "completed {raw:?}"
        );
    }
}

#[test]
fn invalid_lengths() {
    for raw in [
        b"HTTP/1.1 200 OK\r\nContent-Length: abc\r\n\r\nhello".as_slice(),
        b"HTTP/1.1 200 OK\r\nContent-Length: -5\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nContent-Length: 99999999999999999999999999999\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 10\r\n\r\nhello",
        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
    ] {
        assert!(Decoder::new(Method::Other).decode(raw).is_err());
    }
}

#[test]
fn invalid_chunks() {
    for raw in [
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nGGG\r\nhello\r\n0\r\n\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nFFFFFFFFFFFFFFFF\r\nhello\r\n0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n  5  \r\nhello\r\n0\r\n\r\n",
    ] {
        assert!(Decoder::new(Method::Other).decode(raw).is_err());
    }
}

#[test]
fn invalid_status() {
    for raw in [
        b"HTTP/1.1  OK\r\n\r\n".as_slice(),
        b"HTTP/1.1 999 Custom\r\n\r\n",
        b"HTTP/1.1 1000 Custom\r\n\r\n",
        b"HTTP/1.1 200\0OK\r\n\r\n",
    ] {
        assert!(Decoder::new(Method::Other).decode(raw).is_err());
    }
}

#[test]
fn invalid_fields() {
    for raw in [
        b"HTTP/1.1 200 OK\r\n: value\r\n\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\nX-Binary: \0\x01\x02\xff\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nX-Nu\0ll: value\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nX-Test: val\0ue\r\nContent-Length: 0\r\n\r\n",
        b"HTTP/1.1 200 OK\r\nX-Long: first\r\n second\r\nContent-Length: 0\r\n\r\n",
    ] {
        assert!(Decoder::new(Method::Other).decode(raw).is_err());
    }
}

#[test]
fn empty_value() {
    let raw = b"HTTP/1.1 200 OK\r\nX-Empty:\r\nContent-Length: 0\r\n\r\n";
    let reply = match (Decoder::new(Method::Other).decode(raw)).expect("response must decode") {
        Progress::Complete(value) => Some(value),
        Progress::More => None,
    }
    .expect("test input must be complete");
    let value = reply.headers().get("x-empty").expect("x-empty field");
    assert_eq!(value.as_bytes(), b"");
}

#[test]
fn long_value() {
    let value = "x".repeat(10240);
    let raw = format!("HTTP/1.1 200 OK\r\nX-Long: {value}\r\nContent-Length: 0\r\n\r\n");
    let reply =
        match (Decoder::new(Method::Other).decode(raw.as_bytes())).expect("response must decode") {
            Progress::Complete(value) => Some(value),
            Progress::More => None,
        }
        .expect("test input must be complete");
    let value = reply.headers().get("x-long").expect("x-long field");
    assert_eq!(value.as_bytes().len(), 10240);
}

#[test]
fn header_limit() {
    let mut raw = String::from("HTTP/1.1 200 OK\r\n");
    for index in 0..99 {
        raw.push_str(&format!("X-Header-{index}: value\r\n"));
    }
    raw.push_str("Content-Length: 0\r\n\r\n");
    {
        let reply = match (Decoder::new(Method::Other).decode(raw.as_bytes()))
            .expect("response must decode")
        {
            Progress::Complete(value) => Some(value),
            Progress::More => None,
        }
        .expect("test input must be complete");
        assert_eq!(reply.headers().len(), 100);
    };

    let mut raw = String::from("HTTP/1.1 200 OK\r\n");
    for index in 0..101 {
        raw.push_str(&format!("X-Header-{index}: value\r\n"));
    }
    raw.push_str("\r\n");
    assert!(Decoder::new(Method::Other).decode(raw.as_bytes()).is_err());
}

#[test]
fn opaque_reason() {
    let raw = "HTTP/1.1 200 OK™\r\nContent-Length: 0\r\n\r\n".as_bytes();
    let reply = match (Decoder::new(Method::Other).decode(raw)).expect("response must decode") {
        Progress::Complete(value) => Some(value),
        Progress::More => None,
    }
    .expect("test input must be complete");
    assert!(reply.status() == Code::OK);
}

#[test]
fn pipelined_tail() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhelloHTTP/1.1 200 OK\r\n";
    let reply = match (Decoder::new(Method::Other).decode(raw)).expect("response must decode") {
        Progress::Complete(value) => Some(value),
        Progress::More => None,
    }
    .expect("test input must be complete");
    assert_eq!(reply.body().as_slice(), b"hello");
}

#[test]
fn valid_lengths() {
    for name in ["Content-Length", "CONTENT-LENGTH"] {
        for value in ["0005", " 5 "] {
            let raw = format!("HTTP/1.1 200 OK\r\n{name}: {value}\r\n\r\nhello");
            let reply = match (Decoder::new(Method::Other).decode(raw.as_bytes()))
                .expect("response must decode")
            {
                Progress::Complete(value) => Some(value),
                Progress::More => None,
            }
            .expect("test input must be complete");
            assert_eq!(reply.body().as_slice(), b"hello");
        }
    }
}

#[test]
fn incomplete_eof() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhel";
    let reply = Decoder::new(Method::Other).finish(raw);
    assert!(reply.is_err());
}

#[test]
fn chunk_extensions() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5;ext1=val1;ext2=val2\r\nhello\r\n0\r\n\r\n";
    let reply = match (Decoder::new(Method::Other).decode(raw)).expect("response must decode") {
        Progress::Complete(value) => Some(value),
        Progress::More => None,
    }
    .expect("test input must be complete");
    assert_eq!(reply.body().as_slice(), b"hello");
}

#[test]
fn chunk_case() {
    let raw =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: CHUNKED\r\n\r\nf\r\n012345678901234\r\n0\r\n\r\n";
    let reply = match (Decoder::new(Method::Other).decode(raw)).expect("response must decode") {
        Progress::Complete(value) => Some(value),
        Progress::More => None,
    }
    .expect("test input must be complete");
    assert_eq!(reply.body().as_slice(), b"012345678901234");
}
