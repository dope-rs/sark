use sark_http::{
    h1::{codec::decode::response::Decoder, framing::ResponseBody},
    payload::response::{Method, status::Code},
};

#[test]
fn fixed() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let head = (Decoder::new(Method::Other).collect(raw))
        .expect("response must decode")
        .expect("test input must contain a complete head");
    assert!(head.status == Code::OK);
    assert!(matches!(head.payload, ResponseBody::Fixed(5)));
    assert!(!head.fields.is_empty());
    assert_eq!(&raw[head.header_len.get() as usize..], b"hello");
}

#[test]
fn chunked() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
    let head = (Decoder::new(Method::Other).collect(raw))
        .expect("response must decode")
        .expect("test input must contain a complete head");
    assert!(matches!(head.payload, ResponseBody::Chunked));
}

#[test]
fn bodyless_status() {
    for (raw, status) in [
        (
            b"HTTP/1.1 204 No Content\r\n\r\n".as_slice(),
            Code::NO_CONTENT,
        ),
        (b"HTTP/1.1 304 Not Modified\r\n\r\n", Code::NOT_MODIFIED),
        (b"HTTP/1.1 100 Continue\r\n\r\n", Code::CONTINUE),
    ] {
        let head = (Decoder::new(Method::Other).collect(raw))
            .expect("response must decode")
            .expect("test input must contain a complete head");
        assert!(head.status == status);
        assert!(matches!(head.payload, ResponseBody::None));
    }
}

#[test]
fn head_request() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let head = (Decoder::new(Method::Head).collect(raw))
        .expect("response must decode")
        .expect("test input must contain a complete head");
    assert!(matches!(head.payload, ResponseBody::None));
}

#[test]
fn until_eof() {
    let raw = b"HTTP/1.1 200 OK\r\n\r\n";
    let head = (Decoder::new(Method::Other).collect(raw))
        .expect("response must decode")
        .expect("test input must contain a complete head");
    assert!(matches!(head.payload, ResponseBody::UntilEof));
}

#[test]
fn partial() {
    let raw = b"HTTP/1.1 200 OK\r\n";
    assert!(matches!(Decoder::new(Method::Other).collect(raw), Ok(None)));
}

#[test]
fn empty() {
    assert!(matches!(Decoder::new(Method::Other).collect(b""), Ok(None)));
}
