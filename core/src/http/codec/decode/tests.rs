use http::StatusCode;

use super::chunked::{BodyDecoder, DecodeEvent};
use crate::http::codec::Parse;

#[test]
fn simple_200() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn partial_headers_returns_none() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Le";
    assert!(
        Parse::response(raw, crate::http::codec::DecodeMode::Response)
            .unwrap()
            .is_none()
    );
}

#[test]
fn partial_body_returns_none() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nhello";
    assert!(
        Parse::response(raw, crate::http::codec::DecodeMode::Response)
            .unwrap()
            .is_none()
    );
}

#[test]
fn no_content_length_waits_for_eof() {
    let raw = b"HTTP/1.1 200 OK\r\n\r\nsome body data";
    assert!(
        Parse::response(raw, crate::http::codec::DecodeMode::Response)
            .unwrap()
            .is_none()
    );
}

#[test]
fn no_content_length_eof_returns_body() {
    let raw = b"HTTP/1.1 200 OK\r\n\r\nsome body data";
    let resp = Parse::response_after_eof(raw).unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.body_str(), Some("some body data"));
}

#[test]
fn status_404() {
    let raw = b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nnot found";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(resp.body_str(), Some("not found"));
}

#[test]
fn multiple_headers() {
    let raw =
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Custom: foo\r\nContent-Length: 2\r\n\r\nok";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.headers().get("content-type").unwrap(), "text/plain");
    assert_eq!(resp.headers().get("x-custom").unwrap(), "foo");
    assert_eq!(resp.body_str(), Some("ok"));
}

#[test]
fn empty_body_204() {
    let raw = b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(resp.body().is_empty());
}

#[test]
fn no_body_204_ignores_content_length() {
    let raw = b"HTTP/1.1 204 No Content\r\nContent-Length: 5\r\n\r\nhello";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(resp.body().is_empty());
}

#[test]
fn no_body_304() {
    let raw = b"HTTP/1.1 304 Not Modified\r\nContent-Length: 100\r\n\r\n";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
    assert!(resp.body().is_empty());
}

#[test]
fn head_request_ignores_body() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Head)
        .unwrap()
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.body().is_empty());
}

#[test]
fn informational_1xx_no_body() {
    let raw = b"HTTP/1.1 100 Continue\r\n\r\n";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONTINUE);
    assert!(resp.body().is_empty());
}

#[test]
fn chunked_single_chunk() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn chunked_multiple_chunks() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.body_str(), Some("hello world"));
}

#[test]
fn chunked_partial_size_line() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhel";
    assert!(
        Parse::response(raw, crate::http::codec::DecodeMode::Response)
            .unwrap()
            .is_none()
    );
}

#[test]
fn chunked_partial_terminal() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n";
    assert!(
        Parse::response(raw, crate::http::codec::DecodeMode::Response)
            .unwrap()
            .is_none()
    );
}

#[test]
fn chunked_empty_body() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert!(resp.body().is_empty());
}

#[test]
fn chunked_with_extension() {
    let raw =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5;ext=val\r\nhello\r\n0\r\n\r\n";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn chunked_hex_upper() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nA\r\n0123456789\r\n0\r\n\r\n";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.body_str(), Some("0123456789"));
}

#[test]
fn chunked_eof_decode() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n";
    let resp = Parse::response_after_eof(raw).unwrap();
    assert_eq!(resp.body_str(), Some("abc"));
}

#[test]
fn chunked_eof_incomplete_is_err() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nab";
    assert!(Parse::response_after_eof(raw).is_err());
}

#[test]
fn decode_eof_full() {
    let raw = b"HTTP/1.1 200 OK\r\n\r\nhello world";
    let resp = Parse::response_after_eof(raw).unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.body_str(), Some("hello world"));
}

#[test]
fn decode_eof_partial_is_err() {
    let raw = b"HTTP/1.1 200 ";
    assert!(Parse::response_after_eof(raw).is_err());
}

#[test]
fn exact_content_length_boundary() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.body_str(), Some("abc"));
}

#[test]
fn excess_data_after_content_length_ignored() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabcXXXX";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.body_str(), Some("abc"));
}

#[test]
fn chunked_with_trailer_headers() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nX-Checksum: abc123\r\n\r\n";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.body_str(), Some("hello"));
    assert_eq!(resp.headers().get("x-checksum").unwrap(), "abc123");
}

#[test]
fn chunked_with_multiple_trailer_headers() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\nX-A: 1\r\nX-B: 2\r\n\r\n";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.body_str(), Some("abc"));
    assert_eq!(resp.headers().get("x-a").unwrap(), "1");
    assert_eq!(resp.headers().get("x-b").unwrap(), "2");
}

#[test]
fn chunked_eof_with_trailer_headers() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ndata\r\n0\r\nX-Sig: xyz\r\n\r\n";
    let resp = Parse::response_after_eof(raw).unwrap();
    assert_eq!(resp.body_str(), Some("data"));
    assert_eq!(resp.headers().get("x-sig").unwrap(), "xyz");
}

#[test]
fn missing_status_code_is_error() {
    let raw = b"HTTP/1.1 \r\nContent-Length: 0\r\n\r\n";
    assert!(Parse::response(raw, crate::http::codec::DecodeMode::Response).is_err());
}

#[test]
fn invalid_content_length_is_error() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: abc\r\n\r\n";
    assert!(Parse::response(raw, crate::http::codec::DecodeMode::Response).is_err());
}

#[test]
fn conflicting_content_length_values_are_error() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 7\r\n\r\nhello!!";
    assert!(Parse::response(raw, crate::http::codec::DecodeMode::Response).is_err());
}

#[test]
fn duplicate_content_length_same_value_is_allowed() {
    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn transfer_encoding_list_chunked_last_is_allowed() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n";
    let resp = Parse::response(raw, crate::http::codec::DecodeMode::Response)
        .unwrap()
        .unwrap();
    assert_eq!(resp.body_str(), Some("hello"));
}

#[test]
fn transfer_encoding_list_chunked_not_last_is_error() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked, gzip\r\n\r\n";
    assert!(Parse::response(raw, crate::http::codec::DecodeMode::Response).is_err());
}

#[test]
fn incremental_decoder_emits_chunk_then_done() {
    let mut d = BodyDecoder::new();

    let (n1, e1) = d.decode(b"5\r\nhello\r\n0\r\n\r\n").unwrap();
    match e1 {
        DecodeEvent::Chunk(c) => assert_eq!(c, b"hello"),
        _ => panic!("expected chunk"),
    }

    let (n2, e2) = d.decode(&b"5\r\nhello\r\n0\r\n\r\n"[n1..]).unwrap();
    match e2 {
        DecodeEvent::Done(t) => assert!(t.is_empty()),
        _ => panic!("expected done"),
    }
    assert!(n2 > 0);
}

#[test]
fn incremental_decoder_needs_more_for_partial_chunk() {
    let mut d = BodyDecoder::new();
    let (n1, e1) = d.decode(b"5\r\nhe").unwrap();
    assert_eq!(n1, 3);
    assert!(matches!(e1, DecodeEvent::NeedMore));

    let (n2, e2) = d.decode(b"hello\r\n0\r\n\r\n").unwrap();
    match e2 {
        DecodeEvent::Chunk(c) => assert_eq!(c, b"hello"),
        _ => panic!("expected chunk"),
    }
    assert!(n2 > 0);
}
