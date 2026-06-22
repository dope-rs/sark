use http::{HeaderValue, Method, Uri};

use super::*;
use crate::http::Request;

fn encoded_str(req: &Request) -> String {
    String::from_utf8(Wire::request(req)).unwrap()
}

#[test]
fn get_simple() {
    let req = Request::new(
        Method::GET,
        Uri::from_static("http://localhost:3000/api/data"),
    );
    let s = encoded_str(&req);

    assert!(s.starts_with("GET /api/data HTTP/1.1\r\n"));
    assert!(s.contains("Host: localhost:3000\r\n"));
    assert!(s.ends_with("\r\n\r\n"));
}

#[test]
fn post_with_body() {
    let mut req = Request::new(Method::POST, Uri::from_static("http://example.com/submit"));
    req.set_body_str("hello=world");
    let s = encoded_str(&req);

    assert!(s.starts_with("POST /submit HTTP/1.1\r\n"));
    assert!(s.contains("Host: example.com\r\n"));
    assert!(s.contains("Content-Length: 11\r\n"));
    assert!(s.ends_with("\r\n\r\nhello=world"));
}

#[test]
fn custom_headers() {
    let mut req = Request::new(Method::GET, Uri::from_static("http://api.example.com/"));
    req.headers_mut()
        .insert("authorization", HeaderValue::from_static("Bearer tok"));
    req.headers_mut()
        .insert("accept", HeaderValue::from_static("application/json"));
    let s = encoded_str(&req);

    assert!(s.contains("authorization: Bearer tok\r\n"));
    assert!(s.contains("accept: application/json\r\n"));
}

#[test]
fn preserves_query_string() {
    let req = Request::new(
        Method::GET,
        Uri::from_static("http://localhost/search?q=rust&page=1"),
    );
    let s = encoded_str(&req);
    assert!(s.starts_with("GET /search?q=rust&page=1 HTTP/1.1\r\n"));
}

#[test]
fn no_duplicate_host_when_user_set() {
    let mut req = Request::new(Method::GET, Uri::from_static("http://localhost/"));
    req.headers_mut()
        .insert("host", HeaderValue::from_static("custom-host"));
    let s = encoded_str(&req);

    let host_count = s.matches("ost:").count();
    assert_eq!(host_count, 1);
}

#[test]
fn default_port_not_appended() {
    let req = Request::new(Method::GET, Uri::from_static("http://example.com/"));
    let s = encoded_str(&req);
    assert!(s.contains("Host: example.com\r\n"));
    assert!(!s.contains(":80"));
}

#[test]
fn empty_body_no_content_length() {
    let req = Request::new(Method::GET, Uri::from_static("http://localhost/"));
    let s = encoded_str(&req);
    assert!(!s.contains("Content-Length"));
}

#[test]
fn chunk_prefix_encodes_hex_size_and_crlf() {
    let (prefix, len) = Wire::chunk_prefix(10);
    assert_eq!(&prefix[..len], b"a\r\n");
}
