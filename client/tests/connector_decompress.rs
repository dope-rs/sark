mod common;

use std::io::Write;
use std::net::SocketAddr;

use common::{run_get, spawn_raw_server};
use sark_client::connector::{Config, DecompressionPolicy};

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

fn run_get_policy(
    addr: SocketAddr,
    policy: DecompressionPolicy,
    path: &'static str,
) -> Result<sark_core::http::Response, String> {
    run_get(addr, Config::with_decompression("127.0.0.1", policy), path)
}

#[test]
fn gzip_body_is_decompressed() {
    let payload = "Hello, gzip world! ".repeat(8);
    let compressed = gzip(payload.as_bytes());
    let server = spawn_raw_server(move |stream, _req| {
        let mut resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            compressed.len()
        )
        .into_bytes();
        resp.extend_from_slice(&compressed);
        let _ = stream.write_all(&resp);
        let _ = stream.flush();
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let resp = run_get_policy(addr, DecompressionPolicy::Strict, "/").expect("gzip get");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(std::str::from_utf8(resp.body()).unwrap(), payload);
    assert!(resp.headers().get("content-encoding").is_none());
}

#[test]
fn invalid_gzip_strict_errors() {
    let server = spawn_raw_server(|stream, _req| {
        let body = b"not actually gzip";
        let mut resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        resp.extend_from_slice(body);
        let _ = stream.write_all(&resp);
        let _ = stream.flush();
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let err =
        run_get_policy(addr, DecompressionPolicy::Strict, "/").expect_err("strict must error");
    assert!(err.contains("gzip"), "err was: {err}");
}

#[test]
fn decompression_bomb_rejected() {
    let payload = vec![b'x'; 1024 * 1024];
    let compressed = gzip(&payload);
    let server = spawn_raw_server(move |stream, _req| {
        let mut resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            compressed.len()
        )
        .into_bytes();
        resp.extend_from_slice(&compressed);
        let _ = stream.write_all(&resp);
        let _ = stream.flush();
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let config = Config::with_decompression("127.0.0.1", DecompressionPolicy::Strict)
        .max_response_body(4096);
    let err = run_get(addr, config, "/").expect_err("decompression bomb must be rejected");
    assert!(err.contains("size limit"), "err was: {err}");
}

#[test]
fn invalid_gzip_lenient_passes_through() {
    let raw = b"not actually gzip";
    let server = spawn_raw_server(move |stream, _req| {
        let mut resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            raw.len()
        )
        .into_bytes();
        resp.extend_from_slice(raw);
        let _ = stream.write_all(&resp);
        let _ = stream.flush();
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let resp = run_get_policy(addr, DecompressionPolicy::Lenient, "/").expect("lenient get");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.body(), raw);
}
