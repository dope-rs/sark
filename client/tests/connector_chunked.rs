mod common;

use std::net::SocketAddr;

use common::{run_get, spawn_raw_server};
use sark_client::connector::Config;

#[test]
fn connector_chunked_get() {
    let server = spawn_raw_server(|stream, _req| {
        let resp = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
                     5\r\nhello\r\n7\r\n, world\r\n0\r\n\r\n";
        let _ = std::io::Write::write_all(stream, resp);
        let _ = std::io::Write::flush(stream);
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let resp = run_get(addr, Config::new("127.0.0.1"), "/chunked").expect("http get");
    assert_eq!(resp.status().as_u16(), 200);
    let body = std::str::from_utf8(resp.body()).expect("utf8 body");
    assert_eq!(body, "hello, world");
}

#[test]
fn connector_chunked_get_large_body() {
    const CHUNK: usize = 16 * 1024;
    const CHUNKS: usize = 160;
    let server = spawn_raw_server(|stream, _req| {
        let _ = stream.set_nonblocking(false);
        let mut resp = Vec::with_capacity(CHUNK * CHUNKS + 4096);
        resp.extend_from_slice(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
        );
        let payload = vec![b'x'; CHUNK];
        for _ in 0..CHUNKS {
            resp.extend_from_slice(format!("{CHUNK:x}\r\n").as_bytes());
            resp.extend_from_slice(&payload);
            resp.extend_from_slice(b"\r\n");
        }
        resp.extend_from_slice(b"0\r\n\r\n");
        let _ = std::io::Write::write_all(stream, &resp);
        let _ = std::io::Write::flush(stream);
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let resp = run_get(addr, Config::new("127.0.0.1"), "/big-chunked").expect("http get");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.body().len(),
        CHUNK * CHUNKS,
        "full chunked body reassembled"
    );
    assert!(resp.body().iter().all(|&b| b == b'x'), "body intact");
}

#[test]
fn connector_chunked_get_with_trailers() {
    let server = spawn_raw_server(|stream, _req| {
        let resp = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
                     3\r\nabc\r\n3\r\ndef\r\n0\r\nX-Checksum: 42\r\n\r\n";
        let _ = std::io::Write::write_all(stream, resp);
        let _ = std::io::Write::flush(stream);
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let resp = run_get(addr, Config::new("127.0.0.1"), "/chunked-trailers").expect("http get");
    assert_eq!(resp.status().as_u16(), 200);
    let body = std::str::from_utf8(resp.body()).expect("utf8 body");
    assert_eq!(body, "abcdef");
}
