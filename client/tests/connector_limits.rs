mod common;

use std::net::SocketAddr;

use common::{run_get, spawn_raw_server};
use sark_client::connector::Session;

fn run_get_cap(
    addr: SocketAddr,
    cap: usize,
    path: &'static str,
) -> Result<sark_core::http::Response, String> {
    let mut session = Session::new("127.0.0.1");
    session.max_response_body(cap);
    run_get(addr, session, path)
}

#[test]
fn response_under_cap_succeeds() {
    let server = spawn_raw_server(|stream, _req| {
        let body = "Hello, World!";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = std::io::Write::write_all(stream, resp.as_bytes());
        let _ = std::io::Write::flush(stream);
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let resp = run_get_cap(addr, 1024, "/").expect("under-cap response");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(std::str::from_utf8(resp.body()).unwrap(), "Hello, World!");
}

#[test]
fn response_over_cap_errors() {
    let server = spawn_raw_server(|stream, _req| {
        let body = vec![b'x'; 4096];
        let mut resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        resp.extend_from_slice(&body);
        let _ = std::io::Write::write_all(stream, &resp);
        let _ = std::io::Write::flush(stream);
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let err = run_get_cap(addr, 1024, "/").expect_err("over-cap must error");
    assert!(err.contains("size limit"), "err was: {err}");
}
