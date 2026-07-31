mod common;

use std::net::SocketAddr;

use common::{run_get, run_stream_get, spawn_raw_server};
use sark_client::connector::Config;

#[test]
#[ignore = "L4b: cross-host redirect needs OnDemandHosts (ConnectSource 2nd impl)"]
fn cross_host_redirect_followed() {}

#[test]
fn buffered_body_until_eof() {
    let server = spawn_raw_server(|stream, _req| {
        let response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nbody until EOF";
        let _ = std::io::Write::write_all(stream, response);
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let response = run_get(addr, Config::new("127.0.0.1"), "/eof").expect("HTTP response");
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.body(), b"body until EOF");
}

#[test]
fn streaming_body_until_eof() {
    let server = spawn_raw_server(|stream, _req| {
        let response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nstreamed body";
        let _ = std::io::Write::write_all(stream, response);
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let (status, body, trailers, informational) =
        run_stream_get(addr, Config::new("127.0.0.1"), "/stream").expect("response stream");
    assert_eq!(status, 200);
    assert_eq!(body, b"streamed body");
    assert_eq!(trailers, 0);
    assert_eq!(informational, 0);
}

#[test]
fn informational_response_precedes_final_head() {
    let server = spawn_raw_server(|stream, _req| {
        let response = b"HTTP/1.1 100 Continue\r\n\r\n\
                         HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
        let _ = std::io::Write::write_all(stream, response);
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let (status, body, trailers, informational) =
        run_stream_get(addr, Config::new("127.0.0.1"), "/continue").expect("response stream");
    assert_eq!(status, 200);
    assert_eq!(body, b"ok");
    assert_eq!(trailers, 0);
    assert_eq!(informational, 1);
}
