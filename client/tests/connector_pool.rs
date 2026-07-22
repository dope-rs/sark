mod common;

use std::io::Write;
use std::net::SocketAddr;

use common::{raw_http_response, run_gets, spawn_raw_server};
use sark_client::connector::{Config, Port};

#[test]
fn invalid_request_pool_is_rejected_at_factory_creation() {
    assert!(Port::factory(Config::new("127.0.0.1").request_pool(0, 1), 1, 1).is_err());
    assert!(Port::factory(Config::new("127.0.0.1").request_pool(1, 0), 1, 1).is_err());
}

#[test]
fn pool_survives_connection_close_each_response() {
    let server = spawn_raw_server(|stream, _req| {
        let resp = raw_http_response(
            "HTTP/1.1 200 OK",
            &[("Connection", "close"), ("Content-Length", "2")],
            b"ok",
        );
        let _ = stream.write_all(&resp);
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let codes = run_gets(
        addr,
        Config::new("127.0.0.1"),
        2,
        &["/1", "/2", "/3", "/4", "/5", "/6"],
    )
    .expect("all gets succeed across a capacity-2 pool with close-after-response");
    assert_eq!(codes, vec![200, 200, 200, 200, 200, 200]);
}

#[test]
fn pool_capacity_four_drains_long_sequence() {
    let server = spawn_raw_server(|stream, _req| {
        let resp = raw_http_response(
            "HTTP/1.1 200 OK",
            &[("Connection", "close"), ("Content-Length", "4")],
            b"done",
        );
        let _ = stream.write_all(&resp);
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let paths: &[&str] = &["/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h", "/i", "/j"];
    let codes = run_gets(addr, Config::new("127.0.0.1"), 4, paths).expect("long sequence drains");
    assert_eq!(codes.len(), 10);
    assert!(codes.iter().all(|&c| c == 200), "all 200: {codes:?}");
}
