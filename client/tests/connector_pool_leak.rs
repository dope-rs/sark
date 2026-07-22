mod common;

use std::io::Write;
use std::net::SocketAddr;
use std::time::Duration;

use common::{raw_http_response, run_gets, run_gets_with_gap, spawn_raw_server};
use sark_client::connector::Config;

fn short_config() -> Config {
    Config::new("127.0.0.1")
        .request_timeout(Duration::from_secs(2))
        .idle_timeout(Duration::from_secs(30))
}

#[test]
fn pool_recovers_from_silent_keepalive_close() {
    let server = spawn_raw_server(|stream, _req| {
        let resp = raw_http_response("HTTP/1.1 200 OK", &[("Content-Length", "2")], b"ok");
        let _ = stream.write_all(&resp);
        let _ = stream.flush();
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let codes = run_gets(
        addr,
        short_config(),
        2,
        &["/1", "/2", "/3", "/4", "/5", "/6", "/7", "/8"],
    )
    .expect("pool must recover from silent server-side keep-alive closes");
    assert_eq!(codes, vec![200, 200, 200, 200, 200, 200, 200, 200]);
}

#[test]
fn pool_recovers_after_idle_stale_recycle() {
    let server = spawn_raw_server(|stream, _req| {
        let resp = raw_http_response("HTTP/1.1 200 OK", &[("Content-Length", "2")], b"ok");
        let _ = stream.write_all(&resp);
        let _ = stream.flush();
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    let config = Config::new("127.0.0.1")
        .request_timeout(Duration::from_secs(2))
        .idle_timeout(Duration::from_millis(100));

    let codes = run_gets_with_gap(
        addr,
        config,
        2,
        &["/1", "/2", "/3", "/4"],
        &["/5", "/6", "/7", "/8"],
        Duration::from_millis(400),
    )
    .expect("second batch must not starve after idle-stale recycle");
    assert_eq!(codes, vec![200, 200, 200, 200, 200, 200, 200, 200]);
}
