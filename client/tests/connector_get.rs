mod common;

use std::net::SocketAddr;

use common::run_get;
use sark_client::connector::Config;

#[test]
fn connector_plaintext_get() {
    let addr: SocketAddr = match std::env::var("HTTP_TEST_ADDR") {
        Ok(a) => a.parse().expect("HTTP_TEST_ADDR"),
        Err(_) => {
            eprintln!("HTTP_TEST_ADDR not set; skipping");
            return;
        }
    };

    let resp = run_get(
        addr,
        Config::new("127.0.0.1"),
        "/api/v3/ticker/price?symbol=BTCUSDT",
    )
    .expect("http get");
    assert_eq!(resp.status().as_u16(), 200);
    let body = std::str::from_utf8(resp.body()).expect("utf8 body");
    assert!(body.contains("BTCUSDT"), "body was: {body}");
}
