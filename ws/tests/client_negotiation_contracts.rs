pub mod common;

use common::client::Probe;
use dope::core::driver::life::recovery;
use sark_http::h1::codec::decode::fields::Sink as _;
use sark_ws::{
    client::{self, headers},
    upgrade::{self, Negotiation},
};

#[test]
fn reject_negotiation() {
    for (name, offer) in [
        ("Sec-WebSocket-Extensions", Negotiation::Extensions),
        ("Sec-WebSocket-Protocol", Negotiation::Protocol),
    ] {
        let config = client::Config::new("localhost", "/");
        let config = config.expect("endpoint");
        assert!(matches!(config.with_header(name, "unsupported"),
            Err(client::Error::Header(headers::Error::Unsupported(actual))) if actual == offer));
        assert!(
            matches!(upgrade::response::Admission::default().push(name.as_bytes(), b"unsupported"),
            Err(upgrade::Error::Unsupported(actual)) if actual == offer)
        );
    }
}

#[test]
fn handshake_error_survives() {
    use std::{io::Read as _, net::TcpListener, thread, time::Duration};

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind peer");
    let address = listener.local_addr().expect("peer address");
    let peer = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept client");
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .expect("read timeout");
        assert_eq!(stream.read(&mut [0; 1]).expect("closed handshake"), 0);
    });
    let config = client::Config::new("localhost", "/");
    let config = config.expect("endpoint");
    let probe =
        Probe::new(address, config).with_headers(vec![("X-Large".into(), "x".repeat(16 * 1024))]);
    let probe = probe.expect("headers");
    let result = probe.close(recovery::Policy::new(|_| {}, common::recover), |result| {
        result.expect("WebSocket probe")
    });
    result
        .as_ref()
        .map_err(|cause| cause.kind())
        .expect("topology");
    let seen = result.ok().expect("checked topology");
    assert!(matches!(
        seen.close(),
        Some(client::CloseReason::Handshake(client::Error::Header(
            headers::Error::Length {
                additional: 16384,
                maximum: 16384,
                ..
            }
        )))
    ));
    peer.join().expect("peer");
}
