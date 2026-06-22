mod common;

use std::io::Write;
use std::net::SocketAddr;
use std::time::Duration;

use common::{raw_http_response, run_gets, run_gets_with_gap, spawn_raw_server};
use sark_client::connector::Session;

fn short_session() -> Session {
    Session::new("127.0.0.1")
        .with_request_timeout(Duration::from_secs(2))
        .with_idle_timeout(Duration::from_secs(30))
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
        short_session(),
        2,
        &["/1", "/2", "/3", "/4", "/5", "/6", "/7", "/8"],
    )
    .expect("pool must recover from silent server-side keep-alive closes");
    assert_eq!(codes, vec![200, 200, 200, 200, 200, 200, 200, 200]);
}

/// Exercises the idle-stale recycle path that caused the 2026-06-17 fixture
/// starvation: after the first batch the connector redials idle connections to
/// fill capacity; with a gap longer than `idle_timeout` they go stale and the
/// second batch's acquire recycles them via `poke_close`. The bug was that
/// `poke_close` -> `drain_close` freed the pool slot but never reconciled the
/// dialer, pinning the upstream slot in `Busy` so `poll_connect` would not redial.
///
/// NOTE: on loopback the eventual `close_slot` reconcile lands almost instantly,
/// so this test passes even on the buggy code — it guards the path, not the
/// timing. The starvation itself is RTT-dependent and is reproduced end-to-end by
/// the `feed --hammer` harness against the real upstream (45s stalls -> <4s after
/// the fix). Keep this as a smoke test for the recycle path; rely on `--hammer`
/// for the timing regression.
#[test]
fn pool_recovers_after_idle_stale_recycle() {
    let server = spawn_raw_server(|stream, _req| {
        let resp = raw_http_response("HTTP/1.1 200 OK", &[("Content-Length", "2")], b"ok");
        let _ = stream.write_all(&resp);
        let _ = stream.flush();
    });
    let addr: SocketAddr = server.addr().parse().expect("addr");

    // idle_timeout 100ms, gap 400ms => connections dialed during the gap go stale
    // and get recycled when batch 2 acquires. request_timeout 2s => a leaked,
    // starving pool fails fast instead of hanging.
    let session = Session::new("127.0.0.1")
        .with_request_timeout(Duration::from_secs(2))
        .with_idle_timeout(Duration::from_millis(100));

    let codes = run_gets_with_gap(
        addr,
        session,
        2,
        &["/1", "/2", "/3", "/4"],
        &["/5", "/6", "/7", "/8"],
        Duration::from_millis(400),
    )
    .expect("second batch must not starve after idle-stale recycle");
    assert_eq!(codes, vec![200, 200, 200, 200, 200, 200, 200, 200]);
}
