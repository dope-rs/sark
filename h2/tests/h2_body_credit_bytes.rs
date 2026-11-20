pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::frame::numbers::ErrorCode;

use crate::common::body::{LIMITS, PROBE, Peer, headers, serve};

#[test]
fn rejects_connection_bytes() {
    let mut server = LIMITS;
    server.max_request_body_bytes = 3;
    server.max_connection_body_bytes = 2;
    serve(server, |client, socket| {
        let stream = client
            .streams()
            .headers()
            .request(
                headers(b"/connection-limit", b"3").expect("request fields"),
                Boundary::More,
            )
            .expect("start request within its byte limit");
        client
            .try_send_all(stream, b"abc", true)
            .expect("H2 request DATA");
        client.try_flush(socket).expect("H2 peer flush");
        assert_eq!(
            client
                .try_reset(socket, stream)
                .expect("H2 peer reset"),
            ErrorCode::EnhanceYourCalm,
        );
    });
}

#[test]
fn returns_fragment_owners() {
    serve(LIMITS, |client, socket| {
        let first = client
            .streams()
            .headers()
            .request(
                headers(b"/first-held", b"4").expect("request fields"),
                Boundary::More,
            )
            .expect("start first held stream");
        client
            .try_send_all(first, b"aa", false)
            .expect("H2 request DATA");
        client.control().ping(PROBE).expect("queue progress probe");
        client.try_flush(socket).expect("H2 peer flush");
        client.try_ping(socket).expect("H2 peer progress");

        let second = client
            .streams()
            .headers()
            .request(
                headers(b"/second-held", b"4").expect("request fields"),
                Boundary::More,
            )
            .expect("start second held stream");
        client
            .try_send_all(second, b"cc", false)
            .expect("H2 request DATA");
        client
            .control()
            .ping(PROBE)
            .expect("queue second progress probe");
        client.try_flush(socket).expect("H2 peer flush");
        client.try_ping(socket).expect("H2 peer progress");

        let other = client
            .streams()
            .headers()
            .request(
                headers(b"/independent", b"2").expect("request fields"),
                Boundary::More,
            )
            .expect("start independent stream");
        client
            .try_send_all(other, b"ok", true)
            .expect("H2 request DATA");
        client.try_flush(socket).expect("H2 peer flush");
        assert_eq!(
            client
                .try_read(socket, other)
                .expect("H2 response body"),
            b"ok",
            "incomplete first fragments must return all receive owners",
        );

        client
            .try_send_all(first, b"bb", true)
            .expect("H2 request DATA");
        client.try_flush(socket).expect("H2 peer flush");
        assert_eq!(
            client
                .try_read(socket, first)
                .expect("H2 response body"),
            b"aabb"
        );

        client
            .try_send_all(second, b"dd", true)
            .expect("H2 request DATA");
        client.try_flush(socket).expect("H2 peer flush");
        assert_eq!(
            client
                .try_read(socket, second)
                .expect("H2 response body"),
            b"ccdd"
        );
    });
}

#[test]
fn headers_reserve_no_bytes() {
    let mut server = LIMITS;
    server.max_request_body_bytes = 4;
    server.max_connection_body_bytes = 4;
    serve(server, |client, socket| {
        let pending = client
            .streams()
            .headers()
            .request(
                headers(b"/pending", b"4").expect("request fields"),
                Boundary::More,
            )
            .expect("start pending stream");

        let next = client
            .streams()
            .headers()
            .request(
                headers(b"/next", b"4").expect("request fields"),
                Boundary::More,
            )
            .expect("start next stream");
        client
            .try_send_all(next, b"next", true)
            .expect("H2 request DATA");
        client.try_flush(socket).expect("H2 peer flush");
        assert_eq!(
            client
                .try_read(socket, next)
                .expect("H2 response body"),
            b"next"
        );

        let key = client
            .status()
            .stream_key(pending)
            .expect("active request stream");
        client
            .streams()
            .try_cancel(key, ErrorCode::Cancel)
            .expect("cancel pending stream");
        client.try_flush(socket).expect("H2 peer flush");
    });
}

#[test]
fn releases_mismatch_budget() {
    let mut server = LIMITS;
    server.max_request_body_bytes = 4;
    server.max_connection_body_bytes = 4;
    serve(server, |client, socket| {
        let bad = client
            .streams()
            .headers()
            .request(
                headers(b"/malformed", b"4").expect("request fields"),
                Boundary::More,
            )
            .expect("start malformed stream");
        client
            .try_send_all(bad, b"n", false)
            .expect("retained request DATA");
        client.control().ping(PROBE).expect("queue progress probe");
        client.try_flush(socket).expect("H2 peer flush");
        client
            .try_ping(socket)
            .expect("retained input before FIN");
        client
            .try_send_all(bad, b"o", true)
            .expect("H2 request DATA");
        client.try_flush(socket).expect("H2 peer flush");
        assert_eq!(
            client.try_reset(socket, bad).expect("H2 peer reset"),
            ErrorCode::ProtocolError,
        );

        let next = client
            .streams()
            .headers()
            .request(
                headers(b"/next", b"4").expect("request fields"),
                Boundary::More,
            )
            .expect("start next stream");
        client
            .try_send_all(next, b"next", true)
            .expect("H2 request DATA");
        client.try_flush(socket).expect("H2 peer flush");
        assert_eq!(
            client
                .try_read(socket, next)
                .expect("H2 response body"),
            b"next"
        );
    });
}
