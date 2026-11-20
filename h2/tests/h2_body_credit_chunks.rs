pub mod common;

use sark_core::manifold::stream::Boundary;
use sark_h2::{conn::DataOutcome, frame::numbers::ErrorCode};

use crate::common::body::{LIMITS, PROBE, Peer, headers, serve};

#[test]
fn rejects_second_owner() {
    let mut server = LIMITS;
    server.max_request_body_chunks = 1;
    server.max_connection_body_chunks = 2;
    serve(server, |client, socket| {
        let stream = client
            .streams()
            .headers()
            .request(
                headers(b"/too-many-owners", b"2").expect("request fields"),
                Boundary::More,
            )
            .expect("start bounded request");
        client
            .try_send_all(stream, b"a", false)
            .expect("H2 request DATA");
        client
            .control()
            .ping(PROBE)
            .expect("queue first-owner probe");
        client.try_flush(socket).expect("H2 peer flush");
        client.try_ping(socket).expect("H2 peer progress");

        client
            .try_send_all(stream, b"b", true)
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
fn reset_returns_credit() {
    let mut server = LIMITS;
    server.max_request_body_chunks = 2;
    server.max_connection_body_chunks = 1;
    serve(server, |client, socket| {
        let old = client
            .streams()
            .headers()
            .request(
                headers(b"/abandoned", b"2").expect("request fields"),
                Boundary::More,
            )
            .expect("start abandoned request");
        client
            .try_send_all(old, b"a", false)
            .expect("H2 request DATA");
        client
            .control()
            .ping(PROBE)
            .expect("queue retained-owner probe");
        client.try_flush(socket).expect("H2 peer flush");
        client.try_ping(socket).expect("H2 peer progress");

        let key = client
            .status()
            .stream_key(old)
            .expect("active request stream");
        client
            .streams()
            .try_cancel(key, ErrorCode::Cancel)
            .expect("reset retained request");
        client.control().ping(PROBE).expect("queue reset probe");
        client.try_flush(socket).expect("H2 peer flush");
        client.try_ping(socket).expect("H2 peer progress");

        let next = client
            .streams()
            .headers()
            .request(
                headers(b"/replacement", b"1").expect("request fields"),
                Boundary::More,
            )
            .expect("start replacement request");
        client
            .try_send_all(next, b"b", true)
            .expect("H2 request DATA");
        client.try_flush(socket).expect("H2 peer flush");
        assert_eq!(
            client
                .try_read(socket, next)
                .expect("H2 response body"),
            b"b"
        );
    });
}

#[test]
fn rejects_atomically() {
    let mut server = LIMITS;
    server.max_request_body_chunks = 2;
    server.max_connection_body_chunks = 1;
    serve(server, |client, socket| {
        let owner = client
            .streams()
            .headers()
            .request(
                headers(b"/owner", b"1").expect("request fields"),
                Boundary::More,
            )
            .expect("start owner request");
        client
            .try_send_all(owner, b"a", false)
            .expect("H2 request DATA");
        client.control().ping(PROBE).expect("queue owner probe");
        client.try_flush(socket).expect("H2 peer flush");
        client.try_ping(socket).expect("H2 peer progress");

        let rejected = client
            .streams()
            .headers()
            .request(
                headers(b"/rejected", b"1").expect("request fields"),
                Boundary::More,
            )
            .expect("start competing request");
        client
            .try_send_all(rejected, b"x", true)
            .expect("H2 request DATA");
        client.try_flush(socket).expect("H2 peer flush");
        assert_eq!(
            client
                .try_reset(socket, rejected)
                .expect("H2 peer reset"),
            ErrorCode::EnhanceYourCalm,
        );

        assert_eq!(
            client
                .streams()
                .try_send(owner, b"", Boundary::End)
                .expect("owner stream remains writable after peer rejection"),
            DataOutcome::Ended,
        );
        client.try_flush(socket).expect("H2 peer flush");
        assert_eq!(
            client
                .try_read(socket, owner)
                .expect("H2 response body"),
            b"a"
        );
    });
}

#[test]
fn empty_fin_uses_no_credit() {
    let mut server = LIMITS;
    server.max_request_body_chunks = 0;
    server.max_connection_body_chunks = 0;
    serve(server, |client, socket| {
        let stream = client
            .streams()
            .headers()
            .request(
                headers(b"/empty", b"0").expect("request fields"),
                Boundary::More,
            )
            .expect("start empty request");
        assert_eq!(
            client
                .streams()
                .try_send(stream, b"", Boundary::End)
                .expect("empty FIN fits zero chunk credit"),
            DataOutcome::Ended,
        );
        client.try_flush(socket).expect("H2 peer flush");
        assert!(
            client
                .try_read(socket, stream)
                .expect("H2 response body")
                .is_empty()
        );
    });
}
