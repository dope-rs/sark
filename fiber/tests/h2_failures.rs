pub mod common;

use std::io::Read as _;

use common::h2::{
    apps::{LARGE_JSON, LARGE_JSON_LEN},
    facts::*,
    peer::{Peer, try_send},
    server::{CONFIG, Request, Server, check},
};

use dope_test::harness::server::Pair;
use sark_core::{life::mode, manifold::stream::Boundary};
use sark_h2::frame::numbers::ErrorCode;

#[test]
fn length_credit() {
    let mut config = CONFIG;
    config.max_request_body_bytes = LARGE_JSON_LEN + 1;
    config.max_connection_body_bytes = LARGE_JSON_LEN;
    check(config, 8, |peer| {
        let longer = (LARGE_JSON_LEN + 1).to_string();
        let short = peer
            .client
            .streams()
            .headers()
            .request(
                Request::new(b"/large")
                    .try_length(longer.as_bytes())
                    .expect("valid request fields"),
                Boundary::More,
            )
            .expect("start streaming length underflow");
        try_send(peer.client, short, LARGE_JSON, Boundary::End).expect("H2 peer send");
        peer.try_flush().expect("H2 peer flush");
        assert_eq!(
            peer.try_reset(short).expect("H2 peer reset"),
            ErrorCode::ProtocolError,
        );

        let shorter = (LARGE_JSON_LEN - 1).to_string();
        let overflow = peer
            .client
            .streams()
            .headers()
            .request(
                Request::new(b"/large")
                    .try_length(shorter.as_bytes())
                    .expect("valid request fields"),
                Boundary::More,
            )
            .expect("start streaming length overflow");
        try_send(peer.client, overflow, LARGE_JSON, Boundary::End).expect("H2 peer send");
        peer.try_flush().expect("H2 peer flush");
        assert_eq!(
            peer.try_reset(overflow).expect("H2 peer reset"),
            ErrorCode::ProtocolError,
        );

        let bad = peer
            .client
            .streams()
            .headers()
            .request(
                Request::new(b"/large")
                    .try_length(b"1")
                    .expect("valid request fields"),
                Boundary::More,
            )
            .expect("start malformed streaming body");
        try_send(peer.client, bad, b"x", Boundary::End).expect("H2 peer send");
        peer.try_flush().expect("H2 peer flush");
        assert_eq!(
            peer.try_reset(bad).expect("H2 peer reset"),
            ErrorCode::ProtocolError,
        );

        let exact = LARGE_JSON_LEN.to_string();
        let next = peer
            .client
            .streams()
            .headers()
            .request(
                Request::new(b"/large")
                    .try_length(exact.as_bytes())
                    .expect("valid request fields"),
                Boundary::More,
            )
            .expect("start replacement streaming request");
        try_send(peer.client, next, LARGE_JSON, Boundary::End).expect("H2 peer send");
        peer.try_flush().expect("H2 peer flush");
        let (_, response) = peer.try_response(next).expect("H2 peer response");
        assert_eq!(response, b"large");
    })
    .expect("H2 streaming length and credit E2E");
}

#[test]
fn pre_fin_credit() {
    let mut config = CONFIG;
    config.max_request_body_chunks = 1;
    config.max_connection_body_chunks = 1;
    check(config, 8, |peer| {
        let streamed = Request::new(b"/large")
            .try_start(peer.client, Boundary::More)
            .expect("start streaming JSON request");
        try_send(peer.client, streamed, br#"{"value":"#, Boundary::More).expect("H2 peer send");
        let progress = Request::new(b"/quick")
            .try_start(peer.client, Boundary::End)
            .expect("start progress probe");
        peer.try_flush().expect("H2 peer flush");
        let (_, response) = peer.try_response(progress).expect("H2 peer response");
        assert_eq!(response, b"quick");

        try_send(peer.client, streamed, b"1}", Boundary::End).expect("H2 peer send");
        peer.try_flush().expect("H2 peer flush");
        let (_, response) = peer.try_response(streamed).expect("H2 peer response");
        assert_eq!(response, b"large");
    })
    .expect("H2 pre-FIN chunk-credit E2E");
}

#[test]
fn reset_reuses_slot() {
    for streaming in [false, true] {
        let harness = Pair::bind().expect("H2 harness");
        let config = CONFIG;
        harness
            .check(
                move |context, listener| {
                    let server = Server::new(config, 1, State::default());
                    server
                        .try_run(context, listener, mode::Disabled)
                        .map(|output| {
                            if streaming {
                                assert!(server.state.stream.polls.get() >= 2);
                                assert_eq!(server.state.stream.drops.get(), 1);
                                assert!(!server.state.stream.moved.get());
                            }
                            output
                        })
                },
                |address| {
                    let (mut socket, mut client) = Peer::connect(address);
                    let mut peer = Peer::new(&mut socket, &mut client);
                    let first = Request::new(if streaming { b"/stream" } else { b"/blocked" })
                        .try_start(peer.client, Boundary::End)
                        .expect("start first blocked task");
                    if streaming {
                        use sark_h2::conn::event::Event;
                        peer.try_flush().expect("stream request");
                        let mut received = Vec::new();
                        let mut input = [0; 4096];
                        while received.is_empty() {
                            let count = peer.socket.read(&mut input).expect("first stream chunk");
                            assert_ne!(count, 0);
                            peer.client
                                .ingress()
                                .ingest(&input[..count])
                                .expect("ingress");
                            while let Some(event) = peer.client.ingress().poll_event() {
                                if let Event::Data {
                                    stream_id,
                                    data,
                                    boundary,
                                } = event
                                {
                                    assert_eq!(stream_id, first);
                                    assert_eq!(boundary, Boundary::More);
                                    data.for_each(|part| received.extend_from_slice(part));
                                }
                            }
                            peer.try_flush().expect("control");
                        }
                        assert_eq!(received, b"held");
                    }
                    let full = Request::new(b"/blocked")
                        .try_start(peer.client, Boundary::End)
                        .expect("start saturated task");
                    peer.try_flush().expect("H2 peer flush");
                    assert_eq!(
                        peer.try_reset(full).expect("H2 peer reset"),
                        ErrorCode::RefusedStream,
                    );

                    let first = peer
                        .client
                        .status()
                        .stream_key(first)
                        .expect("active blocked stream");
                    peer.client
                        .streams()
                        .try_cancel(first, ErrorCode::Cancel)
                        .expect("cancel exact blocked task");
                    let next = Request::new(b"/quick")
                        .try_start(peer.client, Boundary::End)
                        .expect("reuse task slot");
                    peer.try_flush().expect("H2 peer flush");
                    let (_, response) = peer.try_response(next).expect("H2 peer response");
                    assert_eq!(response, b"quick");
                },
                |result| {
                    result.expect("H2 setup");
                },
            )
            .expect("H2 exact cancellation runtime");
    }
}

#[test]
fn route_rollback() {
    check(CONFIG, 3, |peer| {
        let first = Request::new(b"/blocked")
            .try_start(peer.client, Boundary::End)
            .expect("start first route task");
        let second = Request::new(b"/blocked")
            .try_start(peer.client, Boundary::End)
            .expect("start second route task");
        let rejected = Request::new(b"/blocked")
            .try_start(peer.client, Boundary::End)
            .expect("start route-capacity probe");
        peer.try_flush().expect("H2 peer flush");
        assert_eq!(
            peer.try_reset(rejected).expect("H2 peer reset"),
            ErrorCode::RefusedStream,
        );

        let next = Request::new(b"/quick")
            .try_start(peer.client, Boundary::End)
            .expect("reuse rolled-back H2 book slot");
        peer.try_flush().expect("H2 peer flush");
        let (_, response) = peer.try_response(next).expect("H2 peer response");
        assert_eq!(response, b"quick");

        let first = peer
            .client
            .status()
            .stream_key(first)
            .expect("active first route stream");
        let second = peer
            .client
            .status()
            .stream_key(second)
            .expect("active second route stream");
        peer.client
            .streams()
            .try_cancel(first, ErrorCode::Cancel)
            .expect("cancel first route task");
        peer.client
            .streams()
            .try_cancel(second, ErrorCode::Cancel)
            .expect("cancel second route task");
        peer.try_flush().expect("H2 peer flush");
    })
    .expect("H2 transactional admission runtime");
}

#[test]
fn reset_body_credit() {
    let mut config = CONFIG;
    config.max_connection_body_bytes = br#"{"value":1}"#.len();
    config.max_request_body_chunks = 1;
    config.max_connection_body_chunks = 1;
    check(config, 1, |peer| {
        let stale = Request::new(b"/large")
            .try_start(peer.client, Boundary::More)
            .expect("start abandoned streaming request");
        try_send(peer.client, stale, br#"{"value":"#, Boundary::More).expect("H2 peer send");
        peer.try_flush().expect("H2 peer flush");

        let stale = peer
            .client
            .status()
            .stream_key(stale)
            .expect("active pending stream");
        peer.client
            .streams()
            .try_cancel(stale, ErrorCode::Cancel)
            .expect("reset pending builder");
        let next = Request::new(b"/large")
            .try_start(peer.client, Boundary::More)
            .expect("start replacement streaming request");
        try_send(peer.client, next, br#"{"value":1}"#, Boundary::End).expect("H2 peer send");
        peer.try_flush().expect("H2 peer flush");
        let (_, response) = peer.try_response(next).expect("H2 peer response");
        assert_eq!(response, b"large");
    })
    .expect("H2 pending reset runtime");
}
