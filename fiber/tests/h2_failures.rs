pub mod common;

use std::time::Duration;

use common::h2::recover;
use common::h2::{
    apps::{LARGE_JSON, LARGE_JSON_LEN},
    facts::*,
    peer::{Peer, Sender},
    server::{CONFIG, Request, Server},
};

use dope_test::harness::server::Pair;
use sark_core::{life::mode, manifold::stream::Boundary};
use sark_h2::frame::numbers::ErrorCode;

#[test]
fn length_credit() {
    let harness = Pair::bind().expect("H2 harness");
    let mut config = CONFIG;
    config.max_request_body_bytes = LARGE_JSON_LEN + 1;
    config.max_connection_body_bytes = LARGE_JSON_LEN;
    harness
        .check(
            move |context, listener| {
                Server::new(config, 8, State::default()).try_run(
                    context,
                    listener,
                    mode::Disabled,
                    recover,
                    |result| result.expect("H2 execution"),
                )
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");

                let longer = (LARGE_JSON_LEN + 1).to_string();
                let short = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/large")
                            .try_length(longer.as_bytes())
                            .expect("valid request fields"),
                        Boundary::More,
                    )
                    .expect("start streaming length underflow");
                Sender::new(&mut client)
                    .try_send(short, LARGE_JSON, Boundary::End)
                    .expect("H2 peer send");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                assert_eq!(
                    Peer::new(&mut socket, &mut client)
                        .try_reset(short)
                        .expect("H2 peer reset"),
                    ErrorCode::ProtocolError,
                );

                let shorter = (LARGE_JSON_LEN - 1).to_string();
                let overflow = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/large")
                            .try_length(shorter.as_bytes())
                            .expect("valid request fields"),
                        Boundary::More,
                    )
                    .expect("start streaming length overflow");
                Sender::new(&mut client)
                    .try_send(overflow, LARGE_JSON, Boundary::End)
                    .expect("H2 peer send");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                assert_eq!(
                    Peer::new(&mut socket, &mut client)
                        .try_reset(overflow)
                        .expect("H2 peer reset"),
                    ErrorCode::ProtocolError,
                );

                let bad = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/large")
                            .try_length(b"1")
                            .expect("valid request fields"),
                        Boundary::More,
                    )
                    .expect("start malformed streaming body");
                Sender::new(&mut client)
                    .try_send(bad, b"x", Boundary::End)
                    .expect("H2 peer send");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                assert_eq!(
                    Peer::new(&mut socket, &mut client)
                        .try_reset(bad)
                        .expect("H2 peer reset"),
                    ErrorCode::ProtocolError,
                );

                let exact = LARGE_JSON_LEN.to_string();
                let next = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/large")
                            .try_length(exact.as_bytes())
                            .expect("valid request fields"),
                        Boundary::More,
                    )
                    .expect("start replacement streaming request");
                Sender::new(&mut client)
                    .try_send(next, LARGE_JSON, Boundary::End)
                    .expect("H2 peer send");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                let (_, response) = Peer::new(&mut socket, &mut client)
                    .try_response(next)
                    .expect("H2 peer response");
                assert_eq!(response, b"large");
            },
            |result| {
                result.expect("H2 setup");
            },
        )
        .expect("H2 streaming length and credit E2E");
}

#[test]
fn pre_fin_credit() {
    let harness = Pair::bind().expect("H2 harness");
    let mut config = CONFIG;
    config.max_request_body_chunks = 1;
    config.max_connection_body_chunks = 1;
    harness
        .check(
            move |context, listener| {
                Server::new(config, 8, State::default()).try_run(
                    context,
                    listener,
                    mode::Disabled,
                    recover,
                    |result| result.expect("H2 execution"),
                )
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                let streamed = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/large")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::More,
                    )
                    .expect("start streaming JSON request");
                Sender::new(&mut client)
                    .try_send(streamed, br#"{"value":"#, Boundary::More)
                    .expect("H2 peer send");
                let progress = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/quick")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("start progress probe");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                let (_, response) = Peer::new(&mut socket, &mut client)
                    .try_response(progress)
                    .expect("H2 peer response");
                assert_eq!(response, b"quick");

                Sender::new(&mut client)
                    .try_send(streamed, b"1}", Boundary::End)
                    .expect("H2 peer send");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                let (_, response) = Peer::new(&mut socket, &mut client)
                    .try_response(streamed)
                    .expect("H2 peer response");
                assert_eq!(response, b"large");
            },
            |result| {
                result.expect("H2 setup");
            },
        )
        .expect("H2 pre-FIN chunk-credit E2E");
}

#[test]
fn reset_reuses_slot() {
    let harness = Pair::bind().expect("H2 harness");
    let config = CONFIG;
    harness
        .check(
            move |context, listener| {
                Server::new(config, 1, State::default()).try_run(
                    context,
                    listener,
                    mode::Disabled,
                    recover,
                    |result| result.expect("H2 execution"),
                )
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                let first = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/blocked")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("start first blocked task");
                let full = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/blocked")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("start saturated task");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                assert_eq!(
                    Peer::new(&mut socket, &mut client)
                        .try_reset(full)
                        .expect("H2 peer reset"),
                    ErrorCode::RefusedStream,
                );

                let first = client
                    .status()
                    .stream_key(first)
                    .expect("active blocked stream");
                client
                    .streams()
                    .try_cancel(first, ErrorCode::Cancel)
                    .expect("cancel exact blocked task");
                let next = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/quick")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("reuse task slot");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                let (_, response) = Peer::new(&mut socket, &mut client)
                    .try_response(next)
                    .expect("H2 peer response");
                assert_eq!(response, b"quick");
            },
            |result| {
                result.expect("H2 setup");
            },
        )
        .expect("H2 exact cancellation runtime");
}

#[test]
fn route_rollback() {
    let harness = Pair::bind().expect("H2 harness");
    let config = CONFIG;
    harness
        .check(
            move |context, listener| {
                Server::new(config, 3, State::default()).try_run(
                    context,
                    listener,
                    mode::Disabled,
                    recover,
                    |result| result.expect("H2 execution"),
                )
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                let first = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/blocked")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("start first route task");
                let second = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/blocked")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("start second route task");
                let rejected = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/blocked")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("start route-capacity probe");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                assert_eq!(
                    Peer::new(&mut socket, &mut client)
                        .try_reset(rejected)
                        .expect("H2 peer reset"),
                    ErrorCode::RefusedStream,
                );

                let next = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/quick")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::End,
                    )
                    .expect("reuse rolled-back H2 book slot");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                let (_, response) = Peer::new(&mut socket, &mut client)
                    .try_response(next)
                    .expect("H2 peer response");
                assert_eq!(response, b"quick");

                let first = client
                    .status()
                    .stream_key(first)
                    .expect("active first route stream");
                let second = client
                    .status()
                    .stream_key(second)
                    .expect("active second route stream");
                client
                    .streams()
                    .try_cancel(first, ErrorCode::Cancel)
                    .expect("cancel first route task");
                client
                    .streams()
                    .try_cancel(second, ErrorCode::Cancel)
                    .expect("cancel second route task");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
            },
            |result| {
                result.expect("H2 setup");
            },
        )
        .expect("H2 transactional admission runtime");
}

#[test]
fn reset_body_credit() {
    let harness = Pair::bind().expect("H2 harness");
    let mut config = CONFIG;
    config.max_connection_body_bytes = br#"{"value":1}"#.len();
    config.max_request_body_chunks = 1;
    config.max_connection_body_chunks = 1;
    harness
        .check(
            move |context, listener| {
                Server::new(config, 1, State::default()).try_run(
                    context,
                    listener,
                    mode::Disabled,
                    recover,
                    |result| result.expect("H2 execution"),
                )
            },
            |address| {
                let (mut socket, mut client) =
                    Peer::try_connect(address, Duration::from_secs(3)).expect("H2 peer connection");
                let stale = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/large")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::More,
                    )
                    .expect("start abandoned streaming request");
                Sender::new(&mut client)
                    .try_send(stale, br#"{"value":"#, Boundary::More)
                    .expect("H2 peer send");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");

                let stale = client
                    .status()
                    .stream_key(stale)
                    .expect("active pending stream");
                client
                    .streams()
                    .try_cancel(stale, ErrorCode::Cancel)
                    .expect("reset pending builder");
                let next = client
                    .streams()
                    .headers()
                    .request(
                        Request::new(b"/large")
                            .try_headers()
                            .expect("valid request fields"),
                        Boundary::More,
                    )
                    .expect("start replacement streaming request");
                Sender::new(&mut client)
                    .try_send(next, br#"{"value":1}"#, Boundary::End)
                    .expect("H2 peer send");
                Peer::new(&mut socket, &mut client)
                    .try_flush()
                    .expect("H2 peer flush");
                let (_, response) = Peer::new(&mut socket, &mut client)
                    .try_response(next)
                    .expect("H2 peer response");
                assert_eq!(response, b"large");
            },
            |result| {
                result.expect("H2 setup");
            },
        )
        .expect("H2 pending reset runtime");
}
