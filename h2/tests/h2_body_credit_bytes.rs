pub mod common;

use std::{convert::Infallible, time::Duration};

use common::session::{Connect, Serve};
use dope_test::harness::server::Pair;
use sark_core::manifold::stream::Boundary;
use sark_h2::{ClientRole, conn::machine::Connection, frame::numbers::ErrorCode};

use crate::common::body::{Configure, Echo, LIMITS, PROBE, Peer, headers};

#[test]
fn rejects_connection_bytes() {
    let mut server = LIMITS;
    server.max_request_body_bytes = 3;
    server.max_connection_body_bytes = 2;
    let pair = Pair::bind().expect("H2 body-credit harness");
    let bind = pair.addr();
    pair.check(
        move |context, listener| {
            let reports = Echo
                .try_serve(
                    server.try_plan(bind).expect("H2 server plan"),
                    context,
                    listener,
                    common::recover,
                    |result| result.expect("H2 server runtime"),
                )
                .expect("H2 server resources");
            assert!(
                reports.events().expect("H2 retirement").is_empty(),
                "{reports:?}"
            );
            Ok::<(), Infallible>(())
        },
        |address| {
            let mut socket = address
                .try_connect(Duration::from_secs(3))
                .expect("H2 peer connection");
            let mut client =
                Connection::<ClientRole>::try_new().expect("default H2 client plan is valid");
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
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert_eq!(
                client
                    .try_reset(&mut socket, stream)
                    .expect("H2 peer reset"),
                ErrorCode::EnhanceYourCalm,
            );
        },
        |result| result.expect("H2 server runtime"),
    )
    .expect("body-credit E2E");
}

#[test]
fn returns_fragment_owners() {
    let pair = Pair::bind().expect("H2 body-credit harness");
    let bind = pair.addr();
    pair.check(
        move |context, listener| {
            let reports = Echo
                .try_serve(
                    LIMITS.try_plan(bind).expect("H2 server plan"),
                    context,
                    listener,
                    common::recover,
                    |result| result.expect("H2 server runtime"),
                )
                .expect("H2 server resources");
            assert!(
                reports.events().expect("H2 retirement").is_empty(),
                "{reports:?}"
            );
            Ok::<(), Infallible>(())
        },
        |address| {
            let mut socket = address
                .try_connect(Duration::from_secs(3))
                .expect("H2 peer connection");
            let mut client =
                Connection::<ClientRole>::try_new().expect("default H2 client plan is valid");

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
            client.try_flush(&mut socket).expect("H2 peer flush");
            client.try_ping(&mut socket).expect("H2 peer progress");

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
            client.try_flush(&mut socket).expect("H2 peer flush");
            client.try_ping(&mut socket).expect("H2 peer progress");

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
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert_eq!(
                client
                    .try_read(&mut socket, other)
                    .expect("H2 response body"),
                b"ok",
                "incomplete first fragments must return all receive owners",
            );

            client
                .try_send_all(first, b"bb", true)
                .expect("H2 request DATA");
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert_eq!(
                client
                    .try_read(&mut socket, first)
                    .expect("H2 response body"),
                b"aabb"
            );

            client
                .try_send_all(second, b"dd", true)
                .expect("H2 request DATA");
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert_eq!(
                client
                    .try_read(&mut socket, second)
                    .expect("H2 response body"),
                b"ccdd"
            );
        },
        |result| result.expect("H2 server runtime"),
    )
    .expect("body-credit E2E");
}

#[test]
fn headers_reserve_no_bytes() {
    let mut server = LIMITS;
    server.max_request_body_bytes = 4;
    server.max_connection_body_bytes = 4;
    let pair = Pair::bind().expect("H2 body-credit harness");
    let bind = pair.addr();
    pair.check(
        move |context, listener| {
            let reports = Echo
                .try_serve(
                    server.try_plan(bind).expect("H2 server plan"),
                    context,
                    listener,
                    common::recover,
                    |result| result.expect("H2 server runtime"),
                )
                .expect("H2 server resources");
            assert!(
                reports.events().expect("H2 retirement").is_empty(),
                "{reports:?}"
            );
            Ok::<(), Infallible>(())
        },
        |address| {
            let mut socket = address
                .try_connect(Duration::from_secs(3))
                .expect("H2 peer connection");
            let mut client =
                Connection::<ClientRole>::try_new().expect("default H2 client plan is valid");

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
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert_eq!(
                client
                    .try_read(&mut socket, next)
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
            client.try_flush(&mut socket).expect("H2 peer flush");
        },
        |result| result.expect("H2 server runtime"),
    )
    .expect("body-credit E2E");
}

#[test]
fn releases_mismatch_budget() {
    let mut server = LIMITS;
    server.max_request_body_bytes = 4;
    server.max_connection_body_bytes = 4;
    let pair = Pair::bind().expect("H2 body-credit harness");
    let bind = pair.addr();
    pair.check(
        move |context, listener| {
            let reports = Echo
                .try_serve(
                    server.try_plan(bind).expect("H2 server plan"),
                    context,
                    listener,
                    common::recover,
                    |result| result.expect("H2 server runtime"),
                )
                .expect("H2 server resources");
            assert!(
                reports.events().expect("H2 retirement").is_empty(),
                "{reports:?}"
            );
            Ok::<(), Infallible>(())
        },
        |address| {
            let mut socket = address
                .try_connect(Duration::from_secs(3))
                .expect("H2 peer connection");
            let mut client =
                Connection::<ClientRole>::try_new().expect("default H2 client plan is valid");

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
            client.try_flush(&mut socket).expect("H2 peer flush");
            client
                .try_ping(&mut socket)
                .expect("retained input before FIN");
            client
                .try_send_all(bad, b"o", true)
                .expect("H2 request DATA");
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert_eq!(
                client.try_reset(&mut socket, bad).expect("H2 peer reset"),
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
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert_eq!(
                client
                    .try_read(&mut socket, next)
                    .expect("H2 response body"),
                b"next"
            );
        },
        |result| result.expect("H2 server runtime"),
    )
    .expect("body-credit E2E");
}
