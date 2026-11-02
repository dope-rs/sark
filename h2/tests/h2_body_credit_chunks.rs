use sark_h2::conn::DataOutcome;

pub mod common;

use std::{convert::Infallible, time::Duration};

use common::session::{Connect, Serve};
use dope_test::harness::server::Pair;
use sark_core::manifold::stream::Boundary;
use sark_h2::{ClientRole, conn::machine::Connection, frame::numbers::ErrorCode};

use crate::common::body::{Configure, Echo, LIMITS, PROBE, Peer, headers};

#[test]
fn rejects_second_owner() {
    let mut server = LIMITS;
    server.max_request_body_chunks = 1;
    server.max_connection_body_chunks = 2;
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
            client.try_flush(&mut socket).expect("H2 peer flush");
            client.try_ping(&mut socket).expect("H2 peer progress");

            client
                .try_send_all(stream, b"b", true)
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
fn reset_returns_credit() {
    let mut server = LIMITS;
    server.max_request_body_chunks = 2;
    server.max_connection_body_chunks = 1;
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
            client.try_flush(&mut socket).expect("H2 peer flush");
            client.try_ping(&mut socket).expect("H2 peer progress");

            let key = client
                .status()
                .stream_key(old)
                .expect("active request stream");
            client
                .streams()
                .try_cancel(key, ErrorCode::Cancel)
                .expect("reset retained request");
            client.control().ping(PROBE).expect("queue reset probe");
            client.try_flush(&mut socket).expect("H2 peer flush");
            client.try_ping(&mut socket).expect("H2 peer progress");

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
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert_eq!(
                client
                    .try_read(&mut socket, next)
                    .expect("H2 response body"),
                b"b"
            );
        },
        |result| result.expect("H2 server runtime"),
    )
    .expect("body-credit E2E");
}

#[test]
fn rejects_atomically() {
    let mut server = LIMITS;
    server.max_request_body_chunks = 2;
    server.max_connection_body_chunks = 1;
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
            client.try_flush(&mut socket).expect("H2 peer flush");
            client.try_ping(&mut socket).expect("H2 peer progress");

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
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert_eq!(
                client
                    .try_reset(&mut socket, rejected)
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
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert_eq!(
                client
                    .try_read(&mut socket, owner)
                    .expect("H2 response body"),
                b"a"
            );
        },
        |result| result.expect("H2 server runtime"),
    )
    .expect("body-credit E2E");
}

#[test]
fn empty_fin_uses_no_credit() {
    let mut server = LIMITS;
    server.max_request_body_chunks = 0;
    server.max_connection_body_chunks = 0;
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
            client.try_flush(&mut socket).expect("H2 peer flush");
            assert!(
                client
                    .try_read(&mut socket, stream)
                    .expect("H2 response body")
                    .is_empty()
            );
        },
        |result| result.expect("H2 server runtime"),
    )
    .expect("body-credit E2E");
}
