pub mod common;

use std::{cell::Cell, io::Write, net::TcpStream, thread, time::Duration};

use common::server::recover;
use common::{
    retained::{Echo, EchoDispatch, NESTED, Post, State, wire},
    route::{body::retained, chunked::peer::Peer},
    server::{self, Plan},
};
use dope::core::driver::life::recovery::Policy;
use dope_test::harness::server::Pair;
use sark_fiber::Config;

#[test]
fn pipeline_keeps_bytes() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness.check(
        |context, listener| {
            let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
            let failures = Cell::new(Vec::new());
            let result = retained::EchoDispatch::serve(server.with_bound(listener), context,
                State(&failures),
                Config::new(server::MAX_CONNECTIONS),
                Policy::new(|event| reports.push(event), recover),
                |result| result.as_ref().map(|_| ()).map_err(|error| error.cause()).expect("server runtime"),
            );
            assert!(reports.events().expect("reclaimed resources").is_empty(), "server retirement: {reports:?}");
            let failures = failures.into_inner();
            assert!(failures.is_empty(), "retention: {failures:?}");
            result
        },
        |bind| {
            let mut sock = TcpStream::connect(bind).expect("connect");
            sock.set_read_timeout(Some(Duration::from_secs(3)))
                .expect("test socket accepts a read timeout");
            sock.set_nodelay(true)
                .expect("test socket accepts TCP_NODELAY");

            let req1 = b"GET http://origin.example/users/USER-REQ1/teams/TEAM-REQ1?q=kept HTTP/1.1\r\nHost: ignored.example\r\nx-echo-marker: MARKER-REQ1-VALUE\r\n\r\n";
            let req2 = b"GET /users/USER-REQ2/teams/TEAM-REQ2 HTTP/1.1\r\nHost: yyyyyyyyyyyyyyyyyyyyyyyy\r\nx-echo-marker: ZZZZZZZZZZZZZZZZZZZZZZZZ\r\n\r\n";
            let mut batch = Vec::new();
            batch.extend_from_slice(req1);
            batch.extend_from_slice(req2);
            sock.write_all(&batch)
                .expect("connected test socket accepts both pipelined requests");

            let mut acc = Vec::new();
            let resp1 = Peer::new(&mut sock, &mut acc).try_text().expect("HTTP response");
            let resp2 = Peer::new(&mut sock, &mut acc).try_text().expect("HTTP response");

            assert_eq!(resp1.0, 200, "resp1 body: {:?}", resp1.1);
            assert_eq!(
                resp1.1,
                "team=TEAM-REQ1 user=USER-REQ1 marker=MARKER-REQ1-VALUE authority=origin.example",
                "first response did not echo the original request bytes (use-after-free?)"
            );
            assert_eq!(resp2.0, 200, "resp2 body: {:?}", resp2.1);
            assert_eq!(
                resp2.1,
                "team=TEAM-REQ2 user=USER-REQ2 marker=ZZZZZZZZZZZZZZZZZZZZZZZZ authority=yyyyyyyyyyyyyyyyyyyyyyyy"
            );
        },
        |result| {
            assert!(result.is_ok(), "owned request server shutdown: {:?}", result.err());
        },
    ).expect("harness");
}

#[test]
fn accum_retain() {
    let harness = Pair::bind().expect("reserve listener address");
    let bind = harness.addr();
    harness
        .check(
            |context, listener| {
                let server = server::Plan::new(bind, Duration::from_secs(10)).build();
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let failures = Cell::new(Vec::new());
                let result = EchoDispatch::serve(
                    server.with_bound(listener),
                    context,
                    State(&failures),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("server runtime")
                    },
                );
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                let failures = failures.into_inner();
                assert!(failures.is_empty(), "retention: {failures:?}");
                result
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("set bounded response timeout");
                sock.set_nodelay(true)
                    .expect("disable Nagle for request split");

                let r1 = Echo::new("REQ1ID", "MARKER-ONE", "BODY-REQ1-AAAA").wire();
                let head_end = r1
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                    .expect("generated request contains a header terminator")
                    + 4;
                let r2 = Echo::new("REQ2ID", "MARKER-TWO", "BODY-REQ2-BBBB").wire();

                sock.write_all(&r1[..head_end])
                    .expect("write first request head");
                thread::sleep(Duration::from_millis(15));
                sock.write_all(&r1[head_end..])
                    .expect("write first request body");
                thread::sleep(Duration::from_millis(15));
                sock.write_all(&r2).expect("write second request");

                let mut acc = Vec::new();
                let (s1, b1) = Peer::new(&mut sock, &mut acc)
                    .try_text()
                    .expect("HTTP response");
                let (s2, b2) = Peer::new(&mut sock, &mut acc)
                    .try_text()
                    .expect("HTTP response");

                assert_eq!(s1, 200, "resp1: {b1:?}");
                assert_eq!(
                    b1,
                    Echo::new("REQ1ID", "MARKER-ONE", "BODY-REQ1-AAAA").expected(),
                    "req1 head/body corrupted by accum mutation during await (COW failed?)"
                );
                assert_eq!(s2, 200, "resp2: {b2:?}");
                assert_eq!(
                    b2,
                    Echo::new("REQ2ID", "MARKER-TWO", "BODY-REQ2-BBBB").expected()
                );
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "server preparation failed: {:?}",
                    result.err()
                );
            },
        )
        .expect("harness");
}

#[test]
fn socket_retain() {
    let harness = Pair::bind().expect("reserve listener address");
    let bind = harness.addr();
    harness
        .check(
            |context, listener| {
                let server = server::Plan::new(bind, Duration::from_secs(10)).build();
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let failures = Cell::new(Vec::new());
                let result = EchoDispatch::serve(
                    server.with_bound(listener),
                    context,
                    State(&failures),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("server runtime")
                    },
                );
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                let failures = failures.into_inner();
                assert!(failures.is_empty(), "retention: {failures:?}");
                result
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("set bounded response timeout");
                sock.set_nodelay(true)
                    .expect("disable Nagle for pipelining");

                let r1 = Echo::new("REQ1ID", "MARKER-ONE", "BODY-REQ1-AAAA").wire();
                let r2 = Echo::new("REQ2ID", "MARKER-TWO", "BODY-REQ2-BBBB").wire();

                sock.write_all(&r1).expect("write first request");
                thread::sleep(Duration::from_millis(15));
                sock.write_all(&r2).expect("write second request");

                let mut acc = Vec::new();
                let (s1, b1) = Peer::new(&mut sock, &mut acc)
                    .try_text()
                    .expect("HTTP response");
                let (s2, b2) = Peer::new(&mut sock, &mut acc)
                    .try_text()
                    .expect("HTTP response");

                assert_eq!(s1, 200, "resp1: {b1:?}");
                assert_eq!(
                    b1,
                    Echo::new("REQ1ID", "MARKER-ONE", "BODY-REQ1-AAAA").expected(),
                    "req1 corrupted after socket buffer reuse (retain copy failed?)"
                );
                assert_eq!(s2, 200, "resp2: {b2:?}");
                assert_eq!(
                    b2,
                    Echo::new("REQ2ID", "MARKER-TWO", "BODY-REQ2-BBBB").expected()
                );
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "server preparation failed: {:?}",
                    result.err()
                );
            },
        )
        .expect("harness");
}

#[test]
fn body_transfer() {
    let harness = Pair::bind().expect("reserve listener address");
    let bind = harness.addr();
    harness
        .check(
            |context, listener| {
                let server = server::Plan::new(bind, Duration::from_secs(10)).build();
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let failures = Cell::new(Vec::new());
                let result = EchoDispatch::serve(
                    server.with_bound(listener),
                    context,
                    State(&failures),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("server runtime")
                    },
                );
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                let failures = failures.into_inner();
                assert!(failures.is_empty(), "retention: {failures:?}");
                result
            },
            |bind| {
                let payload = vec![b'x'; 16 * 1024];
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("set bounded response timeout");
                sock.set_nodelay(true).expect("disable Nagle for request");
                sock.write_all(&wire(&payload))
                    .expect("write retained body request");

                let mut acc = Vec::new();
                let (status, body) = Peer::new(&mut sock, &mut acc)
                    .try_text()
                    .expect("HTTP response");
                assert_eq!(status, 200);
                assert_eq!(body.as_bytes(), payload);
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "server preparation failed: {:?}",
                    result.err()
                );
            },
        )
        .expect("harness");
}

#[test]
fn listener_admission() {
    const TASKS: usize = 1;
    const { assert!(TASKS < server::MAX_CONNECTIONS) };
    let harness = Pair::bind().expect("reserve listener address");
    let bind = harness.addr();
    harness
        .check(
            |context, listener| {
                let server = server::Plan::new(bind, Duration::from_secs(10)).build();
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
            let failures = Cell::new(Vec::new());
                let result = EchoDispatch::serve(server.with_bound(listener), context,
                    State(&failures),
                    Config::new(TASKS),
                    Policy::new(|event| reports.push(event), recover),
                    |result| result.as_ref().map(|_| ()).map_err(|error| error.cause()).expect("server runtime"),
                );
                assert!(reports.events().expect("reclaimed resources").is_empty(), "server retirement: {reports:?}");
                let failures = failures.into_inner();
                assert!(failures.is_empty(), "retention: {failures:?}");
            result
            },
            |bind| {
                let head = b"POST /retained/id?q=query HTTP/1.1\r\nHost: x\r\nx-echo-marker: marker\r\nContent-Length: 2\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n";
                let mut sockets = Vec::with_capacity(server::MAX_CONNECTIONS);
                for _ in 0..server::MAX_CONNECTIONS {
                    let mut socket = TcpStream::connect(bind).expect("connect");
                    socket
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .expect("set bounded continuation timeout");
                    socket
                        .set_nodelay(true)
                        .expect("disable Nagle for continuation");
                    socket.write_all(head).expect("write partial body");
                    let mut acc = Vec::new();
                    let (status, body) = Peer::new(&mut socket, &mut acc).try_text().expect("HTTP response");
                    assert_eq!(status, 100, "continuation was not admitted");
                    assert!(body.is_empty());
                    sockets.push((socket, acc));
                }

                assert_eq!(sockets.len(), server::MAX_CONNECTIONS);
                for (mut socket, mut acc) in sockets {
                    socket.write_all(b"xy").expect("finish body");
                    let (status, body) = Peer::new(&mut socket, &mut acc).try_text().expect("HTTP response");
                    assert_eq!(status, 200, "continuation rejected before task admission");
                    assert_eq!(body, "xy");
                }
            },
            |result| {
                assert!(result.is_ok(), "server preparation failed: {:?}", result.err());
            },
        )
        .expect("harness");
}

#[test]
fn json_transfer() {
    let harness = Pair::bind().expect("reserve listener address");
    let bind = harness.addr();
    harness
        .check(
            |context, listener| {
                let server = server::Plan::new(bind, Duration::from_secs(10)).build();
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let failures = Cell::new(Vec::new());
                let result = EchoDispatch::serve(
                    server.with_bound(listener),
                    context,
                    State(&failures),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("server runtime")
                    },
                );
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                let failures = failures.into_inner();
                assert!(failures.is_empty(), "retention: {failures:?}");
                result
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("set bounded response timeout");
                sock.set_nodelay(true).expect("disable Nagle for requests");
                sock.write_all(
                    &Post::new("/retained-json", br#"{"value":"owner-backed-json"}"#).wire(),
                )
                .expect("write retained JSON request");
                sock.write_all(&Post::new("/retained-json/nested", NESTED).wire())
                    .expect("write nested retained JSON request");

                let mut acc = Vec::new();
                let (status, body) = Peer::new(&mut sock, &mut acc)
                    .try_text()
                    .expect("HTTP response");
                assert_eq!(status, 200);
                assert_eq!(body, "owner-backed-json");
                let (status, body) = Peer::new(&mut sock, &mut acc)
                    .try_text()
                    .expect("HTTP response");
                assert_eq!(status, 200);
                assert_eq!(body, "first-owner");
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "server preparation failed: {:?}",
                    result.err()
                );
            },
        )
        .expect("harness");
}
