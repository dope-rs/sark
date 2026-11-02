pub mod common;

use std::{
    cell::Cell,
    io::{ErrorKind, Read, Write},
    net::{self, TcpStream},
    str,
    time::Duration,
};

use common::server::recover;
use common::{
    route::{
        body::length::ClOverflowDispatch,
        glue::{
            guard::{Event, GuardDispatch, State},
            hello::HelloDispatch,
            hygiene::HygieneRoutes,
        },
    },
    server::{
        self, Plan,
        peer::{Endpoint as _, Receive as _, Response as _},
    },
};
use dope::core::driver::life::recovery::Policy;
use dope_test::harness::server::Pair;
use sark_fiber::Config;

#[test]
fn handler_return_scopes() {
    let harness = Pair::bind().expect("bind isolated listener");
    let server = server::Plan::new(harness.addr(), Duration::from_secs(10)).build();
    harness
        .check(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let result = HygieneRoutes::serve(
                    server.with_bound(listener),
                    context,
                    (),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("server")
                    },
                );
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                result
            },
            |bind| {
                for (path, expected) in [("/standard", "ok"), ("/early", "early")] {
                    let raw = if path == "/standard" {
                        bind.try_request(&[
                            b"GET /standard HTTP/1.1\r\nHost: example\r\nConnection: close\r\nX-Token-A: a\r\nX-Token-Abc: abc\r\nX-Token-Abcdefg: abcdefg\r\n\r\n",
                        ])
                    } else {
                        bind.try_get(path)
                    }
                    .expect("handler response");
                    let text = str::from_utf8(&raw).expect("UTF-8 response");
                    assert!(text.starts_with("HTTP/1.1 200 "), "{path}: {text:?}");
                    assert!(text.contains("\r\nx-hygiene: kept\r\n"), "{path}: {text:?}");
                    let (_, body) = text.split_once("\r\n\r\n").expect("response body");
                    assert_eq!(body, expected, "{path}");
                }
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
fn rejects_bad_authority() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .check(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let result = HelloDispatch::serve(server.with_bound(listener), context,
                    (),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(
                        |event| reports.push(event),
                        recover,
                    ),
                    |result| {
                        result.as_ref().map(|_| ()).map_err(|error| error.cause()).expect("server")
                    },
                );
                assert!(reports.events().expect("reclaimed resources").is_empty(), "server retirement: {reports:?}");
                result
            },
            |bind| {
                for (name, wire) in [
                    ("missing", b"GET /hello HTTP/1.1\r\nConnection: close\r\n\r\n".as_slice()),
                    ("empty", b"GET /hello HTTP/1.1\r\nHost:\r\nConnection: close\r\n\r\n"),
                    ("duplicate", b"GET /hello HTTP/1.1\r\nHost: one\r\nHost: two\r\nConnection: close\r\n\r\n"),
                    ("invalid", b"GET /hello HTTP/1.1\r\nHost: user@origin\r\nConnection: close\r\n\r\n"),
                ] {
                    let response = bind.try_request(&[wire]).expect("rejected authority response");
                    assert!(
                        response.starts_with(b"HTTP/1.1 400 "),
                        "{name}: {:?}",
                        String::from_utf8_lossy(&response)
                    );
                }

                let response = bind.try_request(
                    &[
                        b"GET /hello HTTP/1.1\r\nHo",
                        b"st: origin.example\r\nConnec",
                        b"tion: close\r\n\r\n",
                    ],
                ).expect("segmented authority response");
                assert!(response.starts_with(b"HTTP/1.1 200 "));
            },
            |result| {
                assert!(result.is_ok(), "server preparation failed: {:?}", result.err());
            },
        )
        .expect("harness");
}

fn guarded_get(addr: net::SocketAddr, path: &str) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("set timeout");
    let line = format!("GET {path} HTTP/1.1\r\nHost:");
    stream
        .write_all(line.as_bytes())
        .expect("send partial head");
    assert!(matches!(
        stream.read(&mut [0]),
        Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
    ));
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set response timeout");
    let fields = format!(" {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(fields.as_bytes()).expect("complete head");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read guarded response");
    buf
}

#[test]
fn guard_integration() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .check(
            |context, listener| {
                let events = Cell::new(Vec::new());
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let result = GuardDispatch::serve(
                    server.with_bound(listener),
                    context,
                    State(&events),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("server")
                    },
                );
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                assert_eq!(
                    events.into_inner(),
                    [
                        Event::Access(Some(b"/log-pass".to_vec())),
                        Event::LogPass,
                        Event::Access(Some(b"/nested/inner/x".to_vec())),
                        Event::Open,
                    ],
                    "guard scope and handler order",
                );
                result
            },
            |bind| {
                let resp = guarded_get(bind, "/log-pass");
                assert_eq!(resp.status_line(), b"HTTP/1.1 200 OK", "log-pass status");

                let resp = guarded_get(bind, "/protected/secret");
                assert_eq!(
                    resp.status_line(),
                    b"HTTP/1.1 401 Unauthorized",
                    "secret status"
                );

                let resp = guarded_get(bind, "/nested/inner/x");
                assert_eq!(
                    resp.status_line(),
                    b"HTTP/1.1 401 Unauthorized",
                    "nested status"
                );

                let resp = guarded_get(bind, "/open");
                assert_eq!(resp.status_line(), b"HTTP/1.1 200 OK", "open status");

                let resp = guarded_get(bind, "/nonexistent");
                assert_eq!(resp.status_line(), b"HTTP/1.1 404 Not Found", "404 status");
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "server preparation failed: {:?}",
                    result.err()
                )
            },
        )
        .expect("harness");
}

#[test]
fn survives_bad_lengths() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .check(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let result = ClOverflowDispatch::serve(server.with_bound(listener), context,
                    (),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(
                        |event| reports.push(event),
                        recover,
                    ),
                    |result| {
                        result.as_ref().map(|_| ()).map_err(|error| error.cause()).expect("server")
                    },
                );
                assert!(reports.events().expect("reclaimed resources").is_empty(), "server retirement: {reports:?}");
                result
            },
            |bind| {
                let lengths: &[&str] = &[
                    "4294967295",
                    "99999999999999999999999999999999999999",
                    "12abc",
                    "18446744073709551615",
                    "-5",
                    " ",
                ];

                for clen in lengths {
                    let mut sock = TcpStream::connect(bind).expect("connect");
                    let req =
                        format!("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: {clen}\r\n\r\n");
                    sock.write_all(req.as_bytes())
                        .expect("write invalid Content-Length request head");
                    let resp = sock.try_head().expect("invalid length response");
                    if !resp.is_empty() {
                        let text = String::from_utf8_lossy(&resp);
                        assert!(
                            text.starts_with("HTTP/1.1 4"),
                            "bad Content-Length {clen:?} produced non-4xx response: {text}"
                        );
                    }
                }

                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set health-check response deadline");
                sock.write_all(b"GET /ping HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .expect("write health-check request");
                let mut last = String::new();
                sock.read_to_string(&mut last)
                    .expect("read health-check response");
                assert!(last.contains("200 OK"), "server died after bad CL: {last}");
                assert!(last.contains("pong"), "server died after bad CL: {last}");

                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set valid POST response deadline");
                let body = b"hello-body";
                let req = format!(
                "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
                sock.write_all(req.as_bytes())
                    .expect("write valid POST request head");
                sock.write_all(body).expect("write valid POST request body");
                let mut resp = String::new();
                sock.read_to_string(&mut resp)
                    .expect("read valid POST response");
                assert!(resp.contains("200 "), "well-formed POST failed: {resp}");
                assert!(resp.contains("echoed"), "echo handler not reached: {resp}");
            },
            |result| {
                assert!(result.is_ok(), "survives_bad_lengths server shutdown: {:?}", result.err());
            },
        )
        .expect("harness");
}
