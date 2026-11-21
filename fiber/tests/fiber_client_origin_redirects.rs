pub mod common;

use std::str;

use common::client::{
    config::FLAVORS,
    routes::*,
    runtime::{
        cases::{Flavor, request::Request},
        peer,
    },
};
use dope_test::harness::{
    GUARD,
    peer::TcpAcceptor,
    scenario::run::{self, Task as _},
};
use sark_client::{Error, RequestError, Safety, protocol::redirect};
use sark_fiber::client::config::Config;
use sark_http::{method::Known, payload::response::status};

const TO_B: &[u8] =
    b"HTTP/1.1 302 Found\r\nLocation: /b\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
const KEEP_OK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok";
const MISSING: &[u8] =
    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";

#[test]
fn follows_same_origin_redirects() {
    let cases: &[(&[(&str, &[u8])], &[u8])] = &[
        (&[
            ("/start", b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"),
            ("/final", b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: keep-alive\r\n\r\narrived"),
        ], b"arrived"),
        (&[
            ("/a", TO_B),
            ("/b", b"HTTP/1.1 301 Moved Permanently\r\nLocation: /c\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"),
            ("/c", b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\ndone"),
        ], b"done"),
        (&[
            ("/dir/start", b"HTTP/1.1 302 Found\r\nLocation: next\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"),
            ("/dir/next", KEEP_OK),
        ], b"ok"),
        (&[
            ("/item?step=1", b"HTTP/1.1 302 Found\r\nLocation: ?step=2\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"),
            ("/item?step=2", KEEP_OK),
        ], b"ok"),
    ];
    for &(routes, expected) in cases {
        let path = routes[0].0;
        peer::with_get(routes, MISSING, |addr| {
            for flavor in FLAVORS {
                let response = flavor.get(addr, path).collect();
                assert_eq!(response.status(), status::Code::OK, "{path}");
                assert_eq!(response.body().as_slice(), expected, "{path}");
            }
        });
    }
}

#[test]
fn redirect_loop_rejected() {
    let to_a = b"HTTP/1.1 302 Found\r\nLocation: /a\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n";
    peer::with_get(&[("/a", TO_B), ("/b", to_a)], MISSING, |addr| {
        for flavor in FLAVORS {
            flavor
                .get(addr, "/a")
                .try_run(|outcome| {
                    let failure = outcome.err().expect("redirect loop must be rejected");
                    assert!(failure.safety() == Safety::PossiblyCommitted);
                    assert!(matches!(
                        failure.cause(),
                        Error::Redirect(redirect::Error::Loop)
                    ));
                })
                .expect("HTTP request execution");
        }
    });
}

#[test]
fn rejects_cross_origin() {
    peer::with_get(&[("/start", CROSS_ORIGIN)], MISSING, |addr| {
        Flavor::Fixed
            .get(addr, "/start")
            .try_run(|outcome| {
                let failure = outcome
                    .err()
                    .expect("cross-origin redirect must be rejected");
                assert!(failure.safety() == Safety::PossiblyCommitted);
                assert!(matches!(
                    failure.cause(),
                    Error::Request(RequestError::CrossOrigin)
                ));
            })
            .expect("HTTP request execution");
    });
}

#[test]
fn accepts_terminal_304() {
    for flavor in FLAVORS {
        let mut acceptor =
            TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("terminal listener");
        let addr = acceptor.local_addr().expect("listener address");
        let (served, ()) = run::Entry(move || {
            let response = flavor
                .get(addr, "/start")
                .collect();
            assert_eq!(response.status(), status::Code::NOT_MODIFIED);
        })
        .with(|| {
            let accepted = acceptor.accept_for(GUARD)?.expect("terminal connection");
            accepted.read_wait(Some(GUARD))?;
            accepted.write_wait(Some(GUARD))?;
            accepted.set_nodelay(true)?;
            let mut conn = peer::Conn::new(&accepted);
            let request = conn.try_request()?.expect("terminal request");
            assert_eq!(request.target(), "/start");
            conn.write_all(TERMINAL)?;
            peer::closed(&mut conn)
        })
        .expect("client worker");
        served.expect("terminal connection closes");
        peer::empty(&mut acceptor).expect("304 must not trigger another request");
    }
}

#[test]
fn preserves_method_policy() {
    let headers = [
        ("cOnTeNt-EnCoDiNg", "gzip"),
        ("Content-Language", "en"),
        ("Content-Location", "/resource"),
        ("Content-Type", "application/octet-stream"),
        ("Digest", "sha-256=original"),
        ("Last-Modified", "Wed, 28 Oct 2026 12:00:00 GMT"),
        ("Content-Digest", "sha-256=:original:"),
        ("Repr-Digest", "sha-256=:original:"),
        ("Content-Range", "bytes 0-6/7"),
        ("X-Metadata", "retained"),
    ];
    for flavor in FLAVORS {
        for (status, initial, expected, retain) in [
            (302, Known::Post, "GET", false),
            (307, Known::Post, "POST", true),
            (307, Known::Get, "GET", true),
        ] {
            let mut acceptor =
                TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("policy listener");
            let addr = acceptor.local_addr().expect("listener address");
            let (served, ()) = run::Entry(move || {
                let response = Request::new(
                    flavor,
                    addr,
                    Config::new(addr.to_string()),
                    initial.into(),
                    "/start",
                    &headers,
                    b"payload",
                )
                .collect();
                assert_eq!(response.status(), status::Code::OK);
            })
            .with(|| {
                let first_reply = format!("HTTP/1.1 {status} Redirect\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n");
                let mut count = 0;
                loop {
                    let accepted = acceptor.accept_for(GUARD)?.expect("policy connection");
                    accepted.read_wait(Some(GUARD))?;
                    accepted.write_wait(Some(GUARD))?;
                    accepted.set_nodelay(true)?;
                    let mut conn = peer::Conn::new(&accepted);
                    while let Some(request) = conn.try_request()? {
                        let wire = str::from_utf8(request.wire()).expect("ASCII request");
                        for (name, _) in headers {
                            assert_eq!(
                                wire.contains(&format!("\r\n{name}: ")),
                                count == 0 || retain || name == "X-Metadata",
                                "redirect content policy for {name}",
                            );
                        }
                        let first = count == 0;
                        let method = if first { initial.as_str() } else { expected };
                        assert_eq!(request.method(), method);
                        assert_eq!(request.body(), if first || retain { b"payload".as_slice() } else { &[] });
                        let (target, reply) = match count {
                            0 => ("/start", first_reply.as_bytes()),
                            1 if !retain => ("/final", b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /after\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".as_slice()),
                            1 => ("/final", OK),
                            _ => ("/after", OK),
                        };
                        assert_eq!(request.target(), target);
                        conn.write_all(reply)?;
                        count += 1;
                        if count == if retain { 2 } else { 3 } {
                            return peer::closed(&mut conn);
                        }
                    }
                }
            })
            .expect("client worker");
            served.expect("redirect policy must finish");
            peer::empty(&mut acceptor).expect("no extra redirect requests");
        }
    }
}
