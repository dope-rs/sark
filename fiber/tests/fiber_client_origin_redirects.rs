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
    peer::{AcceptScript, TcpAcceptor},
    scenario::run::{self, Task as _},
};
use sark_client::{Error, RequestError, Safety, protocol::redirect};
use sark_fiber::client::config::Config;
use sark_http::{method::Method, payload::response::status};

#[test]
fn follows_origin() {
    AcceptScript::with(
        |acceptor| peer::serve(acceptor, peer::Request::origin),
        |server| {
            for flavor in FLAVORS {
                let resp = flavor
                    .get(server.addr(), "/start")
                    .collect();
                assert_eq!(resp.status().as_u16(), 200);
                assert_eq!(
                    str::from_utf8(resp.body().as_slice()).expect("response body is UTF-8"),
                    "arrived"
                );
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop redirect peer");
        },
    )
    .expect("spawn redirect peer");
}

#[test]
fn redirect_chain_followed() {
    AcceptScript::with(
        |acceptor| peer::serve(acceptor, peer::Request::chain),
        |server| {
            for flavor in FLAVORS {
                let resp = flavor
                    .get(server.addr(), "/a")
                    .collect();
                assert_eq!(resp.status().as_u16(), 200);
                assert_eq!(
                    str::from_utf8(resp.body().as_slice()).expect("response body is UTF-8"),
                    "done"
                );
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop redirect peer");
        },
    )
    .expect("spawn redirect peer");
}

#[test]
fn resolves_relative() {
    AcceptScript::with(
        |acceptor| peer::serve(acceptor, peer::Request::relative),
        |server| {
            for flavor in FLAVORS {
                let response = flavor
                    .get(server.addr(), "/dir/start")
                    .collect();
                assert_eq!(response.status().as_u16(), 200);
                assert_eq!(response.body().as_slice(), b"ok");
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop redirect peer");
        },
    )
    .expect("spawn redirect peer");
}

#[test]
fn keeps_query_path() {
    AcceptScript::with(
        |acceptor| peer::serve(acceptor, peer::Request::query_path),
        |server| {
            for flavor in FLAVORS {
                let response = flavor
                    .get(server.addr(), "/item?step=1")
                    .collect();
                assert_eq!(response.status().as_u16(), 200);
                assert_eq!(response.body().as_slice(), b"ok");
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop redirect peer");
        },
    )
    .expect("spawn redirect peer");
}

#[test]
fn redirect_loop_rejected() {
    AcceptScript::with(
        |acceptor| peer::serve(acceptor, peer::Request::looping),
        |server| {
            for flavor in FLAVORS {
                Request::get(
                    flavor,
                    server.addr(),
                    Config::new(server.addr().to_string()),
                    "/a",
                )
                .try_run(|outcome| {
                    let failure = (outcome).err().expect("redirect loop must be rejected");
                    assert!((failure.safety()) == (Safety::PossiblyCommitted));
                    assert!(matches!(
                        failure.cause(),
                        Error::Redirect(redirect::Error::Loop)
                    ));
                })
                .expect("HTTP request execution");
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop redirect peer");
        },
    )
    .expect("spawn redirect peer");
}

#[test]
fn rejects_cross_origin() {
    AcceptScript::with(
        |acceptor| peer::serve(acceptor, |_: peer::Request<'_>| CROSS_ORIGIN.to_vec()),
        |server| {
            Request::get(
                Flavor::Fixed,
                server.addr(),
                Config::new(server.addr().to_string()),
                "/start",
            )
            .try_run(|outcome| {
                let failure = (outcome)
                    .err()
                    .expect("cross-origin redirect must be rejected");
                assert!((failure.safety()) == (Safety::PossiblyCommitted));
                assert!(matches!(
                    failure.cause(),
                    Error::Request(RequestError::CrossOrigin)
                ));
            })
            .expect("HTTP request execution");
            server
                .stop()
                .expect("peer thread")
                .expect("stop redirect peer");
        },
    )
    .expect("spawn redirect peer");
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
            (302, Method::POST, "GET", false),
            (307, Method::POST, "POST", true),
            (307, Method::GET, "GET", true),
        ] {
            let initial_get = initial == Method::GET;
            let mut acceptor =
                TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("policy listener");
            let addr = acceptor.local_addr().expect("listener address");
            let (observed, ()) = run::Entry(move || {
                let response = Request::new(
                    flavor,
                    addr,
                    Config::new(addr.to_string()),
                    initial,
                    "/start",
                    &headers,
                    b"payload",
                )
                .collect();
                assert_eq!(response.status(), status::Code::OK);
            })
            .with(|| {
                let mut observed = Vec::new();
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
                                observed.is_empty() || retain || name == "X-Metadata",
                                "redirect content policy for {name}",
                            );
                        }
                        let response = if !retain && request.target() == "/final" {
                            b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /after\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".to_vec()
                        } else {
                            request.policy_reply(status)
                        };
                        observed.push(Seen::from(request));
                        conn.write_all(&response)?;
                        if observed.len() == if retain { 2 } else { 3 } {
                            peer::closed(&mut conn)?;
                            return Ok::<_, peer::Error>(observed);
                        }
                    }
                }
            })
            .expect("client worker");
            let observed = observed.expect("redirect policy must finish");
            peer::empty(&mut acceptor).expect("no extra redirect requests");
            assert_eq!(
                observed[0].method(),
                if initial_get { "GET" } else { "POST" }
            );
            assert_eq!(observed[0].target(), "/start");
            assert_eq!(observed[0].body(), b"payload");
            assert_eq!(observed[1].target(), "/final");
            for request in observed.iter().skip(1) {
                assert_eq!(request.method(), expected);
                assert_eq!(
                    request.body(),
                    if retain { &b"payload"[..] } else { &[][..] }
                );
            }
        }
    }
}
