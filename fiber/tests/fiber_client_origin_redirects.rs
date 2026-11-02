use dope::core::{driver::life::recovery, io::Reclaimed};
pub mod common;

use std::{io::Write, str};

use common::client::recover;
use common::client::{
    config::FLAVORS,
    routes::*,
    runtime::{
        cases::{Flavor, request::Request},
        peer,
    },
};
use dope::core::platform::wake;
use dope_test::harness::{GUARD, peer::AcceptScript};
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
                    .try_request(&server.addr().to_string(), Method::GET, "/start", &[])
                    .expect("HTTP host allocation")
                    .try_run(
                        |result| result.expect("HTTP response"),
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("request scope");
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
                    .try_request(&server.addr().to_string(), Method::GET, "/a", &[])
                    .expect("HTTP host allocation")
                    .try_run(
                        |result| result.expect("HTTP response"),
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("request scope");
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
                    .try_request(&server.addr().to_string(), Method::GET, "/dir/start", &[])
                    .expect("HTTP host allocation")
                    .try_run(
                        |result| result.expect("HTTP response"),
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("request scope");
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
                    .try_request(&server.addr().to_string(), Method::GET, "/item?step=1", &[])
                    .expect("HTTP host allocation")
                    .try_run(
                        |result| result.expect("HTTP response"),
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("request scope");
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
                Request::new(
                    flavor,
                    &server.addr().to_string(),
                    Config::new(server.addr().to_string()),
                    Method::GET,
                    "/a",
                    &[],
                    &[],
                )
                .try_run(
                    |outcome| {
                        let failure = (outcome).err().expect("redirect loop must be rejected");
                        assert!((failure.safety()) == (Safety::PossiblyCommitted));
                        assert!(matches!(
                            failure.cause(),
                            Error::Redirect(redirect::Error::Loop)
                        ));
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
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
            Request::new(
                Flavor::Fixed,
                &server.addr().to_string(),
                Config::new(server.addr().to_string()),
                Method::GET,
                "/start",
                &[],
                &[],
            )
            .try_run(
                |outcome| {
                    let failure = (outcome)
                        .err()
                        .expect("cross-origin redirect must be rejected");
                    assert!((failure.safety()) == (Safety::PossiblyCommitted));
                    assert!(matches!(
                        failure.cause(),
                        Error::Request(RequestError::CrossOrigin)
                    ));
                },
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
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
        let (quiesced, release) = wake::Ends::blocking()
            .expect("terminal observation gate")
            .split();
        AcceptScript::with(
            move |acceptor| {
                let accepted = acceptor.accept()?;
                let mut conn = peer::Conn::new(&accepted);
                let request = conn.try_request()?.expect("terminal request");
                assert_eq!(request.target(), "/start");
                conn.write_all(TERMINAL)?;
                quiesced
                    .wait_timeout(GUARD)
                    .expect("terminal driver quiesces");
                peer::closed(&mut conn)?;
                peer::empty(acceptor)
            },
            |server| {
                let response = flavor
                    .try_request(&server.addr().to_string(), Method::GET, "/start", &[])
                    .expect("HTTP host allocation")
                    .try_run(
                        |result| result.expect("HTTP response"),
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("request scope");
                assert_eq!(response.status(), status::Code::NOT_MODIFIED);
                release.notify().expect("terminal observation release");
                server
                    .finish()
                    .expect("peer thread")
                    .expect("304 must not trigger another request");
            },
        )
        .expect("spawn redirect peer");
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
            let (quiesced, release) = wake::Ends::blocking()
                .expect("policy observation gate")
                .split();
            AcceptScript::with(
                move |acceptor| {
                    let mut observed = Vec::new();
                    loop {
                        let accepted = acceptor.accept()?;
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
                                quiesced
                                    .wait_timeout(GUARD)
                                    .expect("policy driver quiesces");
                                peer::closed(&mut conn)?;
                                peer::empty(acceptor)?;
                                return Ok::<_, peer::Error>(observed);
                            }
                        }
                    }
                },
                |server| {
                    let addr = server.addr().to_string();
                    let response = Request::new(
                        flavor,
                        &addr,
                        Config::try_new(&addr).expect("HTTP host allocation"),
                        initial,
                        "/start",
                        &headers,
                        b"payload",
                    )
                    .try_run(
                        |result| result.expect("HTTP response"),
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("request scope");
                    assert_eq!(response.status(), status::Code::OK);
                    release.notify().expect("policy observation release");
                    let observed = server
                        .finish()
                        .expect("peer thread")
                        .expect("redirect policy must finish");
                    assert_eq!(observed[0].method(), if initial_get { "GET" } else { "POST" });
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
                },
            )
            .expect("spawn redirect peer");
        }
    }
}
