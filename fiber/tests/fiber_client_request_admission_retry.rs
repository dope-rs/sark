use common::client::recover;
use common::client::runtime::peer;
use dope::core::{driver::life::recovery, io::Reclaimed};
use dope_test::harness::{
    GUARD,
    peer::{AcceptScript, TcpAcceptor},
    scenario::run::{self, Task as _},
};
pub mod common;

use std::{mem, net::Shutdown, str, time::Duration};

use common::client::{
    config::{Config as _, FLAVORS},
    retry::{INVALID_GZIP, MISSING_LOCATION},
    runtime::{
        calls,
        cases::{Flavor, request::Request},
        origins,
        peer::Response as _,
    },
};
use dope_fiber::BlockOn as _;
use sark::client::protocol::retry::Policy;
use sark_client::{
    Error, RequestError, Safety, capacity, connect,
    protocol::{self, redirect},
};
use sark_fiber::client::config::Config;
use sark_http::{field, h1::switch, method::Method};

#[test]
fn rejects_invalid_headers() {
    let mut acceptor = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("validation listener");
    let addr = acceptor.local_addr().expect("listener address").to_string();
    for flavor in FLAVORS {
        for (headers, expected) in [
            (
                &[("Host", "other.example")][..],
                RequestError::ReservedHeader,
            ),
            (
                &[("Transfer-Encoding", "chunked")][..],
                RequestError::ReservedHeader,
            ),
            (
                &[("bad name", "value")][..],
                RequestError::HeaderName(field::Error::InvalidByte {
                    index: 3,
                    byte: b' ',
                }),
            ),
            (
                &[("x-test", "bad\r\nvalue")][..],
                RequestError::HeaderValue(field::Error::InvalidByte {
                    index: 3,
                    byte: b'\r',
                }),
            ),
            (
                &[("x-test", "bad\0value")][..],
                RequestError::HeaderValue(field::Error::InvalidByte { index: 3, byte: 0 }),
            ),
            (
                &[("x-test", "bad\u{b}value")][..],
                RequestError::HeaderValue(field::Error::InvalidByte { index: 3, byte: 11 }),
            ),
            (
                &[("x-test", "bad\u{c}value")][..],
                RequestError::HeaderValue(field::Error::InvalidByte { index: 3, byte: 12 }),
            ),
            (
                &[("x-test", "bad\u{7f}value")][..],
                RequestError::HeaderValue(field::Error::InvalidByte {
                    index: 3,
                    byte: 127,
                }),
            ),
        ] {
            Request::new(
                flavor,
                &addr,
                Config::new(addr.clone()),
                Method::GET,
                "/",
                headers,
                &[],
            )
            .try_run(
                |outcome| {
                    let failure = (outcome)
                        .err()
                        .expect("invalid headers must fail before submission");
                    assert!((failure.safety()) == (Safety::BeforeSubmit));
                    assert!(matches!(
                        failure.cause(),
                        Error::Request(actual) if *actual == expected
                    ));
                },
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("HTTP request execution");
            peer::empty(&mut acceptor).expect("validation never submits any bytes");
        }
    }
}

#[test]
fn failed_origin_keeps_slot() {
    AcceptScript::with(
        |acceptor| {
            peer::serve_each(acceptor, |stream, _| {
                stream.write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            })
        },
        |server| {
            let config = Config::new(server.addr().to_string()).request_pool(1, 128);
            origins::Plan::new(config, 1, 1, 1)
                .expect("dynamic plan")
                .enter(
                    (),
                    |mut app| {
                        let client = app.handle;
                        let failed = app
                            .app
                            .block_on(client.send(Method::POST, "/", &calls::OVERSIZED))
                            .expect("request encoding");
                        let failed = failed.err().expect("bounded request bytes");
                        assert_eq!(failed.attempt(), 1);
                        assert!(failed.safety() == Safety::BeforeSubmit);
                        assert!(matches!(
                            failed.cause(),
                            Error::Capacity(capacity::Error::RequestBytes(_))
                        ));
                        let failed = app
                            .app
                            .block_on(client.get(calls::INVALID))
                            .expect("resolve request");
                        let failed = failed.err().expect("invalid host");
                        assert_eq!(failed.attempt(), 1);
                        assert!(matches!(
                            failed.cause(),
                            Error::Connect(connect::Error::Resolve(_))
                        ));
                        let failed = app
                            .app
                            .block_on(client.send(Method::CONNECT, calls::INVALID, &[]))
                            .expect("combined failure request");
                        assert!(matches!(
                            failed
                                .err()
                                .expect("request error precedes resolve")
                                .cause(),
                            Error::Request(RequestError::UnsupportedTransition)
                        ));
                        let failed = app
                            .app
                            .block_on(client.send(Method::CONNECT, calls::REJECTED, &[]))
                            .expect("encoding request");
                        assert!(matches!(
                            failed.err().expect("unsupported method").cause(),
                            Error::Request(RequestError::UnsupportedTransition)
                        ));
                        let reply = app
                            .app
                            .block_on(client.get("/"))
                            .expect("valid request")
                            .expect("origin capacity retained");
                        assert_eq!(reply.status().as_u16(), 204);
                    },
                    recovery::Policy::new(|_| {}, recover),
                    |result| result.expect("client execution"),
                )
                .expect("dynamic shutdown");
            server
                .stop()
                .expect("peer thread")
                .expect("origin shutdown");
        },
    )
    .expect("origin peer")
}

#[test]
fn rejects_connect() {
    for flavor in FLAVORS {
        let mut acceptor = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("CONNECT listener");
        let addr = acceptor.local_addr().expect("listener address");
        Request::new(
            flavor,
            &addr.to_string(),
            flavor.try_config(addr).expect("HTTP host allocation"),
            Method::CONNECT,
            "example.test:443",
            &[],
            &[],
        )
        .try_run(
            |outcome| {
                let failure = (outcome)
                    .err()
                    .expect("CONNECT must not reach HTTP message mode");
                assert!((failure.safety()) == (Safety::BeforeSubmit));
                assert!(
                    matches!(
                        failure.cause(),
                        Error::Request(RequestError::UnsupportedTransition)
                    ),
                    "{failure:?}"
                );
            },
            recovery::Policy::new(
                |event| std::assert_matches!(event, Reclaimed::Shutdown),
                recover,
            ),
            |result| result.expect("client execution"),
        )
        .expect("HTTP request execution");
        peer::empty(&mut acceptor).expect("CONNECT never submits any bytes");
    }
}

#[test]
fn rejects_switch() {
    for flavor in FLAVORS {
        AcceptScript::with(
            |acceptor| {
                peer::serve(acceptor, |request| {
                    assert_eq!(request.method(), "GET");
                    assert_eq!(request.target(), "/switch");
                    "HTTP/1.1 101 Switching Protocols".response(&[], &[])
                })
            },
            |peer| {
                Request::new(
                    flavor,
                    &peer.addr().to_string(),
                    flavor
                        .try_config(peer.addr())
                        .expect("HTTP host allocation"),
                    Method::GET,
                    "/switch",
                    &[],
                    &[],
                )
                .try_run(
                    |outcome| {
                        let failure = (outcome).err().expect("unsolicited 101 must be rejected");
                        assert!((failure.safety()) == (Safety::PossiblyCommitted));
                        assert!(matches!(
                            failure.cause(),
                            Error::Protocol(protocol::Error::Transition(
                                switch::Error::UnexpectedSwitch
                            ))
                        ));
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("HTTP request execution");
                peer.stop()
                    .expect("peer thread")
                    .expect("stop switching-protocol peer");
            },
        )
        .expect("spawn switching-protocol peer")
    }
}

#[test]
fn attempts() {
    for flavor in FLAVORS {
        for (reply, coding) in [(INVALID_GZIP, true), (MISSING_LOCATION, false)] {
            let mut first = true;
            AcceptScript::with(
                move |acceptor| {
                    peer::serve_each(acceptor, move |stream, _| {
                        if mem::take(&mut first) {
                            return peer::try_shutdown(stream);
                        }
                        stream.write_all(reply)
                    })
                },
                |server| {
                    Request::new(
                        flavor,
                        &server.addr().to_string(),
                        Config::new(server.addr().to_string()).timeout(Duration::from_secs(3)),
                        Method::GET,
                        "/",
                        &[],
                        &[],
                    )
                    .try_run(
                        |result| {
                            let failure = result.err().expect("postprocessing must fail");
                            assert!(if coding {
                                matches!(failure.cause(), Error::Decompression(_))
                            } else {
                                matches!(
                                    failure.cause(),
                                    Error::Redirect(redirect::Error::Location)
                                )
                            });
                            assert_eq!(failure.attempt(), 2);
                            assert_eq!(failure.safety(), Safety::PossiblyCommitted);
                        },
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("retry postprocessing execution");
                    server
                        .stop()
                        .expect("peer thread")
                        .expect("stop retry postprocessing peer");
                },
            )
            .expect("spawn retry postprocessing peer")
        }
    }
}

#[test]
fn get_retries() {
    for flavor in FLAVORS {
        let mut acceptor = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("retry listener");
        let addr = acceptor.local_addr().expect("listener address");
        let (served, ()) = run::Entry(move || {
            let addr = addr.to_string();
            let config = Config::new(addr.clone())
                .retry(Policy::default())
                .timeout(Duration::from_secs(3));
            let response = Request::new(flavor, &addr, config, Method::GET, "/effect", &[], &[])
                .try_run(
                    |result| result.expect("HTTP response"),
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("HTTP request execution");
            assert_eq!(response.status().as_u16(), 200);
        })
        .with(|| {
            for first in [true, false] {
                let stream = acceptor.accept_for(GUARD)?.expect("retry connection");
                stream.read_wait(Some(GUARD))?;
                stream.write_wait(Some(GUARD))?;
                stream.set_nodelay(true)?;
                let mut conn = peer::Conn::new(&stream);
                let request = conn.try_request()?.expect("retry request");
                assert_eq!(request.method(), "GET");
                assert_eq!(request.target(), "/effect");
                if first {
                    stream.shutdown(Shutdown::Both)?;
                    continue;
                }
                conn.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                )?;
                peer::closed(&mut conn)?;
            }
            Ok::<_, peer::Error>(())
        })
        .expect("client worker");
        served.expect("exactly two retry requests");
        peer::empty(&mut acceptor).expect("no extra retry requests");
    }
}

#[test]
fn uses_exact_authority() {
    AcceptScript::with(
        |acceptor| {
            peer::serve_each(acceptor, |stream, request| {
                let request = str::from_utf8(request.wire()).expect("ASCII request");
                assert!(request.contains("\r\nHost: [2001:DB8::1]:8080\r\n"));
                let response = "HTTP/1.1 200 OK"
                    .response(&[("Connection", "close"), ("Content-Length", "2")], b"ok");
                stream.write_all(response.as_slice())
            })
        },
        |server| {
            let response = Request::new(
                Flavor::Fixed,
                &(server.addr()).to_string(),
                Config::try_new("[2001:DB8::1]:8080").expect("HTTP host allocation"),
                Method::GET,
                "/authority",
                &[],
                &[],
            )
            .try_run(
                |result| result.expect("HTTP response"),
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("HTTP request execution");
            assert_eq!(response.status().as_u16(), 200);
            server
                .stop()
                .expect("peer thread")
                .expect("stop authority peer");
        },
    )
    .expect("spawn authority peer")
}
