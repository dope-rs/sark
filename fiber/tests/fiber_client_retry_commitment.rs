use common::client::recover;
use common::client::runtime::peer;
use dope::core::driver::life::recovery;
use dope_test::harness::{GUARD, peer::AcceptScript};
pub mod common;

use std::{convert::Infallible, net::TcpListener, time::Duration};

use common::client::{
    config::FLAVORS,
    retry::AGGRESSIVE,
    runtime::{
        cases::{Flavor, request::Request},
        origins, pools,
    },
};
use dope::core::{io::Reclaimed, platform::wake};
use dope::net::link::event;
use dope_fiber::{BlockOn as _, task::time::budget};
use sark::client::protocol::retry::Policy;
use sark_client::{Error, RequestError, Safety, conn, connect, protocol, timeout};
use sark_fiber::client::{
    config::Config,
    process::{self, Process as _},
};
use sark_http::{
    h1::{field::syntax, head},
    method::Method,
};

#[test]
fn observed_reconnect_keeps_commitment() {
    AcceptScript::with(
        |acceptor| -> Result<(), peer::Error> {
            let accepted = acceptor.accept()?;
            let mut conn = peer::Conn::new(&accepted);
            let request = conn.try_request()?.expect("committed request");
            assert_eq!(request.method(), "GET");
            assert_eq!(request.target(), "/retry");
            peer::try_shutdown(&accepted)?;
            Ok(())
        },
        |server| {
            let config = Config::new(server.addr().to_string()).timeout(Duration::from_secs(3));
            let mut server = Some(server);
            origins::Plan::new(config, 1, 1, 1)
                .expect("reconnect plan")
                .enter(
                    |mut app| {
                        let handle = app.handle;
                        let mut observed = 0;
                        let failure = app
                            .app
                            .block_on(handle.process_observed(
                                Method::GET,
                                "/retry",
                                &[],
                                &[],
                                |failure| {
                                    observed += 1;
                                    assert_eq!(failure.attempt(), observed);
                                    assert_eq!(failure.safety(), Safety::PossiblyCommitted);
                                    if let Some(server) = server.take() {
                                        assert!(matches!(
                                            failure.cause(),
                                            Error::Transport(conn::Error::Classified(
                                                conn::Kind::RemoteEof
                                            ))
                                        ));
                                        // Join before backoff so every later dial sees a closed listener.
                                        server.finish().expect("peer thread").expect("first GET");
                                    } else {
                                        assert!(matches!(failure.cause(), Error::Connect(
                                            connect::Error::Attempt(event::ConnectFault::Transport(
                                                event::ConnectError::Connect(errno)
                                            ))
                                        ) if errno.is_refused()));
                                    }
                                },
                                |_, _, _| Ok::<_, process::Error<Infallible>>(()),
                            ))
                            .expect("drive reconnect")
                            .expect_err("closed listener");
                        assert_eq!(observed, 2);
                        assert_eq!(failure.attempt(), 3);
                        assert_eq!(failure.safety(), Safety::PossiblyCommitted);
                        assert!(matches!(failure.cause(), Error::Connect(
                            connect::Error::Attempt(event::ConnectFault::Transport(
                                event::ConnectError::Connect(errno)
                            ))
                        ) if errno.is_refused()));
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("client shutdown");
        },
    )
    .expect("spawn committed peer");
}

#[test]
fn post_is_not_replayed() {
    for flavor in FLAVORS {
        for policy in [Policy::default(), AGGRESSIVE] {
            let (quiesced, release) = wake::Ends::blocking()
                .expect("post observation gate")
                .split();
            AcceptScript::with(
                move |acceptor| {
                    let accepted = acceptor.accept()?;
                    let mut conn = peer::Conn::new(&accepted);
                    assert!(conn.try_request()?.expect("submitted post").is_effect());
                    peer::try_shutdown(&accepted)?;
                    quiesced.wait_timeout(GUARD).expect("post driver quiesces");
                    peer::closed(&mut conn)?;
                    peer::empty(acceptor)
                },
                |server| {
                    let config = Config::new(server.addr().to_string())
                        .retry(policy)
                        .timeout(Duration::from_secs(3));
                    let outcome = Request::new(
                        flavor,
                        &server.addr().to_string(),
                        config,
                        Method::POST,
                        "/effect",
                        &[],
                        b"effect",
                    )
                    .try_run(
                        |outcome| {
                            let error = (outcome).err().expect("a response-free close must fail");
                            (
                                matches!(
                                    error.cause(),
                                    Error::Transport(error) if error.kind() == conn::Kind::RemoteEof
                                ),
                                error.safety(),
                            )
                        },
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("HTTP request execution");
                    release.notify().expect("post observation release");
                    server
                        .finish()
                        .expect("peer thread")
                        .expect("finish response-free peer");
                    assert!((outcome) == (true, Safety::PossiblyCommitted));
                },
            )
            .expect("spawn response-free peer")
        }
    }
}

#[test]
fn buffered_timeout_commits() {
    for flavor in FLAVORS {
        let (quiesced, release) = wake::Ends::blocking()
            .expect("timeout observation gate")
            .split();
        AcceptScript::with(
            move |acceptor| {
                let accepted = acceptor.accept()?;
                let mut conn = peer::Conn::new(&accepted);
                assert!(conn.try_request()?.expect("submitted post").is_effect());
                quiesced
                    .wait_timeout(GUARD)
                    .expect("timeout driver quiesces");
                peer::closed(&mut conn)?;
                peer::try_shutdown(&accepted)?;
                peer::empty(acceptor)
            },
            |server| {
                let config = Config::new(server.addr().to_string())
                    .retry(AGGRESSIVE)
                    .timeout(Duration::from_millis(50));
                let outcome = Request::new(
                    flavor,
                    &server.addr().to_string(),
                    config,
                    Method::POST,
                    "/effect",
                    &[],
                    b"effect",
                )
                .try_run(
                    |outcome| {
                        let error = (outcome)
                            .err()
                            .expect("a response timeout after submission must fail");
                        (
                            matches!(
                                error.cause(),
                                Error::Timeout(error)
                                    if error.phase() == timeout::Phase::Head
                                        && error.elapsed() == budget::Elapsed::Budget
                            ),
                            error.safety(),
                        )
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("HTTP request execution");
                release.notify().expect("release timed-out peer");
                server
                    .finish()
                    .expect("peer thread")
                    .expect("finish held-response peer");
                assert!((outcome) == (true, Safety::PossiblyCommitted));
            },
        )
        .expect("spawn held-response peer")
    }
}

#[test]
fn stream_failure_commits() {
    for flavor in FLAVORS {
        let (quiesced, release) = wake::Ends::blocking()
            .expect("stream observation gate")
            .split();
        AcceptScript::with(
            move |acceptor| {
                let accepted = acceptor.accept()?;
                let mut conn = peer::Conn::new(&accepted);
                assert!(conn.try_request()?.expect("submitted post").is_effect());
                conn.write_all(b"HTTP/1.1 200 OK\r\nBad Name: value\r\nContent-Length: 0\r\n\r\n")?;
                quiesced
                    .wait_timeout(GUARD)
                    .expect("stream driver quiesces");
                peer::closed(&mut conn)?;
                peer::empty(acceptor)
            },
            |server| {
                let config = Config::new(server.addr().to_string())
                    .retry(AGGRESSIVE)
                    .timeout(Duration::from_secs(3));
                match flavor {
                    Flavor::Fixed => {
                        pools::Plan::new(server.addr(), config, 1, 1)
                            .expect("fixed stream failure topology")
                            .enter(
                                |mut app| {
                                    let client = app.handle;
                                    app.wait().expect("fixed stream failure readiness");
                                    let mut response = app
                                        .app
                                        .block_on(client.stream(Method::POST, "/effect", b"effect"))
                                        .expect("fixed stream failure driver")
                                        .expect("fixed response stream");
                                    loop {
                                        match app
                                        .app
                                        .block_on(response.next_event())
                                        .expect("fixed response event driver")
                                        .expect(
                                            "malformed fixed response must fail before completion",
                                        ) {
                                        Ok(_) => {}
                                        Err(failure) => {
    assert!(failure.safety() == Safety::PossiblyCommitted);
    assert!(matches!(failure.cause(),
        Error::Protocol(protocol::Error::Head(head::ParseError::Field(
            syntax::Error::Name { offset: 20, actual: b' ' }
        )))
    ));
    break;
},
                                    }
                                    }
                                },
                                recovery::Policy::new(
                                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                                    recover,
                                ),
                                |result| result.expect("client execution"),
                            )
                            .expect("fixed stream failure shutdown")
                    }
                    Flavor::OnDemand => origins::Plan::new(config, 1, 1, 1)
                        .expect("on-demand stream failure topology")
                        .enter(
                            |mut app| {
                                let client = app.handle;
                                let mut response = app
                                    .app
                                    .block_on(client.stream(Method::POST, "/effect", b"effect"))
                                    .expect("on-demand stream failure driver")
                                    .expect("on-demand response stream");
                                loop {
                                    match app
                                    .app
                                    .block_on(response.next_event())
                                    .expect("on-demand response event driver")
                                    .expect(
                                        "malformed on-demand response must fail before completion",
                                    ) {
                                    Ok(_) => {}
                                    Err(failure) => {
    assert!(failure.safety() == Safety::PossiblyCommitted);
    assert!(matches!(failure.cause(),
        Error::Protocol(protocol::Error::Head(head::ParseError::Field(
            syntax::Error::Name { offset: 20, actual: b' ' }
        )))
    ));
    break;
},
                                }
                                }
                            },
                            recovery::Policy::new(
                                |event| std::assert_matches!(event, Reclaimed::Shutdown),
                                recover,
                            ),
                            |result| result.expect("client execution"),
                        )
                        .expect("on-demand stream failure shutdown"),
                }
                release.notify().expect("stream observation release");
                server
                    .finish()
                    .expect("peer thread")
                    .expect("finish malformed-response peer");
            },
        )
        .expect("spawn malformed-response peer")
    }
}

#[test]
fn redirect_failure_commits() {
    for flavor in FLAVORS {
        let target = {
            let listener = TcpListener::bind("127.0.0.1:0").expect("unavailable redirect target");
            listener.local_addr().expect("redirect target address")
        };

        let (quiesced, release) = wake::Ends::blocking()
            .expect("redirect observation gate")
            .split();
        let response = format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{target}/next\r\nContent-Length: 0\r\n\r\n"
        );
        AcceptScript::with(
            move |acceptor| {
                let accepted = acceptor.accept()?;
                let mut conn = peer::Conn::new(&accepted);
                assert!(conn.try_request()?.expect("submitted post").is_effect());
                conn.write_all(response.as_bytes())?;
                quiesced
                    .wait_timeout(GUARD)
                    .expect("redirect driver quiesces");
                peer::closed(&mut conn)?;
                peer::empty(acceptor)
            },
            |server| {
                let config = Config::new(server.addr().to_string())
                    .retry(AGGRESSIVE)
                    .timeout(Duration::from_millis(200));
                Request::new(
                    flavor,
                    &server.addr().to_string(),
                    config,
                    Method::POST,
                    "/effect",
                    &[],
                    b"effect",
                )
                .try_run(
                    |outcome| {
                        let failure = (outcome).err().expect("redirect follow must fail");
                        assert!((failure.safety()) == (Safety::PossiblyCommitted));
                        assert!(matches!(
                            failure.cause(),
                            Error::Request(RequestError::CrossOrigin)
                        ));
                    },
                    recovery::Policy::new(
                        |event| {
                            assert!(
                                matches!(event, dope::core::io::Reclaimed::Shutdown),
                                "unexpected client retirement: {event:?}"
                            )
                        },
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("HTTP request execution");
                release.notify().expect("redirect observation release");
                server
                    .finish()
                    .expect("peer thread")
                    .expect("finish redirect peer");
            },
        )
        .expect("spawn redirect peer")
    }
}

#[test]
fn scoped_timeout_commits() {
    for flavor in FLAVORS {
        let (quiesced, release) = wake::Ends::blocking()
            .expect("scoped timeout observation gate")
            .split();
        AcceptScript::with(
            move |acceptor| {
                let accepted = acceptor.accept()?;
                let mut conn = peer::Conn::new(&accepted);
                assert!(conn.try_request()?.expect("submitted post").is_effect());
                quiesced
                    .wait_timeout(GUARD)
                    .expect("scoped timeout driver quiesces");
                peer::closed(&mut conn)?;
                peer::try_shutdown(&accepted)?;
                peer::empty(acceptor)
            },
            |server| {
                let config = Config::new(server.addr().to_string())
                    .retry(AGGRESSIVE)
                    .timeout(Duration::from_millis(50));

                let outcome = Request::new(
                    flavor,
                    &server.addr().to_string(),
                    config,
                    Method::POST,
                    "/effect",
                    &[],
                    b"effect",
                )
                .try_scoped(
                    |result| {
                        let failure = result.err().expect("scoped request must fail");
                        (
                            matches!(
                                failure.cause(),
                                Error::Timeout(error)
                                    if error.phase() == timeout::Phase::Head
                                        && error.elapsed() == budget::Elapsed::Budget
                            ),
                            failure.safety(),
                        )
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("scoped request execution");
                release.notify().expect("release timed-out scoped peer");
                server
                    .finish()
                    .expect("peer thread")
                    .expect("finish held scoped peer");
                assert!((outcome) == (true, Safety::PossiblyCommitted));
            },
        )
        .expect("spawn held scoped peer")
    }
}

#[test]
fn scoped_capacity_commits() {
    for flavor in FLAVORS {
        let (quiesced, release) = wake::Ends::blocking()
            .expect("capacity observation gate")
            .split();
        AcceptScript::with(
            move |acceptor| {
                let accepted = acceptor.accept()?;
                let mut conn = peer::Conn::new(&accepted);
                assert!(conn.try_request()?.expect("submitted post").is_effect());
                conn.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nlarge",
                )?;
                quiesced
                    .wait_timeout(GUARD)
                    .expect("capacity driver quiesces");
                peer::closed(&mut conn)?;
                peer::empty(acceptor)
            },
            |server| {
                let config = Config::new(server.addr().to_string())
                    .retry(AGGRESSIVE)
                    .wire_limit(4)
                    .timeout(Duration::from_secs(3));

                let outcome = Request::new(
                    flavor,
                    &server.addr().to_string(),
                    config,
                    Method::POST,
                    "/effect",
                    &[],
                    b"effect",
                )
                .try_scoped(
                    |result| {
                        let failure = result.err().expect("scoped request must fail");
                        (
                            matches!(failure.cause(), Error::Protocol(protocol::Error::BodyLimit)),
                            failure.safety(),
                        )
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("scoped request execution");
                release.notify().expect("capacity observation release");
                server
                    .finish()
                    .expect("peer thread")
                    .expect("finish oversized-response peer");
                assert!((outcome) == (true, Safety::PossiblyCommitted));
            },
        )
        .expect("spawn oversized-response peer")
    }
}
