use common::client::recover;
use common::client::runtime::peer;
use dope::core::driver::life::recovery;
use dope_test::harness::{
    GUARD,
    peer::{AcceptScript, TcpAcceptor},
    scenario::run::{Entry, Task as _},
};
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
use dope::core::io::Reclaimed;
use dope::{manifold::client::attempt, net::link::event};
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
                    (),
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
                                            connect::Error::Attempt(attempt::ConnectFault::Transport(
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
                            connect::Error::Attempt(attempt::ConnectFault::Transport(
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
            let mut acceptor =
                TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
            let address = acceptor.local_addr().expect("peer address");
            let (served, ()) = Entry(move || {
                let config = Config::new(address.to_string())
                    .retry(policy)
                    .timeout(Duration::from_secs(3));
                let outcome = Request::new(
                    flavor,
                    &address.to_string(),
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
                assert!((outcome) == (true, Safety::PossiblyCommitted));
            })
            .with(|| -> Result<(), peer::Error> {
                let accepted = acceptor.accept_for(GUARD)?.expect("client before deadline");
                accepted.read_wait(Some(GUARD))?;
                accepted.write_wait(Some(GUARD))?;
                accepted.set_nodelay(true)?;
                let mut conn = peer::Conn::new(&accepted);
                assert!(conn.try_request()?.expect("submitted post").is_effect());
                peer::try_shutdown(&accepted)?;
                peer::closed(&mut conn)?;
                Ok(())
            })
            .expect("client worker");
            served.expect("peer exchange");
            peer::empty(&mut acceptor).expect("no replayed requests");
        }
    }
}

#[test]
fn buffered_timeout_commits() {
    for flavor in FLAVORS {
        let mut acceptor = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
        let address = acceptor.local_addr().expect("peer address");
        let (served, ()) = Entry(move || {
            let config = Config::new(address.to_string())
                .retry(AGGRESSIVE)
                .timeout(Duration::from_millis(50));
            let outcome = Request::new(
                flavor,
                &address.to_string(),
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
            assert!((outcome) == (true, Safety::PossiblyCommitted));
        })
        .with(|| -> Result<(), peer::Error> {
            let accepted = acceptor.accept_for(GUARD)?.expect("client before deadline");
            accepted.read_wait(Some(GUARD))?;
            accepted.write_wait(Some(GUARD))?;
            accepted.set_nodelay(true)?;
            let mut conn = peer::Conn::new(&accepted);
            assert!(conn.try_request()?.expect("submitted post").is_effect());
            peer::closed(&mut conn)?;
            peer::try_shutdown(&accepted)?;
            Ok(())
        })
        .expect("client worker");
        served.expect("peer exchange");
        peer::empty(&mut acceptor).expect("no replayed requests");
    }
}

#[test]
fn stream_failure_commits() {
    for flavor in FLAVORS {
        let mut acceptor = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
        let address = acceptor.local_addr().expect("peer address");
        let (served, ()) = Entry(move || {
            let config = Config::new(address.to_string())
                .retry(AGGRESSIVE)
                .timeout(Duration::from_secs(3));
            match flavor {
                Flavor::Fixed => pools::Plan::new(address, config, 1, 1)
                    .expect("fixed stream failure topology")
                    .enter(
                        (),
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
                                    .expect("malformed fixed response must fail before completion")
                                {
                                    Ok(_) => {}
                                    Err(failure) => {
                                        assert!(failure.safety() == Safety::PossiblyCommitted);
                                        assert!(matches!(
                                            failure.cause(),
                                            Error::Protocol(protocol::Error::Head(
                                                head::ParseError::Field(syntax::Error::Name {
                                                    offset: 20,
                                                    actual: b' '
                                                })
                                            ))
                                        ));
                                        break;
                                    }
                                }
                            }
                        },
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("fixed stream failure shutdown"),
                Flavor::OnDemand => origins::Plan::new(config, 1, 1, 1)
                    .expect("on-demand stream failure topology")
                    .enter(
                        (),
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
                                        assert!(matches!(
                                            failure.cause(),
                                            Error::Protocol(protocol::Error::Head(
                                                head::ParseError::Field(syntax::Error::Name {
                                                    offset: 20,
                                                    actual: b' '
                                                })
                                            ))
                                        ));
                                        break;
                                    }
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
        })
        .with(|| -> Result<(), peer::Error> {
            let accepted = acceptor.accept_for(GUARD)?.expect("client before deadline");
            accepted.read_wait(Some(GUARD))?;
            accepted.write_wait(Some(GUARD))?;
            accepted.set_nodelay(true)?;
            let mut conn = peer::Conn::new(&accepted);
            assert!(conn.try_request()?.expect("submitted post").is_effect());
            conn.write_all(b"HTTP/1.1 200 OK\r\nBad Name: value\r\nContent-Length: 0\r\n\r\n")?;
            peer::closed(&mut conn)?;
            Ok(())
        })
        .expect("client worker");
        served.expect("peer exchange");
        peer::empty(&mut acceptor).expect("no replayed requests");
    }
}

#[test]
fn redirect_failure_commits() {
    for flavor in FLAVORS {
        let target = {
            let listener = TcpListener::bind("127.0.0.1:0").expect("unavailable redirect target");
            listener.local_addr().expect("redirect target address")
        };

        let response = format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{target}/next\r\nContent-Length: 0\r\n\r\n"
        );
        let mut acceptor = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
        let address = acceptor.local_addr().expect("peer address");
        let (served, ()) = Entry(move || {
            let config = Config::new(address.to_string())
                .retry(AGGRESSIVE)
                .timeout(Duration::from_millis(200));
            Request::new(
                flavor,
                &address.to_string(),
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
        })
        .with(|| -> Result<(), peer::Error> {
            let accepted = acceptor.accept_for(GUARD)?.expect("client before deadline");
            accepted.read_wait(Some(GUARD))?;
            accepted.write_wait(Some(GUARD))?;
            accepted.set_nodelay(true)?;
            let mut conn = peer::Conn::new(&accepted);
            assert!(conn.try_request()?.expect("submitted post").is_effect());
            conn.write_all(response.as_bytes())?;
            peer::closed(&mut conn)?;
            Ok(())
        })
        .expect("client worker");
        served.expect("peer exchange");
        peer::empty(&mut acceptor).expect("no replayed requests");
    }
}

#[test]
fn scoped_timeout_commits() {
    for flavor in FLAVORS {
        let mut acceptor = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
        let address = acceptor.local_addr().expect("peer address");
        let (served, ()) = Entry(move || {
            let config = Config::new(address.to_string())
                .retry(AGGRESSIVE)
                .timeout(Duration::from_millis(50));

            let outcome = Request::new(
                flavor,
                &address.to_string(),
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
            assert!((outcome) == (true, Safety::PossiblyCommitted));
        })
        .with(|| -> Result<(), peer::Error> {
            let accepted = acceptor.accept_for(GUARD)?.expect("client before deadline");
            accepted.read_wait(Some(GUARD))?;
            accepted.write_wait(Some(GUARD))?;
            accepted.set_nodelay(true)?;
            let mut conn = peer::Conn::new(&accepted);
            assert!(conn.try_request()?.expect("submitted post").is_effect());
            peer::closed(&mut conn)?;
            peer::try_shutdown(&accepted)?;
            Ok(())
        })
        .expect("client worker");
        served.expect("peer exchange");
        peer::empty(&mut acceptor).expect("no replayed requests");
    }
}

#[test]
fn scoped_capacity_commits() {
    for flavor in FLAVORS {
        let mut acceptor = TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
        let address = acceptor.local_addr().expect("peer address");
        let (served, ()) = Entry(move || {
            let config = Config::new(address.to_string())
                .retry(AGGRESSIVE)
                .wire_limit(4)
                .timeout(Duration::from_secs(3));

            let outcome = Request::new(
                flavor,
                &address.to_string(),
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
            assert!((outcome) == (true, Safety::PossiblyCommitted));
        })
        .with(|| -> Result<(), peer::Error> {
            let accepted = acceptor.accept_for(GUARD)?.expect("client before deadline");
            accepted.read_wait(Some(GUARD))?;
            accepted.write_wait(Some(GUARD))?;
            accepted.set_nodelay(true)?;
            let mut conn = peer::Conn::new(&accepted);
            assert!(conn.try_request()?.expect("submitted post").is_effect());
            conn.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nlarge",
            )?;
            peer::closed(&mut conn)?;
            Ok(())
        })
        .expect("client worker");
        served.expect("peer exchange");
        peer::empty(&mut acceptor).expect("no replayed requests");
    }
}
