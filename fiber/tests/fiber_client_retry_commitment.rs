use common::client::assert_shutdown;
use common::client::runtime::peer;
use dope_test::harness::{
    peer::{AcceptScript, TcpAcceptor},
    scenario::run::{Entry, Task as _},
};
pub mod common;

use std::{convert::Infallible, net::TcpListener, time::Duration};

use common::client::{
    config::FLAVORS,
    retry::{self, AGGRESSIVE, Post},
    runtime::{cases::Flavor, origins, pools},
};
use dope::{manifold::client::attempt, net::link::event};
use dope_fiber::BlockOn as _;
use sark::client::protocol::retry::Policy;
use sark_client::{Error, RequestError, Safety, conn, connect, protocol};
use sark_fiber::client::{
    config::Config,
    process::{self, Process as _},
};
use sark_http::method::Method;

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
                    assert_shutdown,
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
                retry::effect(flavor, address, config)
                    .try_run(|result| {
                        let failure = result.err().expect("a response-free close must fail");
                        assert!(failure.safety() == Safety::PossiblyCommitted);
                        assert!(matches!(failure.cause(),
                            Error::Transport(error) if error.kind() == conn::Kind::RemoteEof));
                    })
                    .expect("HTTP request execution");
            })
            .with(|| Post::Disconnect.serve(&mut acceptor))
            .expect("client worker");
            served.expect("peer exchange");
            peer::empty(&mut acceptor).expect("no replayed requests");
        }
    }
}

#[test]
fn buffered_and_scoped_timeouts_commit() {
    for flavor in FLAVORS {
        for scoped in [false, true] {
            let mut acceptor =
                TcpAcceptor::bind(([127, 0, 0, 1], 0).into()).expect("peer listener");
            let address = acceptor.local_addr().expect("peer address");
            let (served, ()) = Entry(move || {
                let config = Config::new(address.to_string())
                    .retry(AGGRESSIVE)
                    .timeout(Duration::from_millis(50));
                let request = retry::effect(flavor, address, config);
                if scoped {
                    request.try_scoped(|result| retry::assert_timeout(result))
                } else {
                    request.try_run(|result| retry::assert_timeout(result))
                }
                .expect("timed POST execution");
            })
            .with(|| Post::Timeout.serve(&mut acceptor))
            .expect("client worker");
            served.expect("peer exchange");
            peer::empty(&mut acceptor).expect("no replayed requests");
        }
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
                            app.wait().expect("fixed stream failure readiness");
                            let response = app
                                .app
                                .block_on(app.handle.stream(Method::POST, "/effect", b"effect"))
                                .expect("fixed stream failure driver")
                                .expect("fixed response stream");
                            retry::assert_invalid_head(&mut app.app, response);
                        },
                        assert_shutdown,
                    )
                    .expect("fixed stream failure shutdown"),
                Flavor::OnDemand => origins::Plan::new(config, 1, 1, 1)
                    .expect("on-demand stream failure topology")
                    .enter(
                        (),
                        |mut app| {
                            let response = app
                                .app
                                .block_on(app.handle.stream(Method::POST, "/effect", b"effect"))
                                .expect("on-demand stream failure driver")
                                .expect("on-demand response stream");
                            retry::assert_invalid_head(&mut app.app, response);
                        },
                        assert_shutdown,
                    )
                    .expect("on-demand stream failure shutdown"),
            }
        })
        .with(|| {
            Post::Reply(b"HTTP/1.1 200 OK\r\nBad Name: value\r\nContent-Length: 0\r\n\r\n")
                .serve(&mut acceptor)
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
            retry::effect(flavor, address, config)
                .try_run(|result| {
                    let failure = result.err().expect("redirect follow must fail");
                    assert!(failure.safety() == Safety::PossiblyCommitted);
                    assert!(matches!(failure.cause(), Error::Request(RequestError::CrossOrigin)));
                })
                .expect("HTTP request execution");
        })
        .with(|| Post::Reply(response.as_bytes()).serve(&mut acceptor))
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
            retry::effect(flavor, address, config)
                .try_scoped(|result| {
                    let failure = result.err().expect("scoped request must fail");
                    assert!(failure.safety() == Safety::PossiblyCommitted);
                    assert!(matches!(failure.cause(), Error::Protocol(protocol::Error::BodyLimit)));
                })
                .expect("scoped request execution");
        })
        .with(|| {
            Post::Reply(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nlarge")
                .serve(&mut acceptor)
        })
        .expect("client worker");
        served.expect("peer exchange");
        peer::empty(&mut acceptor).expect("no replayed requests");
    }
}
