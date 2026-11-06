pub mod common;

use std::{io, net::TcpListener, time::Duration};

use common::client::recover;
use common::client::{
    routes::*,
    runtime::{
        origins,
        peer::{self, Response as _},
    },
};
use dope::core::{driver::life::recovery, io::Reclaimed};
use dope_fiber::BlockOn as _;
use dope_test::harness::peer::{AcceptScript, Stream};
use sark_fiber::client::config::Config;
use sark_http::method::Method;

#[test]
fn redirect_cannot_replay_secrets() {
    let target = TcpListener::bind("127.0.0.1:0").expect("bind redirect target");
    let location = format!(
        "http://{}/final",
        target.local_addr().expect("target address")
    );
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(
                acceptor,
                move |stream: &mut &Stream<'_>, request: peer::Request<'_>| {
                    assert_eq!(request.target(), "/start");
                    assert_eq!(request.body(), SECRET_BODY);
                    assert!(String::from_utf8_lossy(request.wire()).contains("X-Api-Key: secret"));
                    stream.write_all(
                        &"HTTP/1.1 307 Temporary Redirect".response(
                            &[("Location", &location), ("Content-Length", "0")],
                            &[],
                        ),
                    )
                },
            )
        },
        |redirect| {
            origins::Plan::new(Config::new(redirect.addr().to_string()), 2, 1, 2)
                .expect("dynamic client plan")
                .enter(
                    (),
                    |mut app| {
                        let failure = app.app
                            .block_on(app.handle.collect_with(Method::POST, "/start", &SECRETS, SECRET_BODY))
                            .expect("drive HTTP request")
                            .err()
                            .expect("cross-origin redirect must fail");
                        assert!(matches!(failure.cause(), sark_client::Error::Request(sark_client::RequestError::CrossOrigin)));
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("client scope");
            target.set_nonblocking(true).expect("observe redirect target");
            std::assert_matches!(target.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock);
            redirect.stop().expect("peer thread").expect("stop redirect origin");
        },
    )
    .expect("spawn redirect origin");
}

#[test]
fn absolute_rejects_headers_and_body_before_dial() {
    let origin = TcpListener::bind("127.0.0.1:0").expect("bind configured origin");
    let target = TcpListener::bind("127.0.0.1:0").expect("bind absolute target");
    let path = format!(
        "http://{}/direct",
        target.local_addr().expect("target address")
    );
    let addr = origin.local_addr().expect("configured origin address");
    origins::Plan::new(
        Config::new(addr.to_string()).timeout(Duration::from_secs(1)),
        2,
        1,
        2,
    )
    .expect("dynamic client plan")
    .enter(
        (),
        |mut app| {
            for (headers, body) in [(&SECRETS[2..], &[][..]), (&[][..], SECRET_BODY)] {
                let failure = app
                    .app
                    .block_on(app.handle.collect_with(Method::POST, &path, headers, body))
                    .expect("drive HTTP request")
                    .err()
                    .expect("cross-origin request must fail");
                assert!(matches!(
                    failure.cause(),
                    sark_client::Error::Request(sark_client::RequestError::CrossOrigin)
                ));
            }
        },
        recovery::Policy::new(
            |event| std::assert_matches!(event, Reclaimed::Shutdown),
            recover,
        ),
        |result| result.expect("client execution"),
    )
    .expect("client scope");
    for listener in [origin, target] {
        listener.set_nonblocking(true).expect("observe peer");
        std::assert_matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock);
    }
}

#[test]
fn redirect_alias_reuses_origin() {
    let base = TcpListener::bind("127.0.0.1:0").expect("bind configured origin");
    AcceptScript::with(
        |acceptor| {
            peer::serve(acceptor, |request| {
                if request.target() == "/start" {
                    let host = request
                        .wire()
                        .split(|byte| *byte == b'\n')
                        .find_map(|line| line.strip_prefix(b"Host: "))
                        .expect("host field");
                    let host = std::str::from_utf8(host).expect("host text").trim();
                    format!("http://%31{}/final", &host[1..]).location()
                } else {
                    assert_eq!(request.target(), "/final");
                    ARRIVED.to_vec()
                }
            })
        },
        |server| {
            let path = format!("http://{}/start", server.addr());
            let response = origins::Plan::new(
                Config::new(base.local_addr().expect("configured address").to_string()),
                2,
                1,
                1,
            )
            .expect("one dynamic origin")
            .enter(
                (),
                |mut app| {
                    app.app
                        .block_on(app.handle.collect(Method::GET, &path, &[]))
                        .expect("drive HTTP request")
                        .expect("same-origin redirect response")
                },
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("client scope");
            assert_eq!(response.body().as_slice(), b"arrived");
            server.stop().expect("peer thread").expect("stop origin");
        },
    )
    .expect("spawn origin");
}

#[test]
fn routes_each_origin() {
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(
                acceptor,
                |stream: &mut &Stream<'_>, request: peer::Request<'_>| {
                    std::assert_matches!(request.target(), "/first" | "/again");
                    stream.write_all(FIRST)
                },
            )
        },
        |first| {
            AcceptScript::with(
                move |acceptor| {
                    peer::serve_each(
                        acceptor,
                        |stream: &mut &Stream<'_>, request: peer::Request<'_>| {
                            assert_eq!(request.target(), "/second");
                            stream.write_all(SECOND)
                        },
                    )
                },
                |second| {
                    let paths = [
                        format!("http://{}/first", first.addr()),
                        format!("http://{}/second", second.addr()),
                        format!("http://{}/again", first.addr()),
                    ];
                    let replies =
                        origins::Plan::new(Config::new(first.addr().to_string()), 2, 1, 2)
                            .expect("dynamic client plan")
                            .enter(
                                (),
                                |mut app| {
                                    paths
                                        .iter()
                                        .map(|path| {
                                            app.app
                                                .block_on(app.handle.collect(
                                                    Method::GET,
                                                    path,
                                                    &[],
                                                ))
                                                .expect("drive HTTP request")
                                                .expect("HTTP response")
                                        })
                                        .collect::<Vec<_>>()
                                },
                                recovery::Policy::new(
                                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                                    recover,
                                ),
                                |result| result.expect("client execution"),
                            )
                            .expect("client scope");
                    let bodies = replies
                        .iter()
                        .map(|response| response.body().as_slice())
                        .collect::<Vec<_>>();
                    assert_eq!(bodies, [b"A".as_slice(), b"B".as_slice(), b"A".as_slice()]);
                    first
                        .stop()
                        .expect("peer thread")
                        .expect("stop first origin");
                    second
                        .stop()
                        .expect("peer thread")
                        .expect("stop second origin");
                },
            )
            .expect("spawn second origin");
        },
    )
    .expect("spawn first origin");
}
