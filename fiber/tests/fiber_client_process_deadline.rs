use std::{convert::Infallible, thread, time::Duration};

use common::client::recover;
use common::client::{
    gzip, retry,
    runtime::{origins, peer, pools},
};
use dope::core::{driver::life::recovery, io::Reclaimed};
use dope_fiber::{
    BlockOn as _,
    task::{done::Once, time::budget::Elapsed},
};
use dope_test::harness::peer::AcceptScript;
use peer::Response as _;
use sark_client::{Error, Safety, reply::decode::Policy, timeout::Phase};
use sark_fiber::client::{
    config::{Config, Stage},
    process::{self, Process as _},
};
use sark_http::{method::Method, payload::compress::inflate};

pub mod common;

#[test]
fn processing_bound() {
    let body = gzip::DECODED;
    let wire = "HTTP/1.1 200 OK".response(
        &[
            ("Content-Encoding", "gzip"),
            ("Content-Length", &body.len().to_string()),
            ("Connection", "close"),
            ("X-Origin", "fixture"),
        ],
        body,
    );
    AcceptScript::with(
        move |acceptor| peer::serve_each(acceptor, |stream, _| stream.write_all(&wire)),
        |server| {
            for (cap, work, elapsed) in [
                (Some(Duration::from_millis(200)), true, Some(Elapsed::Limit)),
                (Some(Duration::from_millis(200)), false, Some(Elapsed::Limit)),
                (None, false, Some(Elapsed::Budget)),
                (None, false, None),
            ] {
                let config = Config::new(server.addr().to_string())
                    .decode(Policy::Strict)
                    .timeout(Duration::from_secs(2));
                let config = if let Some(cap) = cap {
                    config.limit(Stage::Decode, cap)
                } else {
                    config
                };
                origins::Plan::new(config, 1, 1, 1)
                    .expect("client plan")
                    .enter(
                        |mut app| {
                            let handle = app.handle;
                            let result = app
                                .app
                                .block_on(handle.process(
                                    Method::GET,
                                    "/",
                                    &[],
                                    &[],
                                    |response, check, attempt| {
                                        assert_eq!(attempt.get(), 1);
                                        assert_eq!(
                                            response.body().contiguous(),
                                            Some(b"decoded".as_slice())
                                        );
                                        if let Some(elapsed) = elapsed {
                                            let observed = (0..300)
                                                .find_map(|_| {
                                                    thread::sleep(Duration::from_millis(10));
                                                    check.check().err()
                                                })
                                                .expect("bounded callback deadline");
                                            assert_eq!(observed, elapsed);
                                        }
                                        if work {
                                            Err(process::Error::Work(response))
                                        } else {
                                            Ok(response)
                                        }
                                    },
                                ))
                                .expect("drive process");
                            let mut held = if work || elapsed.is_none() {
                                let result = result.expect("application result");
                                Some(if work {
                                    result.err().expect("retained work error")
                                } else {
                                    result.ok().expect("retained successful value")
                                })
                            } else {
                                let failure = result.err().expect("expired successful work");
                                assert_eq!(failure.safety(), Safety::PossiblyCommitted);
                                assert_eq!(failure.attempt(), 1);
                                assert!(matches!(failure.cause(), Error::Timeout(cause)
                                if cause.phase() == Phase::Decode && Some(cause.elapsed()) == elapsed));
                                None
                            };
                            for _ in 0..2 {
                                let next = app.app.block_on(handle.get("/reuse"))
                                    .expect("drive reuse").expect("reused response");
                                assert_eq!(next.body().contiguous(), Some(b"decoded".as_slice()));
                                if let Some(response) = held.take() {
                                    assert_eq!(response.body().contiguous(), Some(b"decoded".as_slice()));
                                    assert_eq!(response.headers().get("x-origin"), Some(b"fixture".as_slice()));
                                }
                            }
                        },
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("client shutdown");
            }
            server.stop().expect("peer thread").expect("stop peer");
        },
    )
    .expect("spawn peer");
}

#[test]
fn recovered_process_attempt() {
    for (fixed, elapsed) in [(true, false), (false, false), (false, true)] {
        let mut failures = 2;
        AcceptScript::with(
            move |acceptor| {
                peer::serve_each(acceptor, move |stream, request| {
                    assert_eq!(request.method(), "GET");
                    assert_eq!(request.target(), "/retry");
                    if failures != 0 {
                        failures -= 1;
                        return peer::try_shutdown(stream);
                    }
                    stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                })
            },
            |server| {
                let config = Config::new(server.addr().to_string()).timeout(Duration::from_secs(3));
                let mut attempts = [(0, Safety::BeforeSubmit, false); 2];
                if fixed {
                    pools::Plan::new(server.addr(), config, 1, 1)
                        .expect("fixed client plan")
                        .enter(
                            |mut app| {
                                let handle = app.handle;
                                app.wait().expect("fixed client readiness");
                                let (response, attempt) = app
                                    .app
                                    .block_on(handle.process_observed(
                                        Method::GET,
                                        "/retry",
                                        &[],
                                        &[],
                                        retry::observe(&mut attempts),
                                        |response, _, attempt| {
                                            Ok::<_, process::Error<Infallible>>((response, attempt))
                                        },
                                    ))
                                    .expect("drive retry")
                                    .expect("recovered request")
                                    .expect("processed response");
                                assert_eq!(attempt.get(), 3);
                                assert_eq!(response.body().contiguous(), Some(b"ok".as_slice()));
                            },
                            recovery::Policy::new(
                                |event| std::assert_matches!(event, Reclaimed::Shutdown),
                                recover,
                            ),
                            |result| result.expect("client execution"),
                        )
                        .expect("fixed client shutdown");
                } else {
                    origins::Plan::new(config, 1, 1, 1)
                        .expect("client plan")
                        .enter(
                            |mut app| {
                                let handle = app.handle;
                                let result = app
                                    .app
                                    .block_on(handle.process_observed(
                                        Method::GET,
                                        "/retry",
                                        &[],
                                        &[],
                                        retry::observe(&mut attempts),
                                        |response, _, attempt| {
                                            assert_eq!(attempt.get(), 3);
                                            if elapsed {
                                                return Err(process::Error::Elapsed(Elapsed::Limit));
                                            }
                                            Ok::<_, process::Error<Infallible>>((response, attempt))
                                        },
                                    ))
                                    .expect("drive retry");
                                if elapsed {
                                    let failure = result.err().expect("expired recovered processing");
                                    assert_eq!(failure.attempt(), 3);
                                    assert_eq!(failure.safety(), Safety::PossiblyCommitted);
                                    assert!(matches!(failure.cause(), Error::Timeout(cause)
                                        if cause.phase() == Phase::Decode && cause.elapsed() == Elapsed::Limit));
                                } else {
                                    let (response, attempt) = result
                                        .expect("recovered request")
                                        .expect("processed response");
                                    assert_eq!(attempt.get(), 3);
                                    assert_eq!(response.body().contiguous(), Some(b"ok".as_slice()));
                                }
                            },
                            recovery::Policy::new(
                                |event| std::assert_matches!(event, Reclaimed::Shutdown),
                                recover,
                            ),
                            |result| result.expect("client execution"),
                        )
                        .expect("client shutdown");
                }
                assert_eq!(
                    attempts,
                    [
                        (1, Safety::PossiblyCommitted, true),
                        (2, Safety::PossiblyCommitted, true),
                    ]
                );
                server
                    .stop()
                    .expect("peer thread")
                    .expect("stop retry peer");
            },
        )
        .expect("spawn retry peer");
    }
}

#[test]
fn pending_process_cancels() {
    let short = Duration::from_millis(200);
    let long = Duration::from_secs(2);
    for (prefix, total, head, phase, elapsed) in [
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: ".as_slice(),
            long,
            short,
            Phase::Head,
            Elapsed::Limit,
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nx".as_slice(),
            short,
            long,
            Phase::Body,
            Elapsed::Budget,
        ),
    ] {
        AcceptScript::with(
            move |acceptor| -> Result<(), peer::Error> {
                let socket = acceptor.accept()?;
                let mut first = peer::Conn::new(&socket);
                let request = first.try_request()?.expect("pending request");
                assert_eq!(request.method(), "POST");
                assert_eq!(request.target(), "/cancel");
                first.write_all(prefix)?;
                peer::closed(&mut first)?;
                let socket = acceptor.accept()?;
                let mut next = peer::Conn::new(&socket);
                assert_eq!(
                    next.try_request()?.expect("replacement request").target(),
                    "/reuse"
                );
                next.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                )?;
                Ok(())
            },
            |server| {
                let config = Config::new(server.addr().to_string())
                    .timeout(total)
                    .limit(Stage::Head, head);
                origins::Plan::new(config, 1, 1, 1)
                    .expect("client plan")
                    .enter(
                        |mut app| {
                            let handle = app.handle;
                            let mut called = false;
                            let mut retries = 0;
                            let failure = app
                                .app
                                .block_on(handle.process_observed(
                                    Method::POST,
                                    "/cancel",
                                    &[],
                                    &[],
                                    |_| retries += 1,
                                    |_, _, _| {
                                        called = true;
                                        Ok::<_, process::Error<Infallible>>(())
                                    },
                                ))
                                .expect("drive pending process")
                                .expect_err("pending timeout");
                            assert!(!called);
                            assert_eq!(retries, 0);
                            assert_eq!(failure.attempt(), 1);
                            assert_eq!(failure.safety(), Safety::PossiblyCommitted);
                            assert!(matches!(failure.cause(), Error::Timeout(cause)
                                if cause.phase() == phase && cause.elapsed() == elapsed));
                            let next = app
                                .app
                                .block_on(handle.get("/reuse"))
                                .expect("drive replacement")
                                .expect("replacement response");
                            assert_eq!(next.body().contiguous(), Some(b"ok".as_slice()));
                        },
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("client shutdown");
                server
                    .finish()
                    .expect("peer thread")
                    .expect("cancelled peer");
            },
        )
        .expect("spawn peer");
    }
}

#[test]
fn manual_completion_checks() {
    let total = Duration::from_millis(200);
    let offset = Duration::from_millis(1);
    let body = gzip::DECODED;
    let wire = "HTTP/1.1 200 OK".response(
        &[
            ("Content-Encoding", "gzip"),
            ("Content-Length", &body.len().to_string()),
            ("Connection", "close"),
        ],
        body,
    );
    let mut broken = wire.clone();
    let checksum = broken.len() - 8;
    broken[checksum] ^= 1;
    for corrupt in [false, true] {
        let wire = wire.clone();
        let broken = broken.clone();
        AcceptScript::with(
            move |acceptor| {
                let mut count = 0;
                let result = peer::serve_each(acceptor, |stream, request| {
                    count += 1;
                    if count == 1 {
                        assert_eq!(request.target(), "/decode");
                        thread::sleep(total + offset);
                        stream.write_all(if corrupt { &broken } else { &wire })
                    } else {
                        assert_eq!(count, 2);
                        assert_eq!(request.target(), "/reuse");
                        stream.write_all(&wire)
                    }
                });
                assert_eq!(count, 2);
                result
            },
            |server| {
                let config = Config::new(server.addr().to_string())
                    .decode(Policy::Strict)
                    .timeout(total);
                origins::Plan::new(config, 1, 1, 1)
                    .expect("client plan")
                    .enter_at(
                        offset,
                        |mut app| {
                            let handle = app.handle;
                            let start = app.start;
                            let failure = app.app.drive(start, Once::new(handle.get("/decode")))
                                .expect("drive cached response").into_output()
                                .err().expect("expired decode or completed checksum error");
                            assert_eq!(failure.attempt(), 1);
                            assert_eq!(failure.safety(), Safety::PossiblyCommitted);
                            if corrupt {
                                assert!(matches!(failure.cause(), Error::Decompression(inflate::DecodeError::Stream(cause))
                                    if cause.written() == b"decoded".len()
                                        && matches!(cause.error(), inflate::Error::Checksum { expected, actual }
                                            if expected == actual ^ 1)));
                            } else {
                                assert!(matches!(failure.cause(), Error::Timeout(cause)
                                    if cause.phase() == Phase::Decode && cause.elapsed() == Elapsed::Budget));
                            }
                            let failure = app.app.drive(start, Once::new(handle.get("/expired")))
                                .expect("drive expired dispatch").into_output()
                                .err().expect("expired dispatch must not submit");
                            assert_eq!(failure.attempt(), 1);
                            assert_eq!(failure.safety(), Safety::BeforeSubmit);
                            assert!(matches!(failure.cause(), Error::Timeout(cause)
                                if cause.phase() == Phase::Dispatch && cause.elapsed() == Elapsed::Budget));
                            let fresh = start.try_add(start.into_raw().elapsed())
                                .expect("fresh manual sample");
                            let response = app.app.drive(fresh, Once::new(handle.get("/reuse")))
                                .expect("drive reuse").into_output().expect("reused response");
                            assert_eq!(response.body().contiguous(), Some(b"decoded".as_slice()));
                        },
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown), recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("client shutdown");
                server.stop().expect("peer thread").expect("stop peer");
            },
        ).expect("spawn peer");
    }
}
