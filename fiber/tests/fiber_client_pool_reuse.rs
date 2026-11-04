use common::client::recover;
use common::client::runtime::peer;
use dope::core::{driver::life::recovery, io::Reclaimed};
use dope_test::harness::peer::{AcceptScript, Stream};
pub mod common;

use std::{num::NonZero, time::Duration};

use common::client::{
    config::{Config as _, FLAVORS},
    runtime::{
        cases::{Batch, Flavor},
        peer::Response as _,
    },
};
use sark_fiber::client::config::Config;

#[test]
fn survives_peer_close() {
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(
                acceptor,
                |stream: &mut &Stream<'_>, request: peer::Request<'_>| {
                    assert_eq!(request.method(), "GET");
                    let response = "HTTP/1.1 200 OK"
                        .response(&[("Connection", "close"), ("Content-Length", "2")], b"ok");
                    stream.write_all(response.as_slice())
                },
            )
        },
        |server| {
            let addr = server.addr();

            let codes = Batch::new(
                Flavor::Fixed,
                addr,
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                NonZero::new(2).expect("client capacity"),
                &["/1", "/2", "/3", "/4", "/5", "/6"],
                &[],
                None,
            )
            .try_run(
                |result| result.expect("batch response").status().as_u16(),
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("client scenario");
            assert_eq!(codes, vec![200, 200, 200, 200, 200, 200]);
            server
                .stop()
                .expect("peer thread")
                .expect("stop close-after-response peer");
        },
    )
    .expect("spawn close-after-response peer")
}

#[test]
fn drains_long_sequence() {
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(
                acceptor,
                |stream: &mut &Stream<'_>, request: peer::Request<'_>| {
                    assert_eq!(request.method(), "GET");
                    let response = "HTTP/1.1 200 OK"
                        .response(&[("Connection", "close"), ("Content-Length", "4")], b"done");
                    stream.write_all(response.as_slice())
                },
            )
        },
        |server| {
            let addr = server.addr();

            let paths: &[&str] = &["/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h", "/i", "/j"];
            let codes = Batch::new(
                Flavor::Fixed,
                addr,
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                NonZero::new(4).expect("client capacity"),
                paths,
                &[],
                None,
            )
            .try_run(
                |result| result.expect("batch response").status().as_u16(),
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("client scenario");
            assert_eq!(codes.len(), 10);
            assert!(codes.iter().all(|&c| c == 200), "all 200: {codes:?}");
            server
                .stop()
                .expect("peer thread")
                .expect("stop long-sequence peer");
        },
    )
    .expect("spawn long-sequence peer")
}

#[test]
fn reuses_one_slot() {
    for flavor in FLAVORS {
        AcceptScript::with(
            move |acceptor| {
                peer::counted(
                    acceptor,
                    NonZero::new(2).expect("nonzero request budget"),
                    |conn, request| conn.try_final(request),
                )
            },
            |server| {
                let addr = server.addr();
                let codes = Batch::new(
                    flavor,
                    addr,
                    flavor
                        .try_config(addr)
                        .expect("HTTP host allocation")
                        .timeout(Duration::from_secs(2))
                        .inflight(1)
                        .request_pool(2, 1024),
                    NonZero::new(1).expect("client capacity"),
                    &["/first", "/second"],
                    &[],
                    None,
                )
                .try_run(
                    |result| result.expect("batch response").status().as_u16(),
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("client scenario");
                assert!((codes) == ([200, 200]));
                assert_eq!(
                    server
                        .finish()
                        .expect("peer thread")
                        .expect("one-slot peer"),
                    1
                );
            },
        )
        .expect("connection-counting peer")
    }
}

#[test]
fn rejects_excess_response_before_reusing_one_slot() {
    use common::client::runtime::pools;
    use dope_fiber::BlockOn as _;

    AcceptScript::with(
        |acceptor| {
            peer::counted(
                acceptor,
                NonZero::new(2).expect("request count"),
                |conn, request| conn.try_excess_response(request),
            )
        },
        |server| {
            let addr = server.addr();
            let codes = pools::Plan::new(
                addr,
                Config::try_new("127.0.0.1")
                    .expect("HTTP host allocation")
                    .timeout(Duration::from_secs(2))
                    .inflight(2)
                    .request_pool(2, 1024),
                1,
                1,
            )
            .expect("client plan")
            .enter(
                |mut app| {
                    app.wait().expect("active endpoint");
                    let client = app.handle;
                    app.app
                        .block_on(dope_fiber::fiber!('_ => async move {
                            let first = client.get("/first").await.expect("first response");
                            let second = client.get("/second").await.expect("second response");
                            [first.status().as_u16(), second.status().as_u16()]
                        }))
                        .expect("sequential requests")
                },
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("client scenario");
            assert_eq!(codes, [200, 201]);
            server
                .finish()
                .expect("peer thread")
                .expect("excess-response peer");
        },
    )
    .expect("connection-counting peer");
}

#[test]
fn http10_needs_persistence() {
    for flavor in FLAVORS {
        for (persist, expected) in [(false, 2), (true, 1)] {
            let count = AcceptScript::with(
                move |acceptor| {
                    peer::counted(
                        acceptor,
                        NonZero::new(2).expect("nonzero request budget"),
                        |conn, request| conn.try_http10(request, persist),
                    )
                },
                |server| {
                    let addr = server.addr();

                    let paths = &["/first", "/second"];
                    let codes = Batch::new(
                        flavor,
                        addr,
                        flavor.try_config(addr).expect("HTTP host allocation"),
                        NonZero::new(1).expect("client capacity"),
                        paths,
                        &[],
                        None,
                    )
                    .try_run(
                        |result| result.expect("batch response").status().as_u16(),
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("client scenario");
                    assert_eq!(codes, [200, 200]);
                    server
                        .finish()
                        .expect("peer thread")
                        .expect("HTTP/1.0 persistence probe")
                },
            )
            .expect("connection-counting peer");
            assert_eq!(count, expected);
        }
    }
}

#[test]
fn bounds_peer_timeout() {
    for flavor in FLAVORS {
        let count = AcceptScript::with(
            move |acceptor| {
                peer::counted(
                    acceptor,
                    NonZero::new(2).expect("nonzero request budget"),
                    |conn, request| conn.try_bounded(request),
                )
            },
            |server| {
                let addr = server.addr();

                let config = flavor
                    .try_config(addr)
                    .expect("HTTP host allocation")
                    .timeout(Duration::from_secs(2))
                    .idle_timeout(Duration::from_millis(100));
                let codes = Batch::new(
                    flavor,
                    addr,
                    config,
                    NonZero::new(1).expect("client capacity"),
                    &["/first"],
                    &["/second"],
                    Some(Duration::from_millis(300)),
                )
                .try_run(
                    |result| result.expect("batch response").status().as_u16(),
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("client scenario");
                assert_eq!(codes, [200, 200]);
                server
                    .finish()
                    .expect("peer thread")
                    .expect("reuse lease probe")
            },
        )
        .expect("connection-counting peer");
        assert_eq!(count, 2);
    }
}

#[test]
fn clock_governs_reuse() {
    for flavor in FLAVORS {
        for (gap, expected) in [
            (Duration::from_secs(10) - Duration::from_nanos(1), 1),
            (Duration::from_secs(10), 2),
        ] {
            let count = AcceptScript::with(
                move |acceptor| {
                    peer::counted(
                        acceptor,
                        NonZero::new(2).expect("nonzero request budget"),
                        |conn, request| conn.try_final(request),
                    )
                },
                |server| {
                    let addr = server.addr();
                    let codes = Batch::new(
                        flavor,
                        addr,
                        flavor
                            .try_config(addr)
                            .expect("HTTP host allocation")
                            .timeout(Duration::from_secs(1))
                            .idle_timeout(Duration::from_secs(10)),
                        NonZero::new(1).expect("client capacity"),
                        &["/first"],
                        &["/second"],
                        Some(gap),
                    )
                    .try_clock(
                        |result| result.expect("batch response").status().as_u16(),
                        recovery::Policy::new(
                            |event| std::assert_matches!(event, Reclaimed::Shutdown),
                            recover,
                        ),
                        |result| result.expect("client execution"),
                    )
                    .expect("client scenario");
                    assert!((codes) == ([200, 200]));
                    server
                        .finish()
                        .expect("peer thread")
                        .expect("manual-clock lease peer")
                },
            )
            .expect("connection-counting peer");
            assert_eq!(count, expected);
        }
    }
}

#[test]
fn zero_disables_reuse() {
    for flavor in FLAVORS {
        let count = AcceptScript::with(
            move |acceptor| {
                peer::counted(
                    acceptor,
                    NonZero::new(2).expect("nonzero request budget"),
                    |conn, request| conn.try_final(request),
                )
            },
            |server| {
                let addr = server.addr();
                let codes = Batch::new(
                    flavor,
                    addr,
                    flavor
                        .try_config(addr)
                        .expect("HTTP host allocation")
                        .timeout(Duration::from_secs(2))
                        .idle_timeout(Duration::ZERO),
                    NonZero::new(1).expect("client capacity"),
                    &["/first", "/second"],
                    &[],
                    None,
                )
                .try_run(
                    |result| result.expect("batch response").status().as_u16(),
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("client scenario");
                assert_eq!(codes, [200, 200]);
                server
                    .finish()
                    .expect("peer thread")
                    .expect("zero idle timeout probe")
            },
        )
        .expect("connection-counting peer");
        assert_eq!(count, 2);
    }
}

#[test]
fn missing_hint_resets() {
    for flavor in FLAVORS {
        let count = AcceptScript::with(
            move |acceptor| {
                peer::counted(
                    acceptor,
                    NonZero::new(3).expect("nonzero request budget"),
                    |conn, request| conn.try_reset(request),
                )
            },
            |server| {
                let addr = server.addr();

                let config = flavor
                    .try_config(addr)
                    .expect("HTTP host allocation")
                    .timeout(Duration::from_secs(3))
                    .idle_timeout(Duration::from_millis(1_800));
                let codes = Batch::new(
                    flavor,
                    addr,
                    config,
                    NonZero::new(1).expect("client capacity"),
                    &["/hint", "/reset"],
                    &["/after-gap"],
                    Some(Duration::from_millis(1_200)),
                )
                .try_run(
                    |result| result.expect("batch response").status().as_u16(),
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("client scenario");
                assert_eq!(codes, [200, 200, 200]);
                server
                    .finish()
                    .expect("peer thread")
                    .expect("sticky lease probe")
            },
        )
        .expect("connection-counting peer");
        assert_eq!(count, 1);
    }
}

#[test]
fn recovers_silent_close() {
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(
                acceptor,
                |stream: &mut &Stream<'_>, _: peer::Request<'_>| {
                    let response = "HTTP/1.1 200 OK".response(&[("Content-Length", "2")], b"ok");
                    stream.write_all(response.as_slice())
                },
            )
        },
        |server| {
            let addr = server.addr();

            let codes = Batch::new(
                Flavor::Fixed,
                addr,
                Config::try_new("127.0.0.1")
                    .expect("HTTP host allocation")
                    .timeout(Duration::from_secs(2))
                    .idle_timeout(Duration::from_secs(30)),
                NonZero::new(2).expect("client capacity"),
                &["/1", "/2", "/3", "/4", "/5", "/6", "/7", "/8"],
                &[],
                None,
            )
            .try_run(
                |result| result.expect("batch response").status().as_u16(),
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("client scenario");
            assert_eq!(codes, vec![200, 200, 200, 200, 200, 200, 200, 200]);
            server
                .stop()
                .expect("peer thread")
                .expect("stop silent-close peer");
        },
    )
    .expect("spawn silent-close peer")
}
