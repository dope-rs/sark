use common::client::assert_shutdown;
use common::client::runtime::peer;
use dope_test::harness::peer::AcceptScript;
pub mod common;

use std::{num::NonZero, time::Duration};

use common::client::{
    config::{Config as _, FLAVORS},
    pool::{self, SATURATION_LAST_RESPONSE as CLOSE, SATURATION_RESPONSE as OK},
    runtime::cases::Flavor,
};
use sark_fiber::client::config::Config;

#[test]
fn survives_peer_close() {
    let short: &[&str] = &["/1", "/2", "/3", "/4", "/5", "/6"];
    let long: &[&str] = &["/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h", "/i", "/j"];
    let done: &[u8] = b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 4\r\n\r\ndone";
    for (paths, capacity, reply) in [(short, 2, CLOSE), (long, 4, done)] {
        AcceptScript::with(
            |acceptor| {
                peer::serve_each(acceptor, |stream, request| {
                    assert_eq!(request.method(), "GET");
                    stream.write_all(reply)
                })
            },
            |server| {
                let codes = Flavor::Fixed
                    .try_gets(
                        server.addr(),
                        Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                        NonZero::new(capacity).expect("client capacity"),
                        paths,
                        &[],
                        None,
                    )
                    .expect("client scenario");
                assert_eq!(codes.len(), paths.len());
                assert!(codes.iter().all(|&code| code == 200), "{codes:?}");
                server
                    .stop()
                    .expect("peer thread")
                    .expect("close-after-response peer");
            },
        )
        .expect("spawn close-after-response peer");
    }
}

#[test]
fn reuses_one_slot() {
    for flavor in FLAVORS {
        let count = peer::counted(&[OK, CLOSE], |addr| {
            let codes = flavor
                .try_gets(
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
                .expect("client scenario");
            assert_eq!(codes, [200, 200]);
        });
        assert_eq!(count, 1);
    }
}

#[test]
fn rejects_excess_response_before_reusing_one_slot() {
    use common::client::runtime::pools;
    use dope_fiber::BlockOn as _;

    peer::counted(
        &[
            &pool::excess_response(),
            b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ],
        |addr| {
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
                (),
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
                assert_shutdown,
            )
            .expect("client scenario");
            assert_eq!(codes, [200, 201]);
        },
    );
}

#[test]
fn http10_needs_persistence() {
    for flavor in FLAVORS {
        for (persist, expected) in [(false, 2), (true, 1)] {
            let first: &[u8] = if persist {
                b"HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nContent-Length: 2\r\n\r\nok"
            } else {
                b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok"
            };
            let count = peer::counted(
                &[
                    first,
                    b"HTTP/1.0 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok",
                ],
                |addr| {
                    let paths = &["/first", "/second"];
                    let codes = flavor
                        .try_gets(
                            addr,
                            flavor.try_config(addr).expect("HTTP host allocation"),
                            NonZero::new(1).expect("client capacity"),
                            paths,
                            &[],
                            None,
                        )
                        .expect("client scenario");
                    assert_eq!(codes, [200, 200]);
                },
            );
            assert_eq!(count, expected);
        }
    }
}

#[test]
fn bounds_peer_timeout() {
    for flavor in FLAVORS {
        let count = peer::counted(&[b"HTTP/1.1 200 OK\r\nKeep-Alive: timeout=18446744073709551615\r\nContent-Length: 2\r\n\r\nok", CLOSE], |addr| {
            let config = flavor
                .try_config(addr)
                .expect("HTTP host allocation")
                .timeout(Duration::from_secs(2))
                .idle_timeout(Duration::from_millis(100));
            let codes = flavor.try_gets(
                addr,
                config,
                NonZero::new(1).expect("client capacity"),
                &["/first"],
                &["/second"],
                Some(Duration::from_millis(300)),
            )
            .expect("client scenario");
            assert_eq!(codes, [200, 200]);
        });
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
            let count = peer::counted(&[OK, CLOSE], |addr| {
                let codes = flavor
                    .try_timed_pair(
                        addr,
                        flavor
                            .try_config(addr)
                            .expect("HTTP host allocation")
                            .timeout(Duration::from_secs(1))
                            .idle_timeout(Duration::from_secs(10)),
                        gap,
                    )
                    .expect("client scenario");
                assert_eq!(codes, [200, 200]);
            });
            assert_eq!(count, expected);
        }
    }
}

#[test]
fn zero_disables_reuse() {
    for flavor in FLAVORS {
        let count = peer::counted(&[OK, CLOSE], |addr| {
            let codes = flavor
                .try_gets(
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
                .expect("client scenario");
            assert_eq!(codes, [200, 200]);
        });
        assert_eq!(count, 2);
    }
}

#[test]
fn missing_hint_resets() {
    for flavor in FLAVORS {
        let count = peer::counted(
            &[
                b"HTTP/1.1 200 OK\r\nKeep-Alive: timeout=2\r\nContent-Length: 2\r\n\r\nok",
                OK,
                CLOSE,
            ],
            |addr| {
                let config = flavor
                    .try_config(addr)
                    .expect("HTTP host allocation")
                    .timeout(Duration::from_secs(3))
                    .idle_timeout(Duration::from_millis(1_800));
                let codes = flavor
                    .try_gets(
                        addr,
                        config,
                        NonZero::new(1).expect("client capacity"),
                        &["/hint", "/reset"],
                        &["/after-gap"],
                        Some(Duration::from_millis(1_200)),
                    )
                    .expect("client scenario");
                assert_eq!(codes, [200, 200, 200]);
            },
        );
        assert_eq!(count, 1);
    }
}

#[test]
fn recovers_silent_close() {
    peer::with_wire(&[], OK, |addr| {
        let codes = Flavor::Fixed
            .try_gets(
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
            .expect("client scenario");
        assert_eq!(codes, [200; 8]);
    });
}
