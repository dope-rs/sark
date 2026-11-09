use common::client::recover;
use common::client::{
    config::{Config as _, FLAVORS},
    response::framing::{self, BODY, CHUNK, CHUNKS, SEGMENT},
    runtime::peer,
};
use dope::core::{driver::life::recovery, io::Reclaimed};
use dope_test::harness::peer::AcceptScript;
pub mod common;

use std::str;

use common::client::runtime::cases::{Flavor, events::Reader, request::Request};
use sark_fiber::client::config::Config;
use sark_http::method::Method;

#[test]
fn decodes_body() {
    AcceptScript::with(
        |acceptor| {
            peer::serve_each(acceptor, |stream, request| {
                assert_eq!(request.target(), "/chunked");
                framing::chunked(stream)
            })
        },
        |server| {
            let addr = server.addr();

            let resp = Request::new(
                Flavor::Fixed,
                &(addr).to_string(),
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                Method::GET,
                "/chunked",
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
            assert_eq!(resp.status().as_u16(), 200);
            let body = str::from_utf8(resp.body().as_slice()).expect("utf8 body");
            assert_eq!(body, "hello, world");
            server
                .stop()
                .expect("peer thread")
                .expect("stop chunked peer");
        },
    )
    .expect("spawn chunked peer");
}

#[test]
fn preserves_segments() {
    AcceptScript::with(
        |acceptor| {
            peer::serve_each(acceptor, |stream, request| {
                assert_eq!(request.target(), "/scoped-chunked");
                framing::segmented(stream)
            })
        },
        |server| {
            let addr = server.addr();

            for flavor in FLAVORS {
                Request::new(
                    flavor,
                    &addr.to_string(),
                    flavor.try_config(addr).expect("HTTP host allocation"),
                    Method::GET,
                    "/scoped-chunked",
                    &[],
                    &[],
                )
                .try_scoped(
                    |result| {
                        let response = result.expect("scoped response");
                        assert_eq!(response.status().as_u16(), 200);
                        assert_eq!(response.body().segments().len(), 2);
                        assert!(response.body().contiguous().is_none());
                        for (segment, expected) in response.body().segments().zip([b'a', b'b']) {
                            assert_eq!(segment.len(), SEGMENT);
                            assert!(segment.iter().all(|byte| *byte == expected));
                        }
                    },
                    recovery::Policy::new(
                        |event| std::assert_matches!(event, Reclaimed::Shutdown),
                        recover,
                    ),
                    |result| result.expect("client execution"),
                )
                .expect("client scenario");
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop segmented peer");
        },
    )
    .expect("spawn segmented peer");
}

#[test]
fn decodes_large_body() {
    AcceptScript::with(
        |acceptor| {
            peer::serve_each(acceptor, |stream, request| {
                assert_eq!(request.target(), "/big-chunked");
                framing::large(stream)
            })
        },
        |server| {
            let addr = server.addr();

            let resp = Request::new(
                Flavor::Fixed,
                &(addr).to_string(),
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                Method::GET,
                "/big-chunked",
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
            assert_eq!(resp.status().as_u16(), 200);
            assert_eq!(
                resp.body().as_slice().len(),
                CHUNK * CHUNKS,
                "full chunked body reassembled"
            );
            assert!(
                resp.body().as_slice().iter().all(|&b| b == b'x'),
                "body intact"
            );
            server
                .stop()
                .expect("peer thread")
                .expect("stop large-chunk peer");
        },
    )
    .expect("spawn large-chunk peer");
}

#[test]
fn frames_large_chunk() {
    AcceptScript::with(
        |acceptor| {
            peer::serve_each(acceptor, |stream, request| {
                assert_eq!(request.target(), "/single-large-chunk");
                framing::single(stream)
            })
        },
        |server| {
            let addr = server.addr();

            let config = Config::try_new("127.0.0.1")
                .expect("HTTP host allocation")
                .wire_limit(BODY);
            let response = Request::new(
                Flavor::Fixed,
                &(addr).to_string(),
                config,
                Method::GET,
                "/single-large-chunk",
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
            assert_eq!(response.body().as_slice().len(), BODY);
            assert!(response.body().as_slice().iter().all(|byte| *byte == b'z'));
            server
                .stop()
                .expect("peer thread")
                .expect("stop single-chunk peer");
        },
    )
    .expect("spawn single-chunk peer");
}

#[test]
fn decodes_trailers() {
    AcceptScript::with(
        |acceptor| {
            peer::serve_each(acceptor, |stream, request| {
                assert_eq!(request.target(), "/chunked-trailers");
                framing::trailers(stream)
            })
        },
        |server| {
            let addr = server.addr();

            let [resp, dynamic] = FLAVORS.map(|flavor| {
                Request::new(
                    flavor,
                    &addr.to_string(),
                    flavor.try_config(addr).expect("HTTP host allocation"),
                    Method::GET,
                    "/chunked-trailers",
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
                .expect("HTTP request execution")
            });
            for response in [&resp, &dynamic] {
                assert_eq!(response.status().as_u16(), 200);
                assert_eq!(response.body().as_slice(), b"abcdef");
                assert_eq!(
                    response.trailers().expect("chunked response trailers").get("x-checksum"),
                    Some(b"42".as_slice())
                );
            }

            let (status, body, trailers, info) = Reader::new(
                addr,
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                "/chunked-trailers",
            )
            .try_run(
                |result| {
                    let read = result.expect("client scenario");
                    (read.status, read.body, read.trailers, read.info)
                },
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("client scenario");
            assert_eq!(status, 200);
            assert_eq!(body, b"abcdef");
            assert_eq!(trailers, 1);
            assert_eq!(info, 0);
            server
                .stop()
                .expect("peer thread")
                .expect("stop trailer peer");
        },
    )
    .expect("spawn trailer peer");
}

#[test]
fn buffered_body_until_eof() {
    AcceptScript::with(
        |acceptor| {
            peer::serve_each(acceptor, |stream, request| {
                assert_eq!(request.target(), "/eof");
                framing::buffered_eof(stream)
            })
        },
        |server| {
            let addr = server.addr();

            let response = Request::new(
                Flavor::Fixed,
                &(addr).to_string(),
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                Method::GET,
                "/eof",
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
            assert_eq!(response.body().as_slice(), b"body until EOF");
            server.stop().expect("peer thread").expect("stop EOF peer");
        },
    )
    .expect("spawn EOF peer");
}

#[test]
fn streaming_body_until_eof() {
    AcceptScript::with(
        |acceptor| {
            peer::serve_each(acceptor, |stream, request| {
                assert_eq!(request.target(), "/stream");
                framing::stream_eof(stream)
            })
        },
        |server| {
            let addr = server.addr();

            let (status, body, trailers, info) = Reader::new(
                addr,
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                "/stream",
            )
            .try_run(
                |result| {
                    let read = result.expect("client scenario");
                    (read.status, read.body, read.trailers, read.info)
                },
                recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ),
                |result| result.expect("client execution"),
            )
            .expect("client scenario");
            assert_eq!(status, 200);
            assert_eq!(body, b"streamed body");
            assert_eq!(trailers, 0);
            assert_eq!(info, 0);
            server
                .stop()
                .expect("peer thread")
                .expect("stop streaming EOF peer");
        },
    )
    .expect("spawn streaming EOF peer");
}
