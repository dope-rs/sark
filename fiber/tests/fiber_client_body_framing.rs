use common::client::{
    config::{Config as _, FLAVORS},
    response::framing::{self, BODY, CHUNK, CHUNKS, SEGMENT},
    runtime::peer,
};
use dope_test::harness::peer::AcceptScript;
pub mod common;

use std::str;

use common::client::runtime::cases::{Flavor, events, request::Request};
use sark_fiber::client::config::Config;

#[test]
fn decodes_body() {
    peer::with_wire(&["/chunked"], framing::CHUNKED, |addr| {
        let resp = Request::get(
            Flavor::Fixed,
            addr,
            Config::try_new("127.0.0.1").expect("HTTP host allocation"),
            "/chunked",
        )
        .collect();
        assert_eq!(resp.status().as_u16(), 200);
        let body = str::from_utf8(resp.body().as_slice()).expect("utf8 body");
        assert_eq!(body, "hello, world");
    });
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
                Request::get(
                    flavor,
                    addr,
                    flavor.try_config(addr).expect("HTTP host allocation"),
                    "/scoped-chunked",
                )
                .try_scoped(|result| {
                    let response = result.expect("scoped response");
                    assert_eq!(response.status().as_u16(), 200);
                    assert_eq!(response.body().segments().len(), 2);
                    assert!(response.body().contiguous().is_none());
                    for (segment, expected) in response.body().segments().zip([b'a', b'b']) {
                        assert_eq!(segment.len(), SEGMENT);
                        assert!(segment.iter().all(|byte| *byte == expected));
                    }
                })
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

            let resp = Request::get(
                Flavor::Fixed,
                addr,
                Config::try_new("127.0.0.1").expect("HTTP host allocation"),
                "/big-chunked",
            )
            .collect();
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
            let response = Request::get(
                Flavor::Fixed,
                addr,
                config,
                "/single-large-chunk",
            )
            .collect();
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
    peer::with_wire(&["/chunked-trailers"], framing::TRAILERS, |addr| {
        let [resp, dynamic] = FLAVORS.map(|flavor| {
            Request::get(
                flavor,
                addr,
                flavor.try_config(addr).expect("HTTP host allocation"),
                "/chunked-trailers",
            )
            .collect()
        });
        for response in [&resp, &dynamic] {
            assert_eq!(response.status().as_u16(), 200);
            assert_eq!(response.body().as_slice(), b"abcdef");
            assert_eq!(
                response
                    .trailers()
                    .expect("chunked response trailers")
                    .get("x-checksum"),
                Some(b"42".as_slice())
            );
        }

        let observed = events::read(addr, "/chunked-trailers");
        assert_eq!(observed.status, 200);
        assert_eq!(observed.body, b"abcdef");
        assert_eq!(observed.trailers, 1);
        assert_eq!(observed.info, 0);
    });
}

#[test]
fn buffered_body_until_eof() {
    peer::with_wire(&["/eof"], framing::BUFFERED_EOF, |addr| {
        let response = Request::get(
            Flavor::Fixed,
            addr,
            Config::try_new("127.0.0.1").expect("HTTP host allocation"),
            "/eof",
        )
        .collect();
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(response.body().as_slice(), b"body until EOF");
    });
}

#[test]
fn streaming_body_until_eof() {
    peer::with_wire(&["/stream"], framing::STREAM_EOF, |addr| {
        let observed = events::read(addr, "/stream");
        assert_eq!(observed.status, 200);
        assert_eq!(observed.body, b"streamed body");
        assert_eq!(observed.trailers, 0);
        assert_eq!(observed.info, 0);
    });
}
