use common::client::runtime::peer;
use dope_test::harness::peer::{AcceptScript, Stream};
pub mod common;

use std::str;

use common::client::{config::FLAVORS, gzip, runtime::cases::request::Request};
use sark_client::{Error, Safety, reply::decode::Policy};
use sark_fiber::client::config::Config;
use sark_http::payload::compress::inflate;

const GZIP: &str = "Content-Encoding: gzip\r\n";

#[test]
fn decodes_gzip_body() {
    let payload = "decodedx";
    let encoded = [gzip::DECODED, gzip::X, gzip::EMPTY].concat();
    peer::with_body(GZIP, &encoded, |addr| {
        for flavor in FLAVORS {
            let resp = Request::get(
                flavor,
                addr,
                Config::new(addr.to_string()).decode(Policy::Strict),
                "/",
            )
            .collect();
            assert_eq!(resp.status().as_u16(), 200);
            assert_eq!(
                str::from_utf8(resp.body().as_slice()).expect("decoder returns source UTF-8"),
                payload
            );
            assert!(resp.headers().get("content-encoding").is_none());
            assert!(resp.headers().get("content-length").is_none());
        }
    });
}

#[test]
fn decodes_fragmented_gzip() {
    let (payload, encoded) = gzip::fragmented();
    let wire_len = encoded.len();
    let crc = u32::from_le_bytes(
        encoded[wire_len - 8..wire_len - 4]
            .try_into()
            .expect("CRC bytes"),
    );
    let mut corrupt = encoded.clone();
    corrupt[wire_len - 8] ^= 1;
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(acceptor, move |stream: &mut &Stream<'_>, request: peer::Request<'_>| {
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {wire_len}\r\nConnection: close\r\n\r\n"
                );
                stream.write_all(head.as_bytes())?;
                let body = if request.target() == "/checksum" { &corrupt } else { &encoded };
                for segment in body.chunks(4093) {
                    stream.write_all(segment)?;
                }
                Ok(())
            })
        },
        |server| {
            for flavor in FLAVORS {
                let config = || {
                    Config::new(server.addr().to_string())
                        .decode(Policy::Strict)
                        .wire_limit(wire_len)
                        .wire_memory(wire_len + 4096)
                        .plain_limit(payload.len())
                        .plain_memory(payload.len())
                };
                let response = Request::get(
                    flavor,
                    server.addr(),
                    config(),
                    "/fragmented",
                )
                .collect();
                assert_eq!(response.body().as_slice(), payload);
                assert!(response.headers().get("content-encoding").is_none());
                Request::get(
                    flavor,
                    server.addr(),
                    config(),
                    "/checksum",
                )
                .try_run(|result| {
                    let failure = result.err().expect("corrupt fragmented gzip");
                    assert!(matches!(failure.cause(),
                        Error::Decompression(inflate::DecodeError::Stream(cause))
                            if cause.error() == (inflate::Error::Checksum { expected: crc ^ 1, actual: crc })
                    ));
                })
                .expect("corrupt fragmented-gzip execution");
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop fragmented-gzip peer");
        },
    )
    .expect("spawn fragmented-gzip peer");
}

#[test]
fn strict_rejects_bad_gzip() {
    peer::with_body(GZIP, b"not actually gzip", |addr| {
        for flavor in FLAVORS {
            Request::get(
                flavor,
                addr,
                Config::new(addr.to_string()).decode(Policy::Strict),
                "/",
            )
            .try_run(|outcome| {
                let failure = outcome.err().expect("strict must reject invalid gzip");
                assert_eq!(failure.safety(), Safety::PossiblyCommitted);
                assert!(matches!(
                    failure.cause(),
                    Error::Decompression(inflate::DecodeError::Stream(cause))
                        if matches!(cause.error(), inflate::Error::Header {
                            cause: inflate::HeaderFault::Magic { actual: [b'n', b'o'] },
                            ..
                        })
                ));
            })
            .expect("HTTP request execution");
        }
    });
}

#[test]
fn rejects_false_zero_size() {
    let mut encoded = gzip::X.to_vec();
    let footer = encoded.len() - 4;
    encoded[footer..].fill(0);
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(acceptor, move |stream: &mut &Stream<'_>, _: peer::Request<'_>| {
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        encoded.len(),
                    );
                    stream.write_all(head.as_bytes())?;
                    stream.write_all(&encoded)
            })
        },
        |server| {
            for flavor in FLAVORS {
                Request::get(
                    flavor,
                    server.addr(),
                    Config::new(server.addr().to_string()),
                    "/",
                )
                .try_run(|result| {
                    let failure = result.err().expect("false zero gzip size");
                    assert!(matches!(failure.cause(),
                        Error::Decompression(inflate::DecodeError::Stream(cause))
                            if matches!(cause.error(), inflate::Error::Length { expected: 0, actual: 1 })
                    ));
                })
                .expect("zero-size gzip execution");
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop zero-size gzip peer");
        },
    )
    .expect("spawn zero-size gzip peer");
}

#[test]
fn rejects_decode_bomb() {
    let encoded = gzip::bomb();
    peer::with_body(GZIP, &encoded, |addr| {
        for flavor in FLAVORS {
            let config = Config::new(addr.to_string())
                .decode(Policy::Strict)
                .plain_limit(4096);
            Request::get(flavor, addr, config, "/")
                .try_run(|outcome| {
                    let failure = outcome.err().expect("decompression bomb must be rejected");
                    assert!(matches!(
                        failure.cause(),
                        Error::Decompression(inflate::DecodeError::SizeLimit { actual, limit })
                            if *actual == 1024 * 1024 && *limit == 4096
                    ));
                })
                .expect("HTTP request execution");
        }
    });
}

#[test]
fn honors_decode_memory() {
    let plain_len = 4096;
    let encoded = gzip::REPEATED;
    peer::with_body(GZIP, encoded, |addr| {
        for flavor in FLAVORS {
            let strict = Config::new(addr.to_string())
                .decode(Policy::Strict)
                .plain_limit(plain_len)
                .plain_memory(1024);
            Request::get(flavor, addr, strict, "/")
                .try_run(|outcome| {
                    let failure = outcome.err().expect("strict decoded capacity must fail");
                    assert!(matches!(
                        failure.cause(),
                        Error::Decompression(inflate::DecodeError::Allocation {
                            requested,
                            available,
                            cause: o3::buffer::resident::AllocError::Budget(cause),
                        }) if *requested == plain_len && *available == 1024
                            && cause.into_parts().0.get() == *requested
                            && cause.into_parts().1 == *available
                    ));
                })
                .expect("HTTP request execution");

            let lenient = Config::new(addr.to_string())
                .decode(Policy::Lenient)
                .plain_limit(plain_len)
                .plain_memory(1024);
            let response = Request::get(flavor, addr, lenient, "/").collect();
            assert_eq!(response.body().as_slice(), encoded);
            assert_eq!(
                response.headers().get("content-encoding"),
                Some(b"gzip".as_slice())
            );
        }
    });
}

#[test]
fn lenient_keeps_bad_gzip() {
    let raw = b"not actually gzip";
    peer::with_body(GZIP, raw, |addr| {
        for flavor in FLAVORS {
            let resp = Request::get(
                flavor,
                addr,
                Config::new(addr.to_string()).decode(Policy::Lenient),
                "/",
            )
            .collect();
            assert_eq!(resp.status().as_u16(), 200);
            assert_eq!(resp.body().as_slice(), raw);
        }
    });
}
