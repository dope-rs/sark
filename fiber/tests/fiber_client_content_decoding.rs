use common::client::recover;
use common::client::runtime::peer;
use dope::core::{driver::life::recovery, io::Reclaimed};
use dope_test::harness::peer::{AcceptScript, Stream};
pub mod common;

use std::str;

use common::client::{config::FLAVORS, gzip, runtime::cases::request::Request};
use sark_client::{Error, Safety, reply::decode::Policy};
use sark_fiber::client::config::Config;
use sark_http::{method::Method, payload::compress::inflate};

#[test]
fn decodes_gzip_body() {
    let payload = "decodedx";
    let encoded = [gzip::DECODED, gzip::X, gzip::EMPTY].concat();
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(acceptor, move |stream: &mut &Stream<'_>, _: peer::Request<'_>| {
                let mut resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    encoded.len()
                )
                .into_bytes();
                resp.extend_from_slice(&encoded);
                stream.write_all(&resp)
            })
        },
        |server| {
            for flavor in FLAVORS {
                let resp = flavor.try_get(&server.addr().to_string(), Policy::Strict, "/").expect("HTTP host allocation").try_run(|result| result.expect("HTTP response"), recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ), |result| result.expect("client execution")).expect("request scope");
                assert_eq!(resp.status().as_u16(), 200);
                assert_eq!(
                    str::from_utf8(resp.body().as_slice()).expect("decoder returns source UTF-8"),
                    payload
                );
                assert!(resp.headers().get("content-encoding").is_none());
                assert!(resp.headers().get("content-length").is_none());
            }
            server.stop().expect("peer thread").expect("stop gzip peer");
        },
    )
    .expect("spawn gzip peer");
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
                let response = Request::new(
                    flavor,
                    &server.addr().to_string(),
                    config(),
                    Method::GET,
                    "/fragmented",
                    &[],
                    &[],
                )
                .try_run(|result| result.expect("HTTP response"), recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ), |result| result.expect("client execution"))
                .expect("HTTP request execution");
                assert_eq!(response.body().as_slice(), payload);
                assert!(response.headers().get("content-encoding").is_none());
                Request::new(
                    flavor,
                    &server.addr().to_string(),
                    config(),
                    Method::GET,
                    "/checksum",
                    &[],
                    &[],
                )
                .try_run(|result| {
                    let failure = result.err().expect("corrupt fragmented gzip");
                    assert!(matches!(failure.cause(),
                        Error::Decompression(inflate::DecodeError::Stream(cause))
                            if cause.error() == (inflate::Error::Checksum { expected: crc ^ 1, actual: crc })
                    ));
                }, recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ), |result| result.expect("client execution"))
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
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(acceptor, |stream: &mut &Stream<'_>, _: peer::Request<'_>| {
                let body = b"not actually gzip";
                let mut resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .into_bytes();
                resp.extend_from_slice(body);
                stream.write_all(&resp)
            })
        },
        |server| {
            for flavor in FLAVORS {
                Request::new(
                    flavor,
                    &server.addr().to_string(),
                    Config::new(server.addr().to_string()).decode(Policy::Strict),
                    Method::GET,
                    "/",
                    &[],
                    &[],
                )
                .try_run(|outcome| {
                    let failure = (outcome).err().expect("strict must reject invalid gzip");
                    assert!((failure.safety()) == (Safety::PossiblyCommitted));
                    assert!(matches!(
                        failure.cause(),
                        Error::Decompression(inflate::DecodeError::Stream(cause))
                            if matches!(cause.error(), inflate::Error::Header {
                                cause: inflate::HeaderFault::Magic { actual: [b'n', b'o'] },
                                ..
                            })
                    ));
                }, recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ), |result| result.expect("client execution"))
                .expect("HTTP request execution");
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop invalid-gzip peer");
        },
    )
    .expect("spawn invalid-gzip peer");
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
                Request::new(
                    flavor,
                    &server.addr().to_string(),
                    Config::new(server.addr().to_string()),
                    Method::GET,
                    "/",
                    &[],
                    &[],
                )
                .try_run(|result| {
                    let failure = result.err().expect("false zero gzip size");
                    assert!(matches!(failure.cause(),
                        Error::Decompression(inflate::DecodeError::Stream(cause))
                            if matches!(cause.error(), inflate::Error::Length { expected: 0, actual: 1 })
                    ));
                }, recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ), |result| result.expect("client execution"))
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
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(acceptor, move |stream: &mut &Stream<'_>, _: peer::Request<'_>| {
                let mut resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    encoded.len()
                )
                .into_bytes();
                resp.extend_from_slice(&encoded);
                stream.write_all(&resp)
            })
        },
        |server| {
            for flavor in FLAVORS {
                let config = Config::new(server.addr().to_string())
                    .decode(Policy::Strict)
                    .plain_limit(4096);
                Request::new(
                    flavor,
                    &server.addr().to_string(),
                    config,
                    Method::GET,
                    "/",
                    &[],
                    &[],
                )
                .try_run(|outcome| {
                    let failure = (outcome)
                        .err()
                        .expect("decompression bomb must be rejected");
                    assert!(matches!(
                        failure.cause(),
                        Error::Decompression(inflate::DecodeError::SizeLimit { actual, limit })
                            if *actual == 1024 * 1024 && *limit == 4096
                    ));
                }, recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ), |result| result.expect("client execution"))
                .expect("HTTP request execution");
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop decompression-bomb peer");
        },
    )
    .expect("spawn decompression-bomb peer");
}

#[test]
fn honors_decode_memory() {
    let plain_len = 4096;
    let encoded = gzip::REPEATED;
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(acceptor, move |stream: &mut &Stream<'_>, _: peer::Request<'_>| {
                let mut resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    encoded.len()
                )
                .into_bytes();
                resp.extend_from_slice(encoded);
                stream.write_all(&resp)
            })
        },
        |server| {
            for flavor in FLAVORS {
                let strict = Config::new(server.addr().to_string())
                    .decode(Policy::Strict)
                    .plain_limit(plain_len)
                    .plain_memory(1024);
                Request::new(
                    flavor,
                    &server.addr().to_string(),
                    strict,
                    Method::GET,
                    "/",
                    &[],
                    &[],
                )
                .try_run(|outcome| {
                    let failure = (outcome).err().expect("strict decoded capacity must fail");
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
                }, recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ), |result| result.expect("client execution"))
                .expect("HTTP request execution");

                let lenient = Config::new(server.addr().to_string())
                    .decode(Policy::Lenient)
                    .plain_limit(plain_len)
                    .plain_memory(1024);
                let response = Request::new(
                    flavor,
                    &server.addr().to_string(),
                    lenient,
                    Method::GET,
                    "/",
                    &[],
                    &[],
                )
                .try_run(|result| result.expect("HTTP response"), recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ), |result| result.expect("client execution"))
                .expect("HTTP request execution");
                assert_eq!(response.body().as_slice(), encoded);
                assert_eq!(
                    response.headers().get("content-encoding"),
                    Some(b"gzip".as_slice())
                );
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop decoded-capacity peer");
        },
    )
    .expect("spawn decoded-capacity peer");
}

#[test]
fn lenient_keeps_bad_gzip() {
    let raw = b"not actually gzip";
    AcceptScript::with(
        move |acceptor| {
            peer::serve_each(acceptor, move |stream: &mut &Stream<'_>, _: peer::Request<'_>| {
                let mut resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    raw.len()
                )
                .into_bytes();
                resp.extend_from_slice(raw);
                stream.write_all(&resp)
            })
        },
        |server| {
            for flavor in FLAVORS {
                let resp = flavor.try_get(&server.addr().to_string(), Policy::Lenient, "/").expect("HTTP host allocation").try_run(|result| result.expect("HTTP response"), recovery::Policy::new(
                    |event| std::assert_matches!(event, Reclaimed::Shutdown),
                    recover,
                ), |result| result.expect("client execution")).expect("request scope");
                assert_eq!(resp.status().as_u16(), 200);
                assert_eq!(resp.body().as_slice(), raw);
            }
            server
                .stop()
                .expect("peer thread")
                .expect("stop lenient-gzip peer");
        },
    )
    .expect("spawn lenient-gzip peer");
}
