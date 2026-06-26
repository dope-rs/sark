//! End-to-end regression tests for the server request assembler.
//!
//! These drive the real `serve` listener over loopback with a `Conn<ClientRole>`
//! client and exercise the three failure modes the assembler must handle:
//!   1. a bodied request must reach the handler with its full header list,
//!   2. a body split across multiple DATA frames must arrive intact,
//!   3. a request terminated by trailers must be dispatched (not hang).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use dope::fiber::Fiber;
use dope_extra::testing::{ephemeral_addr, run_with_trigger};
use o3::buffer::Shared;
use sark_h2::hpack::OwnedHeader;
use sark_h2::server::{Cfg, Handler, Request, Response, serve};
use sark_h2::{ClientRole, Conn, Header, StreamId, conn};

/// Echoes the request body back and reports, via response headers, whether the
/// request's `:method`/`:path` were seen — so the client can assert the head
/// list survived assembly.
struct Echo;

impl Handler for Echo {
    type Fut<'h> = std::future::Ready<Response>;

    fn on_request<'h>(&'h self, req: Request) -> Fiber<'h, Self::Fut<'h>> {
        let method = req
            .headers
            .iter()
            .find(|h| h.name == b":method")
            .map(|h| h.value.clone())
            .unwrap_or_default();
        let has_path = req.headers.iter().any(|h| h.name == b":path");
        let headers = vec![
            OwnedHeader::new(b":status", b"200"),
            OwnedHeader::new(b"x-method", &method),
            OwnedHeader::new(b"x-has-path", if has_path { b"1" } else { b"0" }),
        ];
        Fiber::new(std::future::ready(Response::new(
            headers,
            Shared::from(req.body),
        )))
    }
}

fn flush(stream: &mut TcpStream, client: &mut Conn<ClientRole>) {
    let out = client.outbound();
    if out.is_empty() {
        return;
    }
    let owned = out.to_vec();
    stream.write_all(&owned).expect("client write");
    client.drain_outbound(owned.len());
}

fn send_all(client: &mut Conn<ClientRole>, sid: StreamId, data: &[u8], end_stream: bool) {
    let mut off = 0;
    loop {
        let n = client
            .send_data(sid, &data[off..], end_stream)
            .expect("send_data");
        off += n;
        if off >= data.len() {
            break;
        }
    }
}

fn request_headers(path: &[u8]) -> [Header<'_>; 4] {
    [
        Header {
            name: b":method",
            value: b"POST",
        },
        Header {
            name: b":scheme",
            value: b"http",
        },
        Header {
            name: b":path",
            value: path,
        },
        Header {
            name: b":authority",
            value: b"localhost",
        },
    ]
}

/// Open a client connection, let `build` emit one request, then read the
/// response, returning its header list and assembled body.
fn round_trip(
    addr: SocketAddr,
    build: impl FnOnce(&mut Conn<ClientRole>) -> StreamId,
) -> (Vec<OwnedHeader>, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("read timeout");
    let mut client = Conn::<ClientRole>::new();
    let sid = build(&mut client);
    flush(&mut stream, &mut client);

    let mut headers = Vec::new();
    let mut body = Vec::new();
    let mut buf = [0u8; 16 * 1024];
    'outer: loop {
        let n = match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        client.ingest(&buf[..n]).expect("client ingest");
        while let Some(ev) = client.poll_event() {
            match ev {
                conn::Event::Headers {
                    stream_id,
                    headers: h,
                    end_stream,
                    ..
                } if stream_id == sid => {
                    headers = h;
                    if end_stream {
                        break 'outer;
                    }
                }
                conn::Event::Data {
                    stream_id,
                    data,
                    end_stream,
                } if stream_id == sid => {
                    body.extend_from_slice(&data);
                    let _ = client.release_capacity(stream_id, data.len());
                    if end_stream {
                        break 'outer;
                    }
                }
                _ => {}
            }
        }
        flush(&mut stream, &mut client);
    }
    (headers, body)
}

fn header_value<'a>(headers: &'a [OwnedHeader], name: &[u8]) -> Option<&'a [u8]> {
    headers
        .iter()
        .find(|h| h.name == name)
        .map(|h| h.value.as_slice())
}

#[test]
fn bodied_request_delivers_headers_and_full_body() {
    let bind = ephemeral_addr();
    let cfg = Cfg {
        bind,
        max_conn: 64,
        backlog: 128,
    };
    run_with_trigger(
        bind,
        move |ctx, trigger| serve(Echo, cfg.clone(), ctx, Some(trigger)),
        |addr| {
            // 20 KiB body sent as two DATA frames; the first is non-terminal so
            // the old "dispatch on the final DATA only" path would lose it.
            let payload: Vec<u8> = (0..20_000).map(|i| (i % 251) as u8).collect();
            let expected = payload.clone();
            let (headers, body) = round_trip(addr, move |client| {
                let sid = client
                    .start_request(&request_headers(b"/echo"), false)
                    .expect("start_request");
                let mid = payload.len() / 2;
                send_all(client, sid, &payload[..mid], false);
                send_all(client, sid, &payload[mid..], true);
                sid
            });

            assert_eq!(header_value(&headers, b":status"), Some(&b"200"[..]));
            assert_eq!(
                header_value(&headers, b"x-method"),
                Some(&b"POST"[..]),
                "request header list must survive assembly"
            );
            assert_eq!(header_value(&headers, b"x-has-path"), Some(&b"1"[..]));
            assert_eq!(body, expected, "multi-frame body must arrive intact");
        },
    );
}

#[test]
fn trailers_terminate_request() {
    let bind = ephemeral_addr();
    let cfg = Cfg {
        bind,
        max_conn: 64,
        backlog: 128,
    };
    run_with_trigger(
        bind,
        move |ctx, trigger| serve(Echo, cfg.clone(), ctx, Some(trigger)),
        |addr| {
            let (headers, body) = round_trip(addr, |client| {
                let sid = client
                    .start_request(&request_headers(b"/trailer"), false)
                    .expect("start_request");
                send_all(client, sid, b"trailer-terminated-body", false);
                client
                    .send_trailers(
                        sid,
                        &[Header {
                            name: b"x-checksum",
                            value: b"deadbeef",
                        }],
                    )
                    .expect("send_trailers");
                sid
            });

            assert_eq!(header_value(&headers, b":status"), Some(&b"200"[..]));
            assert_eq!(header_value(&headers, b"x-method"), Some(&b"POST"[..]));
            assert_eq!(
                body, b"trailer-terminated-body",
                "a trailer-terminated request must be dispatched with its body"
            );
        },
    );
}
