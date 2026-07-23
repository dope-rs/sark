#![allow(clippy::too_many_arguments)]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_extra::harness::Harness;
use http::StatusCode;
use sark::{Executor, Throughput, driver};

#[sark_gen::request]
struct HelloRequest {}

#[sark_gen::response(raw)]
struct HelloReply {
    status: StatusCode,
    body: Vec<u8>,
}

#[sark_gen::handler]
fn hello(_req: HelloRequest, _state: &()) -> HelloReply {
    let mut body = Vec::new();
    body.extend_from_slice(b"hello");
    HelloReply {
        status: StatusCode::OK,
        body,
    }
}

sark_gen::define_route! {
    SmokeDispatch: () => {
        GET "/hello" => hello,
    }
}

fn http_get_close(addr: std::net::SocketAddr, path: &str) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set timeout");
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).expect("send request");
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(body_start) = find_double_crlf(&buf)
                    && let Some(cl) = content_length(&buf[..body_start])
                    && buf.len() >= body_start + cl
                {
                    break;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("read response: {e}"),
        }
    }
    buf
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

fn content_length(headers: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(headers).ok()?;
    for line in text.split("\r\n") {
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            return value.trim().parse().ok();
        }
    }
    None
}

#[test]
fn server_dispatches_get_hello() {
    let bind: std::net::SocketAddr = "127.0.0.1:38765".parse().unwrap();
    let server = support::http_server(bind, Duration::from_secs(10));

    Harness::new(bind)
        .run_with_trigger(
            |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?;
                executor.enter(|mut session| {
                    let timer =
                        sark::Timer::with_capacity(support::MAX_CONNECTIONS.saturating_mul(2));
                    server.clone().serve(
                        &mut session,
                        SmokeDispatch::new(
                            &(),
                            &timer,
                            sark::app::Config {
                                timer_capacity: support::MAX_CONNECTIONS.saturating_mul(2),
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let raw = http_get_close(bind, "/hello");
                let text = std::str::from_utf8(&raw).expect("utf8 response");

                assert!(text.starts_with("HTTP/1.1 200 "), "status line: {text:?}");
                let body_start = text
                    .find("\r\n\r\n")
                    .expect("blank line separating headers from body")
                    + 4;
                assert_eq!(&text[body_start..], "hello", "body mismatch: {text:?}");
            },
        )
        .expect("harness");
}
