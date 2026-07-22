#![allow(clippy::too_many_arguments)]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dope_extra::harness::Harness;
use http::StatusCode;
use sark::middleware::{self, Capture, Middleware};
use sark::{Executor, Throughput, driver};

static ACCESS_LOG_HITS: AtomicU32 = AtomicU32::new(0);
static HANDLER_HITS: AtomicU32 = AtomicU32::new(0);

const UNAUTH_RESP: &[u8] =
    b"HTTP/1.1 401 Unauthorized\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

struct AccessLog;

impl Middleware for AccessLog {
    type State = ();

    fn before(_ctx: &mut middleware::Ctx, _: &(), _capture: &mut Capture) -> bool {
        ACCESS_LOG_HITS.fetch_add(1, Ordering::Relaxed);
        false
    }
}

struct AuthGuard;

impl Middleware for AuthGuard {
    type State = ();

    fn before(_ctx: &mut middleware::Ctx, _: &(), capture: &mut Capture) -> bool {
        capture.close(UNAUTH_RESP);
        true
    }
}

#[sark_gen::request]
struct Req {}

#[sark_gen::response(raw)]
struct Resp {
    status: StatusCode,
    body: Vec<u8>,
}

fn ok_body(body: &'static [u8]) -> Resp {
    HANDLER_HITS.fetch_add(1, Ordering::Relaxed);
    let mut buf = Vec::new();
    buf.extend_from_slice(body);
    Resp {
        status: StatusCode::OK,
        body: buf,
    }
}

#[sark_gen::handler]
fn log_pass(_req: Req, _: &()) -> Resp {
    ok_body(b"log-pass")
}

#[sark_gen::handler]
fn secret(_req: Req, _: &()) -> Resp {
    ok_body(b"secret")
}

#[sark_gen::handler]
fn nested_x(_req: Req, _: &()) -> Resp {
    ok_body(b"nested-x")
}

#[sark_gen::handler]
fn open(_req: Req, _: &()) -> Resp {
    ok_body(b"open")
}

sark_gen::define_route! {
    MwDispatch: () => {
        scope "" with (AccessLog,) => [
            GET "/log-pass" => log_pass,
        ],
        scope "/protected" with (AuthGuard,) => [
            GET "/secret" => secret,
        ],
        scope "/nested" with (AccessLog,) => {
            scope "/inner" with (AuthGuard,) => [
                GET "/x" => nested_x,
            ],
        },
        GET "/open" => open,
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
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("read response: {e}"),
        }
    }
    buf
}

fn status_line(buf: &[u8]) -> &[u8] {
    let end = buf
        .windows(2)
        .position(|w| w == b"\r\n")
        .unwrap_or(buf.len());
    &buf[..end]
}

#[test]
fn middleware_integration() {
    let bind: std::net::SocketAddr = "127.0.0.1:38766".parse().unwrap();
    let server = support::http_server(bind, Duration::from_secs(10));

    Harness::new(bind)
        .run_with_trigger(
            |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?;
                executor.enter(|mut session| {
                    server.clone().serve(
                        &mut session,
                        MwDispatch::new(
                            (),
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
                ACCESS_LOG_HITS.store(0, Ordering::Relaxed);
                HANDLER_HITS.store(0, Ordering::Relaxed);

                let resp = http_get_close(bind, "/log-pass");
                assert_eq!(status_line(&resp), b"HTTP/1.1 200 OK", "log-pass status");
                assert_eq!(ACCESS_LOG_HITS.load(Ordering::Relaxed), 1);
                assert_eq!(HANDLER_HITS.load(Ordering::Relaxed), 1);

                let resp = http_get_close(bind, "/protected/secret");
                assert_eq!(
                    status_line(&resp),
                    b"HTTP/1.1 401 Unauthorized",
                    "secret status"
                );
                assert_eq!(
                    ACCESS_LOG_HITS.load(Ordering::Relaxed),
                    1,
                    "access log not invoked outside its scope"
                );
                assert_eq!(
                    HANDLER_HITS.load(Ordering::Relaxed),
                    1,
                    "blocked handler must not run"
                );

                let resp = http_get_close(bind, "/nested/inner/x");
                assert_eq!(
                    status_line(&resp),
                    b"HTTP/1.1 401 Unauthorized",
                    "nested status"
                );
                assert_eq!(
                    ACCESS_LOG_HITS.load(Ordering::Relaxed),
                    2,
                    "outer access log ran before inner auth blocked"
                );
                assert_eq!(
                    HANDLER_HITS.load(Ordering::Relaxed),
                    1,
                    "blocked handler must not run"
                );

                let resp = http_get_close(bind, "/open");
                assert_eq!(status_line(&resp), b"HTTP/1.1 200 OK", "open status");
                assert_eq!(HANDLER_HITS.load(Ordering::Relaxed), 2);
                assert_eq!(
                    ACCESS_LOG_HITS.load(Ordering::Relaxed),
                    2,
                    "access log untouched for non-wrapped route"
                );

                let resp = http_get_close(bind, "/nonexistent");
                assert_eq!(status_line(&resp), b"HTTP/1.1 404 Not Found", "404 status");
                assert_eq!(
                    ACCESS_LOG_HITS.load(Ordering::Relaxed),
                    2,
                    "404 must not run any middleware"
                );
            },
        )
        .expect("harness");
}
