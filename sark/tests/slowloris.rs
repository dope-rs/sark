#![cfg(target_os = "linux")]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use dope_test::Harness;
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
    SlowlorisDispatch: () => {
        GET "/hello" => hello,
    }
}

const HEAD_TIMEOUT: Duration = Duration::from_millis(500);

fn server(bind: std::net::SocketAddr) -> support::TestHttpServer {
    support::http_server(bind, HEAD_TIMEOUT)
}

fn read_to_close(stream: &mut TcpStream) -> (Vec<u8>, Duration) {
    let start = Instant::now();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("read: {e}"),
        }
    }
    (buf, start.elapsed())
}

#[test]
fn partial_head_then_stall_is_closed_after_deadline() {
    let harness = Harness::bind().expect("reserve test address");
    let bind = harness.addr();
    harness
        .run_with_trigger(
            |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?
                    .with_storage(dope_net::link::egress::storage::Storage::default());
                executor.enter(|mut session| {
                    let timer = sark::Timer::new();
                    server(bind).serve(
                        &mut session,
                        SlowlorisDispatch::new(
                            &(),
                            &timer,
                            sark::app::Config {
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("set timeout");
                sock.write_all(b"GET /hello HTTP/1.1\r\nHost: x\r\n")
                    .expect("write partial head");

                let (buf, elapsed) = read_to_close(&mut sock);
                assert!(
                    elapsed >= Duration::from_millis(300),
                    "closed too early ({elapsed:?}); deadline must bound from first byte"
                );
                assert!(
                    elapsed < Duration::from_secs(4),
                    "not closed by head deadline ({elapsed:?})"
                );
                let text = String::from_utf8_lossy(&buf);
                assert!(
                    buf.is_empty() || text.contains("408"),
                    "expected close or 408, got: {text:?}"
                );
            },
        )
        .expect("harness");
}

#[test]
fn normal_fast_request_is_unaffected() {
    let harness = Harness::bind().expect("reserve test address");
    let bind = harness.addr();
    harness
        .run_with_trigger(
            |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?
                    .with_storage(dope_net::link::egress::storage::Storage::default());
                executor.enter(|mut session| {
                    let timer = sark::Timer::new();
                    server(bind).serve(
                        &mut session,
                        SlowlorisDispatch::new(
                            &(),
                            &timer,
                            sark::app::Config {
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set timeout");
                sock.write_all(b"GET /hello HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .expect("write");
                let (buf, _) = read_to_close(&mut sock);
                let text = String::from_utf8_lossy(&buf);
                assert!(text.starts_with("HTTP/1.1 200 "), "status: {text:?}");
                assert!(!text.contains("408"), "false-positive deadline: {text:?}");
                assert!(text.ends_with("hello"), "body: {text:?}");
            },
        )
        .expect("harness");
}

#[test]
fn slow_but_progressing_within_deadline_completes() {
    let harness = Harness::bind().expect("reserve test address");
    let bind = harness.addr();
    harness
        .run_with_trigger(
            |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?
                    .with_storage(dope_net::link::egress::storage::Storage::default());
                executor.enter(|mut session| {
                    let timer = sark::Timer::new();
                    server(bind).serve(
                        &mut session,
                        SlowlorisDispatch::new(
                            &(),
                            &timer,
                            sark::app::Config {
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set timeout");
                sock.write_all(b"GET /hello HTTP/1.1\r\n").expect("write 1");
                std::thread::sleep(Duration::from_millis(150));
                sock.write_all(b"Host: x\r\n").expect("write 2");
                std::thread::sleep(Duration::from_millis(150));
                sock.write_all(b"Connection: close\r\n\r\n")
                    .expect("write 3");

                let (buf, _) = read_to_close(&mut sock);
                let text = String::from_utf8_lossy(&buf);
                assert!(text.starts_with("HTTP/1.1 200 "), "status: {text:?}");
                assert!(
                    !text.contains("408"),
                    "false close on slow progress: {text:?}"
                );
                assert!(text.ends_with("hello"), "body: {text:?}");
            },
        )
        .expect("harness");
}

#[test]
fn zero_fast_timer_capacity_tracks_every_connection() {
    let harness = Harness::bind().expect("reserve test address");
    let bind = harness.addr();
    harness
        .run_with_trigger(
            |_ctx, trigger| {
                let mut driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                driver_config.timer_slots = 0;
                let executor = Executor::new(driver_config)?
                    .with_storage(dope_net::link::egress::storage::Storage::default());
                executor.enter(|mut session| {
                    let timer = sark::Timer::new();
                    server(bind).serve(
                        &mut session,
                        SlowlorisDispatch::new(
                            &(),
                            &timer,
                            sark::app::Config {
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let mut tracked = TcpStream::connect(bind).expect("connect tracked");
                tracked
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set timeout");
                tracked
                    .write_all(b"GET /hello HTTP/1.1\r\nHost: tracked\r\n")
                    .expect("write tracked head");
                std::thread::sleep(Duration::from_millis(50));

                let mut second = TcpStream::connect(bind).expect("connect second");
                second
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("set timeout");
                second
                    .write_all(b"GET /hello HTTP/1.1\r\nHost: second\r\n")
                    .expect("write second head");

                let (second_reply, elapsed) = read_to_close(&mut second);
                assert!(
                    elapsed >= Duration::from_millis(300),
                    "second connection closed before its deadline: {elapsed:?}"
                );
                assert!(
                    elapsed < Duration::from_secs(2),
                    "second connection was not deadline-tracked: {elapsed:?}"
                );
                let (first_reply, _) = read_to_close(&mut tracked);
                for reply in [first_reply, second_reply] {
                    let text = String::from_utf8_lossy(&reply);
                    assert!(
                        reply.is_empty() || text.contains("408"),
                        "expected close or 408, got: {text:?}"
                    );
                }
            },
        )
        .expect("harness");
}
