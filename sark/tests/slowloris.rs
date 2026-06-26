#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use dope_extra::testing::run_with_trigger;
use http::StatusCode;
use sark::{Build, ServerCfg};

#[sark_gen::request]
struct HelloRequest {}

#[sark_gen::response(raw)]
struct HelloReply {
    status: StatusCode,
    body: o3::buffer::Owned,
}

#[sark_gen::handler]
fn hello(_req: HelloRequest, _state: &()) -> HelloReply {
    let mut body = o3::buffer::Owned::new();
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

fn cfg(bind: std::net::SocketAddr) -> ServerCfg {
    ServerCfg {
        bind,
        max_conn: 16,
        backlog: 16,
        head_timeout: HEAD_TIMEOUT,
    }
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
    let bind: std::net::SocketAddr = "127.0.0.1:38901".parse().unwrap();
    run_with_trigger(
        bind,
        |ctx, trigger| Build::http(slowloris_dispatch::new(&()), cfg(bind), ctx, Some(trigger)),
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
    );
}

#[test]
fn normal_fast_request_is_unaffected() {
    let bind: std::net::SocketAddr = "127.0.0.1:38902".parse().unwrap();
    run_with_trigger(
        bind,
        |ctx, trigger| Build::http(slowloris_dispatch::new(&()), cfg(bind), ctx, Some(trigger)),
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
    );
}

#[test]
fn slow_but_progressing_within_deadline_completes() {
    let bind: std::net::SocketAddr = "127.0.0.1:38903".parse().unwrap();
    run_with_trigger(
        bind,
        |ctx, trigger| Build::http(slowloris_dispatch::new(&()), cfg(bind), ctx, Some(trigger)),
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
    );
}
