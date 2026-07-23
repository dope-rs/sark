#![cfg(target_os = "linux")]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_extra::harness::Harness;
use http::StatusCode;
use o3::buffer::Shared;
use sark::{Executor, Throughput, driver};

#[sark_gen::request(ordered)]
struct EchoReq {
    #[path("id", default = "MISSING")]
    pub id: o3::buffer::Bytes<o3::buffer::Retained>,
    #[header("x-echo-marker", default = "MISSING")]
    pub marker: o3::buffer::Bytes<o3::buffer::Retained>,
    #[raw_body]
    pub payload: Shared,
}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: Vec<u8>,
}

#[sark_gen::handler]
async fn echo_handler(request: EchoReq, _state: &(), timer: sark::Timer) -> Reply {
    timer.sleep(Duration::from_millis(40)).await;
    let mut body = Vec::new();
    body.extend_from_slice(b"id=");
    body.extend_from_slice(request.id.as_slice());
    body.extend_from_slice(b" marker=");
    body.extend_from_slice(request.marker.as_slice());
    body.extend_from_slice(b" payload=");
    body.extend_from_slice(request.payload.as_slice());
    Reply {
        status: StatusCode::OK,
        body,
    }
}

sark_gen::define_route! {
    EchoDispatch: () => {
        POST "/echo/:id" => async(capacity = 32) echo_handler,
    }
}

fn req(id: &str, marker: &str, body: &str) -> Vec<u8> {
    format!(
        "POST /echo/{id} HTTP/1.1\r\nHost: x\r\nx-echo-marker: {marker}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn expect(id: &str, marker: &str, body: &str) -> String {
    format!("id={id} marker={marker} payload={body}")
}

fn server(bind: std::net::SocketAddr) -> support::TestHttpServer {
    support::http_server(bind, Duration::from_secs(10))
}

fn read_body(sock: &mut TcpStream, acc: &mut Vec<u8>) -> (u16, String) {
    loop {
        if let Some(head_end) = acc.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = std::str::from_utf8(&acc[..head_end]).expect("utf8 head");
            let status: u16 = head.split_whitespace().nth(1).unwrap().parse().unwrap();
            let cl = head
                .split("\r\n")
                .find_map(|l| {
                    l.split_once(':')
                        .filter(|(n, _)| n.eq_ignore_ascii_case("content-length"))
                })
                .map_or(0, |(_, v)| v.trim().parse().unwrap());
            let total = head_end + 4 + cl;
            if acc.len() >= total {
                let body = String::from_utf8(acc[head_end + 4..total].to_vec()).expect("utf8 body");
                acc.drain(..total);
                return (status, body);
            }
        }
        let mut buf = [0u8; 4096];
        let n = sock.read(&mut buf).expect("read");
        assert!(n > 0, "connection closed before full response");
        acc.extend_from_slice(&buf[..n]);
    }
}

#[test]
fn accum_zero_copy_retain_survives_cow_during_await() {
    let bind: std::net::SocketAddr = "127.0.0.1:18923".parse().unwrap();
    Harness::new(bind)
        .run_with_trigger(
            |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?;
                executor.enter(|mut session| {
                    let timer = sark::Timer::with_capacity(32);
                    server(bind).serve(
                        &mut session,
                        EchoDispatch::new(
                            &(),
                            &timer,
                            sark::app::Config {
                                timer_capacity: 32,
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                sock.set_nodelay(true).unwrap();

                let r1 = req("REQ1ID", "MARKER-ONE", "BODY-REQ1-AAAA");
                let head_end = r1.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                let r2 = req("REQ2ID", "MARKER-TWO", "BODY-REQ2-BBBB");

                sock.write_all(&r1[..head_end]).unwrap();
                std::thread::sleep(Duration::from_millis(15));
                sock.write_all(&r1[head_end..]).unwrap();
                std::thread::sleep(Duration::from_millis(15));
                sock.write_all(&r2).unwrap();

                let mut acc = Vec::new();
                let (s1, b1) = read_body(&mut sock, &mut acc);
                let (s2, b2) = read_body(&mut sock, &mut acc);

                assert_eq!(s1, 200, "resp1: {b1:?}");
                assert_eq!(
                    b1,
                    expect("REQ1ID", "MARKER-ONE", "BODY-REQ1-AAAA"),
                    "req1 head/body corrupted by accum mutation during await (COW failed?)"
                );
                assert_eq!(s2, 200, "resp2: {b2:?}");
                assert_eq!(b2, expect("REQ2ID", "MARKER-TWO", "BODY-REQ2-BBBB"));
            },
        )
        .expect("harness");
}

#[test]
fn socket_fast_path_retain_survives_buffer_reuse() {
    let bind: std::net::SocketAddr = "127.0.0.1:18924".parse().unwrap();
    Harness::new(bind)
        .run_with_trigger(
            |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?;
                executor.enter(|mut session| {
                    let timer = sark::Timer::with_capacity(32);
                    server(bind).serve(
                        &mut session,
                        EchoDispatch::new(
                            &(),
                            &timer,
                            sark::app::Config {
                                timer_capacity: 32,
                                task_capacity: support::MAX_CONNECTIONS,
                            },
                        ),
                        Some(trigger),
                    )
                })
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                sock.set_nodelay(true).unwrap();

                let r1 = req("REQ1ID", "MARKER-ONE", "BODY-REQ1-AAAA");
                let r2 = req("REQ2ID", "MARKER-TWO", "BODY-REQ2-BBBB");

                sock.write_all(&r1).unwrap();
                std::thread::sleep(Duration::from_millis(15));
                sock.write_all(&r2).unwrap();

                let mut acc = Vec::new();
                let (s1, b1) = read_body(&mut sock, &mut acc);
                let (s2, b2) = read_body(&mut sock, &mut acc);

                assert_eq!(s1, 200, "resp1: {b1:?}");
                assert_eq!(
                    b1,
                    expect("REQ1ID", "MARKER-ONE", "BODY-REQ1-AAAA"),
                    "req1 corrupted after socket buffer reuse (retain copy failed?)"
                );
                assert_eq!(s2, 200, "resp2: {b2:?}");
                assert_eq!(b2, expect("REQ2ID", "MARKER-TWO", "BODY-REQ2-BBBB"));
            },
        )
        .expect("harness");
}
