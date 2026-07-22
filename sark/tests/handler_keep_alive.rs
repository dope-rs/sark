#![cfg(target_os = "linux")]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_extra::harness::Harness;
use http::StatusCode;
use sark::{Executor, Throughput, driver};

#[sark_gen::request]
struct EmptyReq {}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: Vec<u8>,
}

#[sark_gen::handler]
fn ping_handler(_req: EmptyReq, _state: &()) -> Reply {
    let mut body = Vec::new();
    body.extend_from_slice(b"pong");
    Reply {
        status: StatusCode::OK,
        body,
    }
}

sark_gen::define_route! {
    KeepAliveDispatch: () => {
        GET "/ping" => ping_handler,
    }
}

#[test]
fn http11_default_keeps_connection_open() {
    let bind: std::net::SocketAddr = "127.0.0.1:18895".parse().unwrap();
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
                        KeepAliveDispatch::new(
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
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

                sock.write_all(b"GET /ping HTTP/1.1\r\nHost: x\r\n\r\n")
                    .unwrap();
                let mut buf = [0u8; 4096];
                let n1 = sock.read(&mut buf).expect("read 1");
                let resp1 = std::str::from_utf8(&buf[..n1]).unwrap();
                assert!(resp1.contains("200 OK"), "resp1: {}", resp1);
                assert!(resp1.contains("pong"), "resp1: {}", resp1);

                sock.write_all(b"GET /ping HTTP/1.1\r\nHost: x\r\n\r\n")
                    .unwrap();
                let n2 = sock.read(&mut buf).expect("read 2 — connection was closed");
                assert!(n2 > 0, "second read returned EOF — keep-alive broken");
                let resp2 = std::str::from_utf8(&buf[..n2]).unwrap();
                assert!(resp2.contains("200 OK"), "resp2: {}", resp2);
                assert!(resp2.contains("pong"), "resp2: {}", resp2);

                sock.write_all(b"GET /ping HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .unwrap();
                let mut last = String::new();
                sock.read_to_string(&mut last).unwrap();
                assert!(last.contains("200 OK"), "last: {}", last);
            },
        )
        .expect("harness");
}
