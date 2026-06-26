#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_extra::testing::run_with_trigger;
use http::StatusCode;
use o3::buffer::Owned;
use sark::{Build, ServerCfg};

#[sark_gen::request]
struct EmptyReq {}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: Owned,
}

#[sark_gen::handler]
fn ping_handler(_req: EmptyReq, _state: &()) -> Reply {
    let mut body = Owned::new();
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
    let cfg = ServerCfg {
        bind,
        max_conn: 16,
        backlog: 16,
        head_timeout: std::time::Duration::from_secs(10),
    };

    run_with_trigger(
        bind,
        |ctx, trigger| {
            Build::http(
                keep_alive_dispatch::new(&()),
                cfg.clone(),
                ctx,
                Some(trigger),
            )
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
    );
}
