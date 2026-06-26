#![cfg(target_os = "linux")]
#![allow(clippy::too_many_arguments)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_extra::testing::run_with_trigger;
use http::StatusCode;
use o3::buffer::Owned;
use sark::{Build, ServerCfg, body};

#[sark_gen::request]
struct PingReq {}

#[sark_gen::response(raw)]
struct PingReply {
    status: StatusCode,
    body: Owned,
}

#[sark_gen::handler]
fn ping(_req: PingReq, _state: &()) -> PingReply {
    let mut body = Owned::new();
    body.extend_from_slice(b"pong");
    PingReply {
        status: StatusCode::OK,
        body,
    }
}

#[sark_gen::request]
struct EchoReq {}

#[sark_gen::response(raw)]
struct EchoReply {
    status: StatusCode,
    body: Owned,
}

#[sark_gen::handler]
fn echo(_request: EchoReq, _state: &()) -> EchoReply {
    EchoReply {
        status: StatusCode::OK,
        body: body!("echoed"),
    }
}

sark_gen::define_route! {
    ClOverflowDispatch: () => {
        GET "/ping" => ping,
        POST "/echo" => echo,
    }
}

fn read_head(sock: &mut TcpStream) -> Vec<u8> {
    sock.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match sock.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                out.extend_from_slice(&buf[..n]);
                if out.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => panic!("read: {e}"),
        }
    }
    out
}

#[test]
fn bad_content_length_does_not_panic_and_server_survives() {
    let bind: std::net::SocketAddr = "127.0.0.1:18920".parse().unwrap();
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
                cl_overflow_dispatch::new(&()),
                cfg.clone(),
                ctx,
                Some(trigger),
            )
        },
        |bind| {
            let bad_clens: &[&str] = &[
                "4294967295",
                "99999999999999999999999999999999999999",
                "12abc",
                "18446744073709551615",
                "-5",
                " ",
            ];

            for clen in bad_clens {
                let mut sock = TcpStream::connect(bind).expect("connect");
                let req =
                    format!("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: {clen}\r\n\r\n");
                sock.write_all(req.as_bytes()).unwrap();
                let _ = sock.write_all(b"hi");
                let resp = read_head(&mut sock);
                if !resp.is_empty() {
                    let text = String::from_utf8_lossy(&resp);
                    assert!(
                        text.starts_with("HTTP/1.1 4"),
                        "bad Content-Length {clen:?} produced non-4xx response: {text}"
                    );
                }
            }

            let mut sock = TcpStream::connect(bind).expect("connect");
            sock.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            sock.write_all(b"GET /ping HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                .unwrap();
            let mut last = String::new();
            sock.read_to_string(&mut last).unwrap();
            assert!(last.contains("200 OK"), "server died after bad CL: {last}");
            assert!(last.contains("pong"), "server died after bad CL: {last}");

            let mut sock = TcpStream::connect(bind).expect("connect");
            sock.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let body = b"hello-body";
            let req = format!(
                "POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            sock.write_all(req.as_bytes()).unwrap();
            sock.write_all(body).unwrap();
            let mut resp = String::new();
            sock.read_to_string(&mut resp).unwrap();
            assert!(resp.contains("200 "), "well-formed POST failed: {resp}");
            assert!(resp.contains("echoed"), "echo handler not reached: {resp}");
        },
    );
}
