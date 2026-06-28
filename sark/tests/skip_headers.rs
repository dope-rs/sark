#![allow(clippy::too_many_arguments)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_extra::testing::run_with_trigger;
use http::StatusCode;
use sark::{Build, ServerCfg};

#[sark_gen::request]
struct LeanRequest {}

#[sark_gen::request]
struct FullRequest {}

#[sark_gen::request]
struct NoServerRequest {}

#[sark_gen::request]
struct NoDateRequest {}

#[sark_gen::response(raw)]
#[header("content-type", "text/plain")]
struct PingReply {
    status: StatusCode,
    body: &'static [u8],
}

// Opts out of both Server and Date for this (static) route.
#[sark_gen::handler]
#[static_response]
#[skip(date, server)]
fn lean(_req: LeanRequest, _state: &()) -> PingReply {
    PingReply {
        status: StatusCode::OK,
        body: b"ok",
    }
}

// Control: default behavior keeps Server and Date.
#[sark_gen::handler]
#[static_response]
fn full(_req: FullRequest, _state: &()) -> PingReply {
    PingReply {
        status: StatusCode::OK,
        body: b"ok",
    }
}

// Single-skip cases: Date is dropped while Server stays patchable, and vice
// versa (exercises the offset shift in `apply_head_skip`).
#[sark_gen::handler]
#[static_response]
#[skip(server)]
fn no_server(_req: NoServerRequest, _state: &()) -> PingReply {
    PingReply {
        status: StatusCode::OK,
        body: b"ok",
    }
}

#[sark_gen::handler]
#[static_response]
#[skip(date)]
fn no_date(_req: NoDateRequest, _state: &()) -> PingReply {
    PingReply {
        status: StatusCode::OK,
        body: b"ok",
    }
}

sark_gen::define_route! {
    SkipDispatch: () => {
        GET "/lean" => lean,
        GET "/full" => full,
        GET "/no-server" => no_server,
        GET "/no-date" => no_date,
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
        // The response head fully determines the Server/Date policy under test;
        // stop once the blank line terminating it has arrived (a static response
        // streams its body as a separate segment).
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("read response: {e}"),
        }
    }
    buf
}

#[test]
fn skip_attribute_trims_static_response_headers() {
    let bind: std::net::SocketAddr = "127.0.0.1:38771".parse().unwrap();
    let cfg = ServerCfg {
        bind,
        max_conn: 16,
        backlog: 16,
        head_timeout: std::time::Duration::from_secs(10),
    };

    run_with_trigger(
        bind,
        |ctx, trigger| Build::http(skip_dispatch::new(&()), cfg.clone(), ctx, Some(trigger)),
        |bind| {
            let lean_resp = String::from_utf8(http_get_close(bind, "/lean")).expect("utf8");
            assert!(
                lean_resp.starts_with("HTTP/1.1 200 "),
                "lean status: {lean_resp:?}"
            );
            assert!(
                lean_resp.contains("Content-Length: 2\r\n"),
                "lean content-length: {lean_resp:?}"
            );
            assert!(
                !lean_resp.contains("Server:"),
                "lean must omit Server: {lean_resp:?}"
            );
            assert!(
                !lean_resp.contains("Date:"),
                "lean must omit Date: {lean_resp:?}"
            );
            assert!(
                lean_resp.contains("content-type: text/plain\r\n"),
                "lean keeps content-type: {lean_resp:?}"
            );

            let full_resp = String::from_utf8(http_get_close(bind, "/full")).expect("utf8");
            assert!(
                full_resp.contains("Server: sark\r\n"),
                "full keeps Server: {full_resp:?}"
            );
            assert!(
                full_resp.contains("Date: "),
                "full keeps Date: {full_resp:?}"
            );

            let no_server_resp =
                String::from_utf8(http_get_close(bind, "/no-server")).expect("utf8");
            assert!(
                !no_server_resp.contains("Server:"),
                "no-server must omit Server: {no_server_resp:?}"
            );
            assert!(
                no_server_resp.contains("Date: "),
                "no-server keeps patchable Date: {no_server_resp:?}"
            );

            let no_date_resp = String::from_utf8(http_get_close(bind, "/no-date")).expect("utf8");
            assert!(
                no_date_resp.contains("Server: sark\r\n"),
                "no-date keeps Server: {no_date_resp:?}"
            );
            assert!(
                !no_date_resp.contains("Date:"),
                "no-date must omit Date: {no_date_resp:?}"
            );
        },
    );
}
