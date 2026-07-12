#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use dope_extra::testing::{ephemeral_addr, run_with_trigger};
use http::StatusCode;
use o3::buffer::Owned;
use sark::request::BodyLen;
use sark::{Build, ServerCfg};

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
struct UpReq {
    #[stream_body]
    payload: BodyLen,
}

#[sark_gen::response(raw)]
struct UpReply {
    status: StatusCode,
    body: Owned,
}

#[sark_gen::handler]
#[max_body(16 * 1024 * 1024)]
fn up(req: UpReq, _state: &()) -> UpReply {
    let mut body = Owned::new();
    body.extend_from_slice(req.payload.len().to_string().as_bytes());
    UpReply {
        status: StatusCode::OK,
        body,
    }
}

sark_gen::define_route! {
    StreamDiscardDispatch: () => {
        GET "/ping" => ping,
        POST "/up" => up,
    }
}

fn cfg() -> ServerCfg {
    ServerCfg {
        bind: ephemeral_addr(),
        max_conn: 16,
        backlog: 16,
        head_timeout: Duration::from_secs(10),
    }
}

fn serve(cfg: ServerCfg, client: impl FnOnce(SocketAddr)) {
    let bind = cfg.bind;
    run_with_trigger(
        bind,
        move |ctx, trigger| {
            Build::http(
                stream_discard_dispatch::new(&()),
                cfg.clone(),
                ctx,
                Some(trigger),
            )
        },
        client,
    );
}

fn content_length(head: &[u8]) -> usize {
    std::str::from_utf8(head)
        .unwrap_or("")
        .split("\r\n")
        .filter_map(|line| line.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse().ok())
        .unwrap_or(0)
}

fn body_of(resp: &str) -> &str {
    resp.split_once("\r\n\r\n").map_or("", |(_, body)| body)
}

fn read_response(sock: &mut TcpStream) -> String {
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        if let Some(end) = out.windows(4).position(|w| w == b"\r\n\r\n") {
            let body_start = end + 4;
            if out.len() - body_start >= content_length(&out[..body_start]) {
                break;
            }
        }
        match sock.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => panic!("read: {e}"),
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn ping_on(sock: &mut TcpStream) -> String {
    sock.write_all(b"GET /ping HTTP/1.1\r\nHost: x\r\n\r\n")
        .unwrap();
    read_response(sock)
}

fn ping_fresh(bind: SocketAddr) -> String {
    for _ in 0..200 {
        if let Ok(mut sock) = TcpStream::connect(bind) {
            let resp = ping_on(&mut sock);
            if body_of(&resp) == "pong" {
                return resp;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("server never answered a fresh /ping");
}

fn upload_head(sock: &mut TcpStream, total: usize) -> String {
    let head = format!("POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: {total}\r\n\r\n");
    sock.write_all(head.as_bytes()).unwrap();
    read_response(sock)
}

#[test]
fn large_body_is_drained_and_conn_reusable() {
    serve(cfg(), |bind| {
        let total: usize = 8 * 1024 * 1024;
        let mut sock = TcpStream::connect(bind).expect("connect");
        let resp = upload_head(&mut sock, total);
        assert_eq!(body_of(&resp), total.to_string(), "upload response: {resp}");
        let chunk = vec![0xa5u8; 64 * 1024];
        let mut sent = 0;
        while sent < total {
            let n = chunk.len().min(total - sent);
            sock.write_all(&chunk[..n]).unwrap();
            sent += n;
        }
        assert_eq!(body_of(&ping_on(&mut sock)), "pong", "reuse after drain");
    });
}

#[test]
fn body_tail_and_pipelined_request_in_one_segment() {
    serve(cfg(), |bind| {
        let total: usize = 256 * 1024;
        let mut sock = TcpStream::connect(bind).expect("connect");
        let prefix = vec![0x11u8; 1000];
        let head = format!("POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: {total}\r\n\r\n");
        let mut first = head.into_bytes();
        first.extend_from_slice(&prefix);
        sock.write_all(&first).unwrap();
        let resp = read_response(&mut sock);
        assert_eq!(body_of(&resp), total.to_string(), "{resp}");
        let mut second = vec![0x22u8; total - prefix.len()];
        second.extend_from_slice(b"GET /ping HTTP/1.1\r\nHost: x\r\n\r\n");
        sock.write_all(&second).unwrap();
        let resp = read_response(&mut sock);
        assert_eq!(
            body_of(&resp),
            "pong",
            "pipelined request after drain boundary: {resp}"
        );
    });
}

#[test]
fn small_body_in_first_chunk_needs_no_discard() {
    serve(cfg(), |bind| {
        let mut sock = TcpStream::connect(bind).expect("connect");
        let body = b"tiny";
        let req = format!(
            "POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        sock.write_all(req.as_bytes()).unwrap();
        sock.write_all(body).unwrap();
        let resp = read_response(&mut sock);
        assert_eq!(body_of(&resp), body.len().to_string(), "{resp}");
        assert_eq!(body_of(&ping_on(&mut sock)), "pong");
    });
}

#[test]
fn peer_close_mid_body_does_not_wedge_server() {
    serve(cfg(), |bind| {
        let total: usize = 4 * 1024 * 1024;
        let mut sock = TcpStream::connect(bind).expect("connect");
        let resp = upload_head(&mut sock, total);
        assert_eq!(body_of(&resp), total.to_string(), "{resp}");
        sock.write_all(&vec![0u8; 128 * 1024]).unwrap();
        drop(sock);
        assert_eq!(
            body_of(&ping_fresh(bind)),
            "pong",
            "server wedged after mid-body FIN"
        );
    });
}
