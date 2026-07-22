#![cfg(target_os = "linux")]

mod support;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use dope_extra::harness::Harness;
use http::StatusCode;
use sark::request::BodyLen;
use sark::service::{BodyPolicy, RouteRequestImpl};
use sark::{Executor, Throughput, driver};

#[sark_gen::request]
struct PingReq {}

#[sark_gen::response(raw)]
struct PingReply {
    status: StatusCode,
    body: Vec<u8>,
}

#[sark_gen::handler]
fn ping(_req: PingReq, _state: &()) -> PingReply {
    let mut body = Vec::new();
    body.extend_from_slice(b"pong");
    PingReply {
        status: StatusCode::OK,
        body,
    }
}

#[sark_gen::request]
struct UpReq {
    #[body_len]
    payload: BodyLen,
}

#[sark_gen::response(raw)]
struct UpReply {
    status: StatusCode,
    body: Vec<u8>,
}

#[sark_gen::handler]
#[max_body(16 * 1024 * 1024)]
fn up(req: UpReq, _state: &()) -> UpReply {
    let mut body = Vec::new();
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

#[test]
fn request_macro_selects_discard_policy_at_compile_time() {
    assert_eq!(PingReq::BODY_POLICY, BodyPolicy::Discarded);
    assert_eq!(UpReq::BODY_POLICY, BodyPolicy::Discarded);
}

fn serve(client: impl FnOnce(SocketAddr)) {
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let server = support::http_server(bind, Duration::from_secs(10));
    harness
        .run_with_trigger(
            move |_ctx, trigger| {
                let driver_config =
                    driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?;
                executor.enter(|mut session| {
                    server.clone().serve(
                        &mut session,
                        StreamDiscardDispatch::new(
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
            client,
        )
        .expect("harness");
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
    serve(|bind| {
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
    serve(|bind| {
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
    serve(|bind| {
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
fn deep_large_body_pipeline_does_not_enter_the_recv_backlog() {
    serve(|bind| {
        const DEPTH: usize = 8;
        const BODY_LEN: usize = 1024 * 1024;

        let mut sock = TcpStream::connect(bind).expect("connect");
        sock.set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let body = [0xa5; 64 * 1024];
        for _ in 0..DEPTH {
            let head =
                format!("POST /up HTTP/1.1\r\nHost: x\r\nContent-Length: {BODY_LEN}\r\n\r\n");
            sock.write_all(head.as_bytes()).unwrap();
            for _ in 0..BODY_LEN / body.len() {
                sock.write_all(&body).unwrap();
            }
        }

        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut responses = Vec::new();
        let mut chunk = [0; 4096];
        while responses
            .windows(b"HTTP/1.1 200".len())
            .filter(|window| *window == b"HTTP/1.1 200")
            .count()
            < DEPTH
        {
            let n = sock.read(&mut chunk).expect("read pipelined responses");
            assert_ne!(n, 0, "server closed before every pipelined response");
            responses.extend_from_slice(&chunk[..n]);
        }
        let expected_len = BODY_LEN.to_string();
        assert_eq!(
            responses
                .windows(expected_len.len())
                .filter(|window| *window == expected_len.as_bytes())
                .count(),
            DEPTH
        );
    });
}

#[test]
fn peer_close_mid_body_does_not_wedge_server() {
    serve(|bind| {
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
