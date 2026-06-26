#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_extra::testing::run_with_trigger;
use http::StatusCode;
use o3::buffer::{Owned, Shared};
use sark::{Build, ServerCfg};

#[sark_gen::request(ordered)]
struct EchoReq {
    #[path("id", default = "MISSING")]
    pub id: sark_core::http::LocalFrameBytes,
    #[header("x-echo-marker", default = "MISSING")]
    pub marker: sark_core::http::LocalFrameBytes,
    #[raw_body]
    pub payload: Shared,
}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: Owned,
}

#[sark_gen::handler]
async fn echo_handler(request: EchoReq, _state: &(), timer: sark::Timer) -> Reply {
    timer.sleep(Duration::from_millis(40)).await;
    let mut body = Owned::new();
    body.extend_from_slice(b"id=");
    body.extend_from_slice(request.id.as_bytes());
    body.extend_from_slice(b" marker=");
    body.extend_from_slice(request.marker.as_bytes());
    body.extend_from_slice(b" payload=");
    body.extend_from_slice(request.payload.as_slice());
    Reply {
        status: StatusCode::OK,
        body,
    }
}

sark_gen::define_route! {
    EchoDispatch: () => {
        POST "/echo/:id" => async echo_handler,
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

fn cfg(bind: std::net::SocketAddr) -> ServerCfg {
    ServerCfg {
        bind,
        max_conn: 16,
        backlog: 16,
        head_timeout: Duration::from_secs(10),
    }
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

/// req1 lands in the recv accum (head, then body across two reads), so its retained
/// bytes are a zero-copy slice of the accum. While req1 is still awaiting, req2 arrives
/// and mutates that same accum — the slice must stay intact via copy-on-write.
#[test]
fn accum_zero_copy_retain_survives_cow_during_await() {
    let bind: std::net::SocketAddr = "127.0.0.1:18923".parse().unwrap();
    run_with_trigger(
        bind,
        |ctx, trigger| Build::http(echo_dispatch::new(&()), cfg(bind), ctx, Some(trigger)),
        |bind| {
            let mut sock = TcpStream::connect(bind).expect("connect");
            sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            sock.set_nodelay(true).unwrap();

            let r1 = req("REQ1ID", "MARKER-ONE", "BODY-REQ1-AAAA");
            let head_end = r1.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
            let r2 = req("REQ2ID", "MARKER-TWO", "BODY-REQ2-BBBB");

            // Head first → server reserves the accum and waits for the body.
            sock.write_all(&r1[..head_end]).unwrap();
            std::thread::sleep(Duration::from_millis(15));
            // Body → req1 dispatches from the accum (zero-copy retain), then awaits.
            sock.write_all(&r1[head_end..]).unwrap();
            std::thread::sleep(Duration::from_millis(15));
            // req2 mutates the accum while req1's retained slice is still alive.
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
    );
}

/// req1 arrives whole in one read (socket fast-path), so its retained bytes are an
/// owned copy, not an accum slice. After req1 awaits, req2 reuses the socket buffer —
/// req1's head and body must survive because the copy is independent.
#[test]
fn socket_fast_path_retain_survives_buffer_reuse() {
    let bind: std::net::SocketAddr = "127.0.0.1:18924".parse().unwrap();
    run_with_trigger(
        bind,
        |ctx, trigger| Build::http(echo_dispatch::new(&()), cfg(bind), ctx, Some(trigger)),
        |bind| {
            let mut sock = TcpStream::connect(bind).expect("connect");
            sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            sock.set_nodelay(true).unwrap();

            let r1 = req("REQ1ID", "MARKER-ONE", "BODY-REQ1-AAAA");
            let r2 = req("REQ2ID", "MARKER-TWO", "BODY-REQ2-BBBB");

            // Whole request in one write → socket fast-path, retained by copy.
            sock.write_all(&r1).unwrap();
            std::thread::sleep(Duration::from_millis(15));
            // Second request reuses the recv buffer while req1 is still awaiting.
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
    );
}
