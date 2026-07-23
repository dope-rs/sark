#![cfg(target_os = "linux")]

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_extra::harness::Harness;
use http::StatusCode;
use sark::{Executor, Throughput, driver};

#[sark_gen::request(ordered)]
struct EchoReq {
    #[path("id", default = "MISSING")]
    pub id: o3::buffer::Bytes<o3::buffer::Retained>,
    #[header("x-echo-marker", default = "MISSING")]
    pub marker: o3::buffer::Bytes<o3::buffer::Retained>,
}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: Vec<u8>,
}

#[sark_gen::handler]
async fn echo_handler(request: EchoReq, _state: &(), timer: sark::Timer) -> Reply {
    timer.sleep(Duration::from_millis(20)).await;
    let mut body = Vec::new();
    body.extend_from_slice(b"id=");
    body.extend_from_slice(request.id.as_slice());
    body.extend_from_slice(b" marker=");
    body.extend_from_slice(request.marker.as_slice());
    Reply {
        status: StatusCode::OK,
        body,
    }
}

sark_gen::define_route! {
    EchoDispatch: () => {
        GET "/echo/:id" => async(capacity = 32) echo_handler,
    }
}

struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

impl HttpResponse {
    fn body_str(&self) -> &str {
        std::str::from_utf8(&self.body).expect("response body is utf8")
    }
}

fn read_one_response(sock: &mut TcpStream, acc: &mut Vec<u8>) -> HttpResponse {
    loop {
        if let Some(head_end) = find_double_crlf(acc) {
            let head = &acc[..head_end];
            let status = parse_status(head).expect("status line");
            let cl = content_length(head).unwrap_or(0);
            let total = head_end + 4 + cl;
            if acc.len() >= total {
                let body = acc[head_end + 4..total].to_vec();
                acc.drain(..total);
                return HttpResponse { status, body };
            }
        }
        let mut buf = [0u8; 4096];
        let n = sock.read(&mut buf).expect("read response");
        assert!(n > 0, "connection closed before full response");
        acc.extend_from_slice(&buf[..n]);
    }
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_status(head: &[u8]) -> Option<u16> {
    let text = std::str::from_utf8(head).ok()?;
    text.split("\r\n")
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

fn content_length(head: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(head).ok()?;
    for line in text.split("\r\n") {
        let mut it = line.splitn(2, ':');
        let name = it.next()?.trim();
        if name.eq_ignore_ascii_case("content-length") {
            return it.next()?.trim().parse().ok();
        }
    }
    None
}

#[test]
fn async_handler_keeps_request_bytes_after_pipelined_request() {
    let bind: std::net::SocketAddr = "127.0.0.1:18922".parse().unwrap();
    let server = support::http_server(bind, Duration::from_secs(10));

    Harness::new(bind).run_with_trigger(
        |_ctx, trigger| {
            let driver_config =
                driver::Config::for_tcp_profile::<Throughput>(support::MAX_CONNECTIONS);
                let executor = Executor::new(driver_config)?;
                executor.enter(|mut session| {
                    let timer = sark::Timer::with_capacity(
                        support::MAX_CONNECTIONS.saturating_mul(2),
                    );
                    server.clone().serve(
                        &mut session,
                        EchoDispatch::new(
                            &(),
                            &timer,
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
            sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            sock.set_nodelay(true).unwrap();

            let req1 = b"GET /echo/MAGICBYTESREQ1 HTTP/1.1\r\nHost: x\r\nx-echo-marker: MARKER-REQ1-VALUE\r\n\r\n";
            let req2 = b"GET /echo/ZZZZZZZZZZZZZZZZZZZZZZZZZZZZ HTTP/1.1\r\nHost: yyyyyyyyyyyyyyyyyyyyyyyy\r\nx-echo-marker: ZZZZZZZZZZZZZZZZZZZZZZZZ\r\n\r\n";
            let mut pipelined = Vec::new();
            pipelined.extend_from_slice(req1);
            pipelined.extend_from_slice(req2);
            sock.write_all(&pipelined).unwrap();

            let mut acc = Vec::new();
            let resp1 = read_one_response(&mut sock, &mut acc);
            let resp2 = read_one_response(&mut sock, &mut acc);

            assert_eq!(resp1.status, 200, "resp1 body: {:?}", resp1.body_str());
            assert_eq!(
                resp1.body_str(),
                "id=MAGICBYTESREQ1 marker=MARKER-REQ1-VALUE",
                "first response did not echo the original request bytes (use-after-free?)"
            );
            assert_eq!(resp2.status, 200, "resp2 body: {:?}", resp2.body_str());
            assert_eq!(
                resp2.body_str(),
                "id=ZZZZZZZZZZZZZZZZZZZZZZZZZZZZ marker=ZZZZZZZZZZZZZZZZZZZZZZZZ"
            );
        },
    ).expect("harness");
}
