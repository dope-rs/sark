#![cfg(target_os = "linux")]
#![allow(clippy::too_many_arguments)]

use std::collections::VecDeque;
use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use dope_extra::testing::run_with_trigger;
use o3::buffer::Shared;
use sark::{Build, ServerCfg};
use sark_core::http::Stream;

const CHUNK_TERMINATOR_BYTES: &[u8] = b"0\r\n\r\n";

struct DelayedChunks {
    chunks: VecDeque<Shared>,
    pend_next: bool,
    yielded: u32,
}

impl DelayedChunks {
    fn new<I: IntoIterator<Item = &'static [u8]>>(parts: I) -> Self {
        Self {
            chunks: parts.into_iter().map(Shared::copy_from_slice).collect(),
            pend_next: true,
            yielded: 0,
        }
    }

    fn arm_pend(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.pend_next {
            self.pend_next = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.pend_next = true;
        Poll::Ready(())
    }
}

impl Future for DelayedChunks {
    type Output = Option<Shared>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Shared>> {
        let this = self.get_mut();
        if this.chunks.is_empty() {
            return Poll::Ready(None);
        }
        if this.arm_pend(cx).is_pending() {
            return Poll::Pending;
        }
        this.yielded += 1;
        Poll::Ready(this.chunks.pop_front())
    }
}

#[sark_gen::request]
struct EmptyReq {}

#[sark_gen::handler]
fn stream_handler(_req: EmptyReq, _state: &()) -> Stream<DelayedChunks> {
    Stream::new(DelayedChunks::new([
        b"alpha".as_slice(),
        b"-beta".as_slice(),
        b"-gamma".as_slice(),
    ]))
    .header(b"content-type", b"text/plain")
}

sark_gen::define_route! {
    StreamPendingDispatch: () => {
        GET "/stream" => stream stream_handler,
    }
}

#[test]
fn pending_chunk_producer_completes_the_response() {
    let bind: std::net::SocketAddr = "127.0.0.1:18921".parse().unwrap();
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
                stream_pending_dispatch::new(&()),
                cfg.clone(),
                ctx,
                Some(trigger),
            )
        },
        |bind| {
            let mut sock = TcpStream::connect(bind).expect("connect");
            sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();

            sock.write_all(b"GET /stream HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                .unwrap();
            let mut resp = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                match sock.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        resp.extend_from_slice(&chunk[..n]);
                        if resp.ends_with(CHUNK_TERMINATOR_BYTES) {
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
            let resp_str = String::from_utf8_lossy(&resp);

            assert!(resp_str.contains("200 OK"), "resp: {resp_str}");
            assert!(
                resp_str
                    .to_lowercase()
                    .contains("transfer-encoding: chunked"),
                "resp: {resp_str}"
            );
            assert!(
                resp.ends_with(CHUNK_TERMINATOR_BYTES),
                "stream never completed (wedged?): {resp_str}"
            );

            let body_start = resp
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|i| i + 4)
                .expect("header terminator");
            let body = &resp[body_start..];

            let mut cursor = 0usize;
            let mut reassembled = Vec::new();
            loop {
                let line_end = body[cursor..]
                    .windows(2)
                    .position(|w| w == b"\r\n")
                    .expect("chunk size CRLF")
                    + cursor;
                let size_str = std::str::from_utf8(&body[cursor..line_end]).expect("hex utf8");
                let size = usize::from_str_radix(size_str.trim(), 16).expect("hex parse");
                cursor = line_end + 2;
                if size == 0 {
                    break;
                }
                reassembled.extend_from_slice(&body[cursor..cursor + size]);
                cursor += size;
                assert_eq!(&body[cursor..cursor + 2], b"\r\n", "chunk CRLF");
                cursor += 2;
            }
            assert_eq!(
                reassembled, b"alpha-beta-gamma",
                "reassembled body: {resp_str}"
            );
        },
    );
}
