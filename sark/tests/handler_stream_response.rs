#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use dope_extra::testing::run_with_trigger;
use o3::buffer::Shared;
use sark::{Build, ServerCfg};
use sark_core::http::{IterStream, Stream};

const CHUNK_TERMINATOR_BYTES: &[u8] = b"0\r\n\r\n";

#[sark_gen::request]
struct EmptyReq {}

#[sark_gen::handler]
fn stream_handler(
    _req: EmptyReq,
    _state: &(),
) -> Stream<IterStream<core::array::IntoIter<Shared, 2>>> {
    Stream::from_chunks([
        Shared::copy_from_slice(b"hello"),
        Shared::copy_from_slice(b" world"),
    ])
    .header(b"content-type", b"text/plain")
}

sark_gen::define_route! {
    StreamDispatch: () => {
        GET "/stream" => stream stream_handler,
    }
}

#[test]
fn handler_yields_chunked_response() {
    let bind: std::net::SocketAddr = "127.0.0.1:18891".parse().unwrap();
    let cfg = ServerCfg {
        bind,
        max_conn: 16,
        backlog: 16,
    };

    run_with_trigger(
        bind,
        |ctx, trigger| Build::http(stream_dispatch::new(&()), cfg.clone(), ctx, Some(trigger)),
        |bind| {
            let mut sock = TcpStream::connect(bind).expect("connect");
            sock.set_read_timeout(Some(Duration::from_millis(500)))
                .unwrap();

            sock.write_all(b"GET /stream HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                .unwrap();
            let mut resp = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                match sock.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        resp.extend_from_slice(&chunk[..n]);
                        if resp.windows(5).any(|w| w == CHUNK_TERMINATOR_BYTES)
                            && resp.ends_with(b"0\r\n\r\n")
                        {
                            break;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("read: {e}"),
                }
            }
            let resp_str = String::from_utf8_lossy(&resp);

            assert!(resp_str.contains("200 OK"), "resp: {}", resp_str);
            assert!(
                resp_str
                    .to_lowercase()
                    .contains("transfer-encoding: chunked"),
                "resp: {}",
                resp_str
            );

            let body_start = resp
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|i| i + 4)
                .expect("header terminator");
            let body = &resp[body_start..];

            let mut cursor = 0usize;
            let mut reassembled = Vec::new();
            let mut chunk_sizes = Vec::new();
            loop {
                let line_end = body[cursor..]
                    .windows(2)
                    .position(|w| w == b"\r\n")
                    .expect("chunk size CRLF")
                    + cursor;
                let size_str = std::str::from_utf8(&body[cursor..line_end]).expect("hex utf8");
                let size = usize::from_str_radix(size_str.trim(), 16).expect("hex parse");
                chunk_sizes.push(size);
                cursor = line_end + 2;
                if size == 0 {
                    assert_eq!(
                        &body[cursor..],
                        b"\r\n",
                        "trailer must be CRLF, got {:?}",
                        &body[cursor..]
                    );
                    break;
                }
                reassembled.extend_from_slice(&body[cursor..cursor + size]);
                cursor += size;
                assert_eq!(&body[cursor..cursor + 2], b"\r\n", "chunk CRLF");
                cursor += 2;
            }

            assert_eq!(chunk_sizes, vec![5, 6, 0], "chunk sizes: {:?}", chunk_sizes);
            assert_eq!(reassembled, b"hello world", "reassembled body mismatch");
            assert!(
                resp.ends_with(b"0\r\n\r\n"),
                "expected terminator: {}",
                resp_str
            );
        },
    );
}
