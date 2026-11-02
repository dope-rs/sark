pub mod common;

use std::{
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    time::Duration,
};

use common::server::recover;
use common::{
    route::body::stream::*,
    server::{self, Plan},
};
use dope::core::driver::life::recovery::Policy;
use dope_test::harness::server::Pair;
use sark_fiber::Config;

#[test]
fn pending_chunks_complete() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .check(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let result = StreamPendingDispatch::serve(
                    server.with_bound(listener),
                    context,
                    (),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(|event| reports.push(event), recover),
                    |result| {
                        result
                            .as_ref()
                            .map(|_| ())
                            .map_err(|error| error.cause())
                            .expect("server")
                    },
                );
                assert!(
                    reports.events().expect("reclaimed resources").is_empty(),
                    "server retirement: {reports:?}"
                );
                result
            },
            |bind| {
                let mut sock = TcpStream::connect(bind).expect("connect");
                sock.set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("set bounded response wait");

                sock.write_all(b"GET /stream HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .expect("send complete stream request");
                let mut resp = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let n = match sock.read(&mut chunk) {
                        Err(error)
                            if error.kind() == ErrorKind::WouldBlock
                                || error.kind() == ErrorKind::TimedOut =>
                        {
                            break;
                        }
                        result => result.expect("read pending stream response"),
                    };
                    if n == 0 {
                        break;
                    }
                    resp.extend_from_slice(&chunk[..n]);
                    if resp.ends_with(b"0\r\n\r\n") {
                        break;
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
                    resp.ends_with(b"0\r\n\r\n"),
                    "stream never completed (wedged?): {resp_str}"
                );

                assert!(resp.ends_with(b"5\r\nalpha\r\n5\r\n-beta\r\n6\r\n-gamma\r\n0\r\n\r\n"));
            },
            |result| {
                assert!(
                    result.is_ok(),
                    "pending_chunks_complete server shutdown: {:?}",
                    result.err()
                );
            },
        )
        .expect("harness");
}

#[test]
fn streams_response() {
    let harness = Pair::bind().expect("bind isolated listener");
    let bind = harness.addr();
    let server = server::Plan::new(bind, Duration::from_secs(10)).build();

    harness
        .check(
            |context, listener| {
                let mut reports = Plan::try_reports(&server).expect("server descriptor claims");
                let result = StreamDispatch::serve(server.with_bound(listener), context,
                    State(|result| result.expect("stream response")),
                    Config::new(server::MAX_CONNECTIONS),
                    Policy::new(
                        |event| reports.push(event),
                        recover,
                    ),
                    |result| {
                        result.as_ref().map(|_| ()).map_err(|error| error.cause()).expect("server")
                    },
                );
                assert!(reports.events().expect("reclaimed resources").is_empty(), "server retirement: {reports:?}");
                result
            },
            |bind| {
                {
                    let mut sock = TcpStream::connect(bind).expect("connect");
                    sock.set_read_timeout(Some(Duration::from_millis(500)))
                        .expect("connected socket must accept a read timeout");

                sock.write_all(b"GET /stream HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                    .expect("connected socket must accept the stream request");
                let mut resp = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let n = match sock.read(&mut chunk) {
                        Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                        result => result.expect("read streamed response"),
                    };
                    if n == 0 {
                        break;
                    }
                    resp.extend_from_slice(&chunk[..n]);
                    if resp.windows(5).any(|w| w == b"0\r\n\r\n")
                        && resp.ends_with(b"0\r\n\r\n")
                    {
                        break;
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

                assert!(resp.ends_with(b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"));

                }
                {
                    let mut socket = TcpStream::connect(bind).expect("connect array stream");
                    socket
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .expect("set array response deadline");
                    socket
                        .write_all(b"GET /arrays HTTP/1.1\r\nHost: task\r\nConnection: close\r\n\r\n")
                        .expect("write array stream request");
                    let mut response = Vec::new();
                    socket
                        .read_to_end(&mut response)
                        .expect("read array stream response");
                    assert!(response.ends_with(b"6\r\nhello \r\n6\r\nworld!\r\n0\r\n\r\n"));
                }
                let mut sock = TcpStream::connect(bind).expect("connect body stream");
                sock.set_read_timeout(Some(Duration::from_secs(3)))
                    .expect("connected socket must accept a read timeout");
                sock.write_all(
                    b"POST /echo HTTP/1.1\r\nHost: x\r\nx-echo: borrowed\r\nContent-Length: 11\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
                )
                .expect("connected socket must accept the echo headers");
                let mut interim = Vec::new();
                let mut chunk = [0u8; 256];
                while !interim.ends_with(b"\r\n\r\n") {
                    let count = sock.read(&mut chunk).expect("read 100 Continue");
                    assert!(count > 0, "connection closed before 100 Continue");
                    interim.extend_from_slice(&chunk[..count]);
                }
                assert_eq!(interim, b"HTTP/1.1 100 Continue\r\n\r\n");

                sock.write_all(b"hello world")
                    .expect("connected socket must accept the echo body");
                let mut response = Vec::new();
                while !response.ends_with(b"0\r\n\r\n") {
                    let count = sock.read(&mut chunk).expect("read streamed echo");
                    assert!(count > 0, "connection closed before streamed echo");
                    response.extend_from_slice(&chunk[..count]);
                }
                assert!(
                    response.windows(6).any(|window| window == b"200 OK"),
                    "missing streamed success: {}",
                    String::from_utf8_lossy(&response),
                );
                assert!(
                    response
                        .windows(b"x-echo: borrowed".len())
                        .any(|window| window.eq_ignore_ascii_case(b"x-echo: borrowed")),
                    "missing borrowed response header: {}",
                    String::from_utf8_lossy(&response),
                );
                assert!(
                    response.ends_with(b"b\r\nhello world\r\n0\r\n\r\n"),
                    "request body was not streamed unchanged: {}",
                    String::from_utf8_lossy(&response),
                );
            },
            |result| {
                assert!(result.is_ok(), "streamed response server shutdown: {:?}", result.err());
            },
        )
        .expect("harness");
}
