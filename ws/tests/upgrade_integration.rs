use std::{io::Write, net::TcpListener, sync::mpsc, thread, time::Duration};

use common::{app, client::Probe};
use dope::core::{driver::life::recovery, io::Reclaimed};
use sark_ws::client;

pub mod common;

#[test]
fn interim_upgrade() {
    let mut retired = Vec::new();
    {
        let sock = TcpListener::bind("127.0.0.1:0").expect("bind coalesced response probe");
        let addr = sock.local_addr().expect("coalesced response address");
        let (tx, rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = sock.accept().expect("accept coalesced response probe");
            let req = app::Upgrade::new(&stream).try_head().expect("request head");
            let accept = app::Head::new(&req).try_accept().expect("opening accept");
            stream
                .write_all(b"HTTP/1.1 103 Early Hints\r\nSec-WebSocket-Accept: ignored\r\n")
                .expect("partial informational head");
            stream
                .write_all(b"\r\nHTTP/1.1 100 Continue\r\n\r\n")
                .expect("interim response sequence");
            let mut head = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c/1, websocket\r\nUpgrade: custom/2\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ".to_vec();
            head.extend_from_slice(&accept);
            head.extend_from_slice(b"\r\n\r\n\x81\x04pong");
            stream
                .write_all(&head)
                .expect("write response and first frame atomically");
            rx.recv_timeout(Duration::from_secs(3))
                .expect("client consumed coalesced frame");
        });

        let config = client::Config::new("127.0.0.1", "/ws");
        let config = config.expect("endpoint");
        let result = Probe::new(addr, config).message(
            |result| {
                result
                    .as_ref()
                    .map_err(|cause| cause.cause())
                    .expect("opening");
            },
            recovery::Policy::new(|event| retired.push(event), common::recover),
            |result| result.expect("WebSocket probe"),
        );
        result
            .as_ref()
            .map_err(|cause| cause.kind())
            .expect("topology");
        let seen = result.ok().expect("checked topology");
        assert!(seen.is_open());
        assert_eq!(seen.reply(), Some("pong"));
        tx.send(()).expect("release coalesced probe");
        server.join().expect("coalesced response probe");
    };
    assert!(
        retired
            .iter()
            .all(|event| matches!(event, Reclaimed::Shutdown)),
        "unexpected reports WebSocket client completion: {retired:?}"
    );
}

#[test]
fn bad_accept_blocks_frame() {
    let mut retired = Vec::new();
    {
        let sock = TcpListener::bind("127.0.0.1:0").expect("bind wrong accept probe");
        let addr = sock.local_addr().expect("wrong accept address");
        let (tx, rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = sock.accept().expect("accept wrong response probe");
            app::Upgrade::new(&stream).try_head().expect("request head");
            stream
            .write_all(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: A3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n\x81\x04pong")
            .expect("write wrong response and frame atomically");
            rx.recv_timeout(Duration::from_secs(3))
                .expect("client rejected wrong accept");
        });

        let config = client::Config::new("127.0.0.1", "/ws");
        let config = config.expect("endpoint");
        let result = Probe::new(addr, config).close(
            recovery::Policy::new(|event| retired.push(event), common::recover),
            |result| result.expect("WebSocket probe"),
        );
        result
            .as_ref()
            .map_err(|cause| cause.kind())
            .expect("topology");
        let seen = result.ok().expect("checked topology");
        assert!(!seen.is_open());
        assert_eq!(seen.reply(), None);
        assert!(matches!(seen.close(), Some(client::CloseReason::Protocol)));
        tx.send(()).expect("release wrong accept probe");
        server.join().expect("wrong accept response probe");
    };
    assert!(
        retired
            .iter()
            .all(|event| matches!(event, Reclaimed::Shutdown)),
        "unexpected reports WebSocket client completion: {retired:?}"
    );
}

#[test]
fn bad_field_blocks_open() {
    let mut retired = Vec::new();
    {
        let sock = TcpListener::bind("127.0.0.1:0").expect("bind malformed response probe");
        let addr = sock.local_addr().expect("malformed response address");
        let (tx, rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = sock.accept().expect("accept malformed response probe");
            let req = app::Upgrade::new(&stream).try_head().expect("request head");
            let accept = app::Head::new(&req).try_accept().expect("opening accept");
            stream
            .write_all(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nX-Nul: safe\0shadow\r\nSec-WebSocket-Accept: ")
            .expect("write malformed response");
            stream.write_all(&accept).expect("write response accept");
            stream
                .write_all(b"\r\n\r\n")
                .expect("finish malformed response");
            rx.recv_timeout(Duration::from_secs(3))
                .expect("client rejected malformed response");
        });

        let config = client::Config::new("127.0.0.1", "/ws");
        let config = config.expect("endpoint");
        let result = Probe::new(addr, config).close(
            recovery::Policy::new(|event| retired.push(event), common::recover),
            |result| result.expect("WebSocket probe"),
        );
        result
            .as_ref()
            .map_err(|cause| cause.kind())
            .expect("topology");
        let seen = result.ok().expect("checked topology");
        assert!(matches!(
            seen.close(),
            Some(client::CloseReason::Decode(client::Fault::Open(_)))
        ));
        tx.send(()).expect("release malformed probe");
        server.join().expect("malformed response probe");
    };
    assert!(
        retired
            .iter()
            .all(|event| matches!(event, Reclaimed::Shutdown)),
        "unexpected reports WebSocket client completion: {retired:?}"
    );
}

#[test]
fn interim_cumulative_limit() {
    use std::io::Read as _;

    use sark_http::h1::{
        codec::decode::response,
        head::{Boundary, ParseError},
    };

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind interim peer");
    let addr = listener.local_addr().expect("interim address");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept interim client");
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .expect("peer deadline");
        app::Upgrade::new(&stream).try_head().expect("request head");
        let head = b"HTTP/1.1 103 Early Hints\r\n\r\n";
        stream
            .write_all(&head.repeat(16 * 1024 / head.len() + 1))
            .expect("interim heads");
        assert_eq!(stream.read(&mut [0; 1]).expect("rejected opening"), 0);
    });
    let result = Probe::new(
        addr,
        client::Config::new("localhost", "/").expect("endpoint"),
    )
    .close(recovery::Policy::new(|_| {}, common::recover), |result| {
        let seen = result.expect("WebSocket probe");
        assert!(matches!(
            seen.close(),
            Some(client::CloseReason::Decode(client::Fault::Open(
                response::Error::Head(ParseError::Boundary(Boundary::TooLarge { .. }))
            )))
        ));
        assert!(!seen.is_open());
    });
    result
        .as_ref()
        .map_err(|cause| cause.kind())
        .expect("topology");
    server.join().expect("interim peer");
}
