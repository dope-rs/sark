use std::{
    io::{Read, Write},
    net::TcpListener,
};

use common::{app, client::Probe};
use dope::core::{driver::life::recovery, io::Reclaimed};
use dope_test::harness::scenario::run::{Entry, Task as _};
use sark_ws::client;

pub mod common;

#[test]
fn interim_upgrade() {
    let mut retired = Vec::new();
    {
        let sock = TcpListener::bind("127.0.0.1:0").expect("bind coalesced response probe");
        let addr = sock.local_addr().expect("coalesced response address");
        Entry(move || {
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
            while stream.read(&mut [0; 64]).expect("client close") != 0 {}
        })
        .with(|| {
            let config = client::Config::new("127.0.0.1", "/ws");
            let config = config.expect("endpoint");
            let seen = Probe::new(addr, config).run(
                common::client::Goal::Message,
                recovery::Policy::new(|event| retired.push(event), common::recover),
            );
            assert!(seen.is_open());
            assert_eq!(seen.reply(), Some("pong"));
        })
        .expect("coalesced response probe");
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
        Entry(move || {
            let (mut stream, _) = sock.accept().expect("accept wrong response probe");
            app::Upgrade::new(&stream).try_head().expect("request head");
            stream
            .write_all(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: A3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n\x81\x04pong")
            .expect("write wrong response and frame atomically");
            while stream.read(&mut [0; 64]).expect("client close") != 0 {}
        })
        .with(|| {
            let config = client::Config::new("127.0.0.1", "/ws");
            let config = config.expect("endpoint");
            let seen = Probe::new(addr, config).run(
                common::client::Goal::Close,
                recovery::Policy::new(|event| retired.push(event), common::recover),
            );
            assert!(!seen.is_open());
            assert_eq!(seen.reply(), None);
            assert!(matches!(seen.close(), Some(client::CloseReason::Protocol)));
        })
        .expect("wrong accept response probe");
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
        Entry(move || {
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
            while stream.read(&mut [0; 64]).expect("client close") != 0 {}
        })
        .with(|| {
            let config = client::Config::new("127.0.0.1", "/ws");
            let config = config.expect("endpoint");
            let seen = Probe::new(addr, config).run(
                common::client::Goal::Close,
                recovery::Policy::new(|event| retired.push(event), common::recover),
            );
            assert!(matches!(
                seen.close(),
                Some(client::CloseReason::Decode(client::Fault::Open(_)))
            ));
        })
        .expect("malformed response probe");
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
    use sark_http::h1::{
        codec::decode::response,
        head::{Boundary, ParseError},
    };

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind interim peer");
    let addr = listener.local_addr().expect("interim address");
    Entry(move || {
        let (mut stream, _) = listener.accept().expect("accept interim client");
        app::Upgrade::new(&stream).try_head().expect("request head");
        let head = b"HTTP/1.1 103 Early Hints\r\n\r\n";
        stream
            .write_all(&head.repeat(16 * 1024 / head.len() + 1))
            .expect("interim heads");
        assert_eq!(stream.read(&mut [0; 1]).expect("rejected opening"), 0);
    })
    .with(|| {
        let seen = Probe::new(
            addr,
            client::Config::new("localhost", "/").expect("endpoint"),
        )
        .run(
            common::client::Goal::Close,
            recovery::Policy::new(|_| {}, common::recover),
        );
        assert!(matches!(
            seen.close(),
            Some(client::CloseReason::Decode(client::Fault::Open(
                response::Error::Head(ParseError::Boundary(Boundary::TooLarge { .. }))
            )))
        ));
        assert!(!seen.is_open());
    })
    .expect("interim peer");
}
