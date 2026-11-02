pub mod common;
use common::upgrade::server::{Admit as _, Error};
use dope_test::alloc::Tracker;
use sark_ws::upgrade;

#[global_allocator]
static ALLOCATOR: Tracker = Tracker::new();

#[test]
fn lists_ignore_order() {
    for fields in [
        b"Connection: Upgrade\r\nConnection: keep-alive\r\nUpgrade: h2c\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
            .as_slice(),
        b"Upgrade: websocket\r\nUpgrade: h2c\r\nConnection: keep-alive\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
    ] {
        let req = [b"GET /socket HTTP/1.1\r\nHost: origin.example\r\n".as_slice(), fields, b"\r\n"].concat();
        assert!(req.try_admit("/socket") == Ok(*b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
    }
}

#[test]
fn rejects_invalid_openings() {
    for bad in [
        b"GET /socket HTTP/1.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n".as_slice(),
        b"GET /socket HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\nx",
        b"GET /socket HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
        b"GET /socket HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZ!==\r\nSec-WebSocket-Version: 13\r\n\r\n",
        b"GET /socket HTTP/1.1\r\nHost: x\r\nConnection: close, Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
    ] {
        assert!(bad.try_admit("/socket").is_err());
    }
}

#[test]
fn rejects_versioned_token() {
    let req = b"GET /socket HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket/1\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
    assert!(matches!(
        req.try_admit("/socket"),
        Err(Error::Handshake(upgrade::Error::Protocol))
    ));
}

#[test]
fn admission_does_not_alloc() {
    let req = b"GET /socket HTTP/1.1\r\nHost: origin.example\r\nConnection: keep-alive, Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";

    let (result, alloc) = Tracker::usage(|| req.try_admit("/socket"));

    assert!(result == Ok(*b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
    assert_eq!(alloc, (0, 0));
}
