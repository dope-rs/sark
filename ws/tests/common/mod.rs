#![allow(dead_code)]
//! Shared harness for ws integration tests: spin up the real echo server and
//! talk to it over a socket with masked client frames.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use dope_extra::harness::Harness;
use sark_ws::crypto::Crypto;
use sark_ws::frame::FrameHead;
use sark_ws::mask::Mask;
use sark_ws::server::{self, Config, Message, Response};

const KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

/// A client-masked frame, ready to write to the server.
pub fn masked(opcode: u8, fin: bool, payload: &[u8]) -> Vec<u8> {
    let mask = [0x11u8, 0x22, 0x33, 0x44];
    let mut v = vec![if fin { 0x80 | opcode } else { opcode }];
    FrameHead::encode_len(&mut v, payload.len(), true);
    v.extend_from_slice(&mask);
    let start = v.len();
    v.extend_from_slice(payload);
    Mask::unmask_in_place(&mut v[start..], mask);
    v
}

/// Perform the upgrade handshake, asserting the 101 + accept, and return any
/// frame bytes that arrived past the response head.
pub fn handshake(sock: &mut TcpStream) -> Vec<u8> {
    let req = format!(
        "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: {KEY}\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    sock.write_all(req.as_bytes()).unwrap();

    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..end]).to_string();
            assert!(head.starts_with("HTTP/1.1 101"), "head: {head}");
            assert!(head.contains(&Crypto::expected_accept(KEY)), "head: {head}");
            return buf.split_off(end + 4);
        }
        let n = sock.read(&mut tmp).unwrap();
        assert!(n > 0, "closed during handshake");
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Read one server frame (any opcode), returning `(opcode, payload)`.
pub fn next_message(sock: &mut TcpStream, buf: &mut Vec<u8>) -> (u8, Vec<u8>) {
    let mut tmp = [0u8; 4096];
    loop {
        if let Ok(Some(h)) = FrameHead::parse(buf, 0, usize::MAX) {
            let msg = (h.opcode, buf[h.payload_start..h.payload_end].to_vec());
            buf.drain(..h.payload_end);
            return msg;
        }
        let n = sock.read(&mut tmp).unwrap();
        assert!(n > 0, "server closed before a full frame");
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Close code from a close-frame payload, if present.
pub fn close_code(payload: &[u8]) -> Option<u16> {
    (payload.len() >= 2).then(|| u16::from_be_bytes([payload[0], payload[1]]))
}

/// Connect + handshake, returning the socket and any leftover frame bytes.
pub fn connect(bind: SocketAddr) -> (TcpStream, Vec<u8>) {
    let mut sock = TcpStream::connect(bind).unwrap();
    sock.set_nodelay(true).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let buf = handshake(&mut sock);
    (sock, buf)
}

/// Run `client` against a fresh single-worker echo server.
pub fn run_echo<C, R>(client: C) -> R
where
    C: FnOnce(SocketAddr) -> R,
{
    let harness = Harness::bind().expect("harness");
    let bind = harness.addr();
    let cfg = Config {
        bind,
        max_connections: 16,
        backlog: 16,
        path: "/",
        max_frame_payload: 16 * 1024 * 1024,
    };
    harness
        .run_with_trigger(
            move |ctx, trigger| {
                let echo = |msg: Message<'_>, resp: &mut Response<'_>| match msg {
                    Message::Text(s) => {
                        let _ = resp.text(s);
                    }
                    Message::Binary(b) => {
                        let _ = resp.binary(b);
                    }
                };
                server::serve(echo, cfg.clone(), ctx, Some(trigger))
            },
            client,
        )
        .expect("harness")
}
