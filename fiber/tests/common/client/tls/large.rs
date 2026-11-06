use crate::common::client::runtime::{server::Peer, sockets::Error};

pub const BODY_LEN: usize = 504_651;

pub const BODY_CHUNK: usize = 0xC000;

pub const MARKETS_PATHS: [&str; 4] = [
    "/v1/descriptions/ko/markets.xml",
    "/v1/descriptions/ja/markets.xml",
    "/v1/descriptions/zh/markets.xml",
    "/v1/descriptions/en/markets.xml",
];

pub fn try_response(peer: &mut Peer) -> Result<(), Error> {
    peer.try_write(
        b"HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\nConnection: keep-alive\r\n\r\n",
    )?;
    let body = [b'x'; BODY_CHUNK];
    let mut left = BODY_LEN;
    while left != 0 {
        let len = left.min(body.len());
        peer.try_write(format!("{len:08X}\r\n").as_bytes())?;
        peer.try_write(&body[..len])?;
        peer.try_write(b"\r\n")?;
        left -= len;
    }
    peer.try_write(b"00000000\r\n\r\n")
}
