use o3::alloc;
use sark_http::h1::{codec::decode::request::Head, head::ParseError};
use sark_ws::upgrade::{self, request::Admission};

use crate::common::echo::OPENING;

pub const BAD_KEY: &[u8] = b"GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: bad\r\nSec-WebSocket-Version: 13\r\n\r\n";
pub const BAD_PATH: &[u8] = b"GET /other HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";

pub fn with_headers(fields: &[u8]) -> Vec<u8> {
    let mut request = OPENING[..OPENING.len() - 2].to_vec();
    request.extend_from_slice(fields);
    request.extend_from_slice(b"\r\n");
    request
}

/// Pads a complete opening to 16 KiB plus `extra`, keeping every field below 8 KiB.
pub fn try_padded(extra: u8) -> Result<Vec<u8>, alloc::Error> {
    let length = 16 * 1024 + usize::from(extra);
    let mut request = Vec::new();
    alloc::VecExt::try_reserve(&mut request, length)?;
    request.extend_from_slice(&OPENING[..OPENING.len() - 2]);
    for _ in 0..3 {
        request.extend_from_slice(b"X-Padding: ");
        request.extend_from_slice(&[b'a'; 4096]);
        request.extend_from_slice(b"\r\n");
    }
    request.extend_from_slice(b"X-Padding: ");
    request.resize(length - 4, b'a');
    request.extend_from_slice(b"\r\n\r\n");
    Ok(request)
}

#[derive(PartialEq, Eq)]
pub enum Error {
    Decode(ParseError<upgrade::Error>),
    Incomplete { actual: usize },
    Handshake(upgrade::Error),
}

pub trait Admit {
    fn try_admit(&self, path: &str) -> Result<[u8; 28], Error>;
}

impl Admit for [u8] {
    fn try_admit(&self, path: &str) -> Result<[u8; 28], Error> {
        let head = Head::parse(self, self.len(), Admission::default())
            .map_err(Error::Decode)?
            .ok_or(Error::Incomplete { actual: self.len() })?;
        head.fields
            .finish(
                head.method,
                head.target,
                head.version,
                head.admission.body(),
                head.connection,
                path.as_bytes(),
            )
            .map_err(Error::Handshake)
    }
}
