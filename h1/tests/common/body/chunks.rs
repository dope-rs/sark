use std::num::NonZero;

use dope::{core::io::transfer, net::link::egress::data};
use o3::buffer::{self, bytes::bounded::Overflow};
use sark_h1::server::payload::Frame;

pub const FRAMING: &[(&[u8], &[u8])] = &[
    (b"a".as_slice(), b"1\r\na\r\n".as_slice()),
    (
        b"123456789abcdef".as_slice(),
        b"f\r\n123456789abcdef\r\n".as_slice(),
    ),
    (
        b"123456789abcdef0".as_slice(),
        b"10\r\n123456789abcdef0\r\n".as_slice(),
    ),
];

pub fn try_collect(frame: &Frame<'_>, cap: NonZero<usize>) -> Option<Vec<u8>> {
    use buffer::raw::Scatter as _;
    use data::raw::Payload as _;
    let mut chunks = frame.cursor(data::Offset::ZERO)?;
    let mut wire = Vec::with_capacity(frame.len());
    while let Some(chunk) = chunks.take_bytes(cap.get()).next() {
        wire.extend_from_slice(chunk.bytes());
    }
    Some(wire)
}

pub fn try_position(bytes: usize) -> Result<data::Offset, Overflow> {
    Ok(data::Offset::from_len(transfer::Len::try_new(bytes)?))
}
