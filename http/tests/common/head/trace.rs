use o3::buffer::{storage, write::consume};
use sark_http::h1::head::{
    Limit, ParseError,
    request::{Parts, trace},
};

#[derive(Debug)]
pub enum Build {
    Capacity { length: usize, spare: usize },
    Allocation(storage::AllocError),
    Write(consume::Error),
    Parse(ParseError<trace::Error>),
}

pub const REPLACEMENTS: &[&[u8]] = &[
    b"POST /items HTTP/1.1\r\ntraceparent: x\r\nHost: example.test\r\nContent-Length: 4\r\n\r\nbody",
    b"POST /items HTTP/1.1\r\nHost: example.test\r\ntraceparent:\r\nContent-Length: 4\r\n\r\nbody",
    b"POST http://example.test/items HTTP/1.1\r\ntraceparent: malformed\r\nHost: example.test\r\nContent-Length: 4\r\n\r\nbody",
    b"POST /items HTTP/1.1\r\ntraceparent: this-invalid-parent-is-longer-than-the-replacement-value-and-retains-padding\r\nHost: example.test\r\nContent-Length: 4\r\n\r\nbody",
];

pub const CONTEXTS: &[(&[u8], bool)] = &[
    (
        b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        true,
    ),
    (
        b"01-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        true,
    ),
    (
        b"0a-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01-opaque",
        true,
    ),
    (
        b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01-opaque",
        false,
    ),
    (
        b"01-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01opaque",
        false,
    ),
    (
        b"ff-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        false,
    ),
    (
        b"0A-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        false,
    ),
    (
        b"00-Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        false,
    ),
    (
        b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-Bbbbbbbbbbbbbbbb-01",
        false,
    ),
    (
        b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-0A",
        false,
    ),
];

pub trait Read: Sized {
    fn try_read(wire: &[u8], spare: usize) -> Result<Option<Self>, Build>;
}

impl Read for trace::Owned {
    fn try_read(wire: &[u8], spare: usize) -> Result<Option<Self>, Build> {
        let capacity = wire.len().checked_add(spare).ok_or(Build::Capacity {
            length: wire.len(),
            spare,
        })?;
        let mut bytes = storage::Owned::try_with(capacity).map_err(Build::Allocation)?;
        bytes.try_extend(wire).map_err(Build::Write)?;
        let parts = Parts::traced(bytes, Limit::DEFAULT).map_err(Build::Parse)?;
        Ok(parts.map(Parts::into_owned))
    }
}
