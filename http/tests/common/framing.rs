use sark_http::{
    field::{
        head::{self, Flags, headers::Known},
        metadata::map::record::Entry,
        text::IntError,
    },
    h1::{
        field::expect,
        framing::{Admission, RequestBody},
        head::{Semantic, line::Version, request::Fields},
        target,
    },
    payload::response::Method,
};

pub const NO_AUTHORITY: &[u8] = b"GET / HTTP/1.0\r\n\r\n";
pub const EXPECT_VERSIONS: &[(&[u8], bool)] = &[
    (
        b"POST / HTTP/1.0\r\nHost: local\r\nContent-Length: 1\r\nExpect: 100-continue\r\n\r\n",
        false,
    ),
    (
        b"POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\nExpect: 100-continue\r\n\r\n",
        true,
    ),
    (
        b"POST / HTTP/1.0\r\nHost: local\r\nContent-Length: 1\r\nExpect: 100-continue\r\nExpect: 100-continue\r\n\r\n",
        false,
    ),
    (
        b"POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\nExpect: \r\nExpect: 100-continue\r\nExpect: 100-continue\r\n\r\n",
        true,
    ),
];
pub const ACCEPT_GZIP: &[u8] = b"gzip";
pub const ENCODING_QUALITY: [(&[u8], (u16, u16, u16)); 5] = [
    (b"", (0, 0, 1000)),
    (b"x-gzip;q=0,*;q=0.8", (800, 800, 1000)),
    (b"br;q=1,gzip;q=0.4,identity;q=0.5", (1000, 400, 500)),
    (b"br;q=0.6", (600, 400, 500)),
    (b"gzip;q=0,*;q=0,identity;q=0", (600, 0, 0)),
];
pub const INVALID_GZIP_LINE: &[u8] = b"gzip;q=0, br;q=2\r\n";
pub const INVALID_CONNECTION_LINE: &[u8] = b" \t, bad/name, \t\r\n";
pub const LENGTH_LINES: &[(&[u8], usize, usize, usize)] = &[
    (b"123\r\n", 0, 3, 3),
    (b" 123\r\n", 1, 4, 4),
    (b" 123 \t\r\n", 1, 4, 6),
    (b"\t 123 \t\r\n", 2, 5, 7),
];
pub const INVALID_LENGTH_LINES: &[(&[u8], IntError)] = &[
    (b"123x\r\n", IntError::InvalidByte),
    (b"123 \rx", IntError::InvalidByte),
    (b"\r\n", IntError::InvalidByte),
    (b"99999999999999999999999999999\r\n", IntError::Overflow),
];

#[derive(PartialEq, Eq, Debug)]
pub enum Failure {
    Field(head::Error),
    Semantic(Semantic),
}

fn try_admit(scan: Fields, method: &[u8], target: &[u8]) -> Result<Admission, Failure> {
    let target = target::Target::parse(method, target)
        .map_err(|error| Failure::Semantic(Semantic::Authority(error)))?;
    let plan = scan
        .admit_target(target, b"HTTP/1.1", target::Policy::Require)
        .map_err(Failure::Semantic)?;
    scan.transition(Method::from_bytes(method), Version::Eleven)
        .map_err(Failure::Semantic)?;
    Ok(plan)
}

pub const AUTHORITY: &[u8] = b"origin.example:443";
pub const AUTHORITY_TARGET: &[u8] = b"/items?q=1";

pub fn expectation(mut scan: Fields) -> Result<expect::Decision, Failure> {
    let mut flags = Flags::default();
    for (known, value) in [
        (Known::ContentLength, b"1".as_slice()),
        (Known::Host, b"test.invalid"),
    ] {
        known
            .apply(&mut scan, &mut flags, value, Version::Eleven)
            .map_err(Failure::Field)?;
    }
    try_admit(scan, b"POST", b"/").map(|admit| admit.expectation())
}

fn request_fields<'a>(
    headers: impl IntoIterator<Item = (Known, &'a [u8])>,
) -> Result<Fields, head::Error> {
    let mut scan = Fields::default();
    let mut flags = Flags::default();
    for (header, value) in headers {
        header.apply(&mut scan, &mut flags, value, Version::Eleven)?;
    }
    Ok(scan)
}

const HOST: (Known, &[u8]) = (Known::Host, b"test.invalid");

pub fn try_headers(headers: &[(Known, &[u8])]) -> Result<RequestBody, Failure> {
    let scan = request_fields(headers.iter().copied().chain([HOST])).map_err(Failure::Field)?;
    try_body(scan, target::Policy::AllowEmpty).map_err(Failure::Semantic)
}

pub fn try_request(
    method: &[u8],
    target: &[u8],
    headers: &[(Known, &[u8])],
) -> Result<RequestBody, Failure> {
    let scan = request_fields(headers.iter().copied()).map_err(Failure::Field)?;
    try_admit(scan, method, target).map(|admit| admit.body())
}

pub fn try_scan(headers: &[Entry<'_>]) -> Result<Fields, head::Error> {
    let known = headers
        .iter()
        .filter_map(|entry| Some((Known::from_name(entry.name)?, entry.value)));
    request_fields(known.chain([HOST]))
}

pub fn try_body(fields: Fields, policy: target::Policy) -> Result<RequestBody, Semantic> {
    let target = target::Target::parse(b"GET", b"/").map_err(Semantic::Authority)?;
    fields
        .admit_target(target, b"HTTP/1.1", policy)
        .map(|admit| admit.body())
}
