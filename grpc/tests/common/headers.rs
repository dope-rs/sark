use o3::alloc::{self, VecExt};
use sark_grpc::{Code, metadata};
use sark_h2::conn;
use sark_http::field::{
    Indexing,
    metadata::map::{Owned, record::Entry},
};

pub const DETAILS: &[u8] = &[8, 3];
const DETAILS_ENCODED: &[u8] = b"CAM=";
pub const CONFLICT: (&[u8], &[u8]) = (b"3", b"CAc");
pub const DEFAULT_CODE: (&[u8], &[u8]) = (b"3", b"EgF4");
pub const OK_DETAILS: (&[u8], &[u8]) = (b"0", b"CAc");
pub const MALFORMED_DETAILS: (&[u8], &[u8]) = (b"3", b"CA");
pub const BAD_BASE64_DETAILS: (&[u8], &[u8]) = (b"0", b"A");
pub const BAD_BINARY: [&[u8]; 4] = [b"@A", b"YQ,@A", b"@A,YQ", b"YQ,@A,YQ"];
pub const RAW_CONTROL_VALUES: [&[u8]; 3] = [b"raw\rvalue", b"raw\nvalue", b"raw\0value"];
pub const GRPC_VALUE_FIELDS: [&[u8]; 2] = [b"grpc-message", b"grpc-ignored"];
pub const INVALID_STATUS_VALUES: [(&[u8], Code); 7] = [
    (b"", Code::Internal),
    (b"-1", Code::Internal),
    (b"+0", Code::Internal),
    (b" 0", Code::InvalidArgument),
    (b"0 ", Code::InvalidArgument),
    (b"a", Code::Internal),
    (b"256", Code::Internal),
];

pub const LARGE: &[u8] = &{
    let mut bytes = [0; 3_077];
    let (head, body) = bytes.split_at_mut(5);
    head.copy_from_slice(&[8, 3, 82, 128, 24]);
    let mut index = 0;
    while index < body.len() {
        body[index] = (index * 37 % 256) as u8;
        index += 1;
    }
    bytes
};

pub const PAYLOAD: &[u8] = LARGE.split_at(5).1;

pub const CONNECTION: conn::Error = conn::Error::StreamLimit {
    active: 2,
    limit: 1,
};

const VALUE: [u8; 64] = [b'a'; 64];

pub const JOINED: [(&[u8], &[u8], Indexing); 5] = [
    (b"trace-bin", b"", Indexing::Never),
    (b"trace-bin", &VALUE, Indexing::Never),
    (b"trace-bin", b"", Indexing::Never),
    (b"trace-bin", b"", Indexing::Never),
    (b"x-after", b"after", Indexing::Allowed),
];

pub fn try_details((code, details): (&[u8], &[u8])) -> Result<Vec<Owned>, alloc::Exhausted> {
    Ok(vec![
        Owned::try_new(b"grpc-status", code)?,
        Owned::try_new(b"grpc-status-details-bin", details)?,
    ])
}

pub fn try_binary(value: &[u8]) -> Result<Vec<Owned>, alloc::Exhausted> {
    Ok(vec![
        Owned::try_new(b"grpc-status", b"0")?,
        Owned::try_new(b"trace-bin", value)?,
    ])
}

pub fn try_joined() -> Result<Vec<Owned>, alloc::Exhausted> {
    let mut value = vec![b','];
    value.extend_from_slice(&b"YWFh".repeat(21));
    value.extend_from_slice(b"YQ,,");
    Ok(vec![
        Owned::try_new(b"trace-bin", &value)?.with_index(Indexing::Never),
        Owned::try_new(b"x-after", b"after")?,
        Owned::try_new(b"grpc-status", b"0")?,
    ])
}

pub fn try_status() -> Result<Vec<Owned>, alloc::Exhausted> {
    Ok(vec![
        Owned::try_new(b"grpc-status", b"3")?.with_index(Indexing::Never),
        Owned::try_new(b"grpc-message", b"terminal%20%E2%98%83%0A")?.with_index(Indexing::Never),
        Owned::try_new(b"grpc-status-details-bin", DETAILS_ENCODED)?.with_index(Indexing::Never),
        Owned::try_new(b"x-trace", b"42")?,
    ])
}

pub fn try_request(extra: impl IntoIterator<Item = Owned>) -> Result<Vec<Owned>, alloc::Error> {
    try_fields(
        [
            Entry::new(b":method", b"POST"),
            Entry::new(b":scheme", b"https"),
            Entry::new(b":path", b"/service/method"),
            Entry::new(b"content-type", b"application/grpc+proto"),
            Entry::new(b"te", b"trailers"),
        ],
        extra,
    )
}

pub fn try_response(extra: impl IntoIterator<Item = Owned>) -> Result<Vec<Owned>, alloc::Error> {
    try_fields(
        [
            Entry::new(b":status", b"200"),
            Entry::new(b"content-type", b"application/grpc+proto"),
        ],
        extra,
    )
}

pub fn try_trailers(extra: impl IntoIterator<Item = Owned>) -> Result<Vec<Owned>, alloc::Error> {
    try_fields([Entry::new(b"grpc-status", b"0")], extra)
}

fn try_fields<const N: usize>(
    base: [Entry<'_>; N],
    extra: impl IntoIterator<Item = Owned>,
) -> Result<Vec<Owned>, alloc::Error> {
    let mut fields = Vec::try_new(N)?;
    for field in base {
        fields.push(Owned::try_from(field)?);
    }
    for field in extra {
        VecExt::try_reserve(&mut fields, 1)?;
        fields.push(field);
    }
    Ok(fields)
}

pub fn try_sensitive_metadata() -> Result<metadata::Metadata, metadata::Error> {
    let mut metadata = metadata::Metadata::new();
    metadata.push_entry(Entry::new(b"trace-bin", &[0, 1, 2, 255]).with_index(Indexing::Never))?;
    Ok(metadata)
}
