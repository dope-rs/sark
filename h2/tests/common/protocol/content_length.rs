use sark_http::field::{self, metadata::map::record::Entry};

use crate::common::{headers::entry, hpack::GET, response::HEAD};

pub const BODY: &[u8] = b"body";

pub const LENGTHS: [(&[u8], u64); 4] = [
    (b"3", 3),
    (b"4", 4),
    (b"5", 5),
    (b"18446744073709551615", u64::MAX),
];

pub const EMPTY_LENGTHS: [(&[u8], bool); 2] = [(b"0", true), (b"4", false)];

const REQUEST: [field::Entry<'static>; 4] = [
    field::literal!(entry(GET[0].name, GET[0].value)),
    field::literal!(entry(GET[1].name, GET[1].value)),
    field::literal!(entry(GET[2].name, GET[2].value)),
    field::literal!(entry(GET[3].name, GET[3].value)),
];

pub const EMPTY_RESPONSES: [(&[field::Entry<'static>], &[Entry<'static>]); 4] = [
    (
        &HEAD,
        &[
            Entry::new(b":status", b"200"),
            Entry::new(b"content-length", b"42"),
        ],
    ),
    (&REQUEST, &[Entry::new(b":status", b"204")]),
    (
        &REQUEST,
        &[
            Entry::new(b":status", b"304"),
            Entry::new(b"content-length", b"42"),
        ],
    ),
    (
        &REQUEST,
        &[
            Entry::new(b":status", b"205"),
            Entry::new(b"content-length", b"0"),
        ],
    ),
];

pub const FORBIDDEN_LENGTHS: [(&[field::Entry<'static>], &[u8], &[u8]); 3] = [
    (&REQUEST, b"103", b"0"),
    (&REQUEST, b"204", b"0"),
    (&REQUEST, b"205", b"1"),
];

pub const CONNECT_STATUSES: [&[u8]; 3] = [b"200", b"204", b"205"];
