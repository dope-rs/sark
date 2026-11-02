use sark_http::field::metadata::block;

pub const VALID: &[(&[u8], &[u8], block::Response)] = &[
    (b"1", b"03", block::Response::Informational),
    (b"20", b"0", block::Response::Final),
];

pub const INVALID: &[&[u8]] = &[b"101", b"099", b"600", b"20", b"2000", b"2x0"];
