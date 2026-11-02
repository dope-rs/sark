use sark_http::field::metadata::block;

pub const AUTHORITIES: &[(&[u8], block::Validation)] = &[
    (b"example.test", block::Validation::Malformed),
    (b"example.test:", block::Validation::Malformed),
    (b"[::1]", block::Validation::Malformed),
    (b"[::1]:", block::Validation::Malformed),
    (b"example.test:443", block::Validation::Valid),
    (b"[::1]:443", block::Validation::Valid),
    (b"example.test:0", block::Validation::Valid),
    (b"example.test:65536", block::Validation::Valid),
];

pub const METHOD: &[u8] = b"CONNECT";
pub const PATH: &[u8] = b"/chat";
pub const SCHEME: &[u8] = b"https";
pub const PROTOCOL: &[u8] = b"websocket";
pub const HOST: &[u8] = b"example.test:443";
