pub const EMPTY: &[u8; 18] = b"GET / HTTP/1.1\r\n\r\n";
pub const DEFAULT: &[u8; 4] = b"none";
pub const GZIP: &[u8; 25] = b"Accept-Encoding: gzip\r\n\r\n";
pub const DUPLICATE_EXPECT: &[u8; 65] =
    b"Content-Length: 4\r\nExpect: 100-continue\r\nExpect: 100-continue\r\n\r\n";
pub const HOST: &[u8; 40] = b"GET / HTTP/1.1\r\nHost: origin.example\r\n\r\n";
pub const ABSOLUTE: &[u8; 52] = b"GET https://origin.example/items HTTP/1.1\r\nHost:\r\n\r\n";
pub const OPTIONS: &[u8; 44] = b"OPTIONS * HTTP/1.1\r\nHost: origin.example\r\n\r\n";
pub const EMPTY_HOST: &[u8; 25] = b"GET / HTTP/1.1\r\nHost:\r\n\r\n";
pub const DUPLICATE_HOST: &[u8; 56] =
    b"GET / HTTP/1.1\r\nHost: one.example\r\nHost: two.example\r\n\r\n";
pub const USER_HOST: &[u8; 45] = b"GET / HTTP/1.1\r\nHost: user@origin.example\r\n\r\n";
pub const TEXT_PORT: &[u8; 45] = b"GET / HTTP/1.1\r\nHost: origin.example:http\r\n\r\n";
pub const ASTERISK: &[u8; 40] = b"GET * HTTP/1.1\r\nHost: origin.example\r\n\r\n";
pub const USER_TARGET: &[u8; 63] =
    b"GET https://user@origin.example/items HTTP/1.1\r\nHost: local\r\n\r\n";
pub const ASTERISK_PREFIX: &[u8; 6] = b"GET * ";
pub const ORIGIN: &[u8; 45] = b"GET /items HTTP/1.1\r\nHost: origin.example\r\n\r\n";
pub const ORIGIN_HOST: &[u8; 14] = b"origin.example";
pub const TARGET: &[u8; 68] =
    b"GET https://target.example/items HTTP/1.1\r\nHost: ignored.example\r\n\r\n";
pub const TARGET_HOST: &[u8; 14] = b"target.example";
pub const PREFIXES: &[u8; 63] =
    b"GET / HTTP/1.1\r\nHost: [2001:db8::1]:443\r\nX-Note: split-safe\r\n\r\n";
pub const BARE_LF: &[u8; 21] = b"X-Token: good\nbad\r\n\r\n";
pub const SHORT: &[u8; 12] = b"x: \tok\t \r\n\r\n";
pub const NAME: &[u8; 7] = b"x-token";
pub const NAME_FIRST: &[u8; 4] = b"x-to";
pub const NAME_LAST: &[u8; 3] = b"ken";
pub const LF_VALUE: &[u8; 22] = b"X-Smuggle: foo\nbar\r\n\r\n";
pub const CR_VALUE: &[u8; 22] = b"X-Smuggle: foo\rbar\r\n\r\n";
pub const NUL_VALUE: &[u8; 22] = b"X-Smuggle: foo\x00bar\r\n\r\n";
pub const BELL_VALUE: &[u8; 22] = b"X-Smuggle: foo\x07bar\r\n\r\n";
pub const DEL_VALUE: &[u8; 22] = b"X-Smuggle: foo\x7fbar\r\n\r\n";
pub const VALID: &[u8; 55] = b"Host: example.com\r\nUser-Agent: x/1\r\nX-Note: foo\tbar\r\n\r\n";
pub const AGENT: &[u8; 12] = b"User-Agent: ";
const TERMINATOR: &[u8; 4] = b"\r\n\r\n";
pub const PARTIAL: &[u8; 27] = b"User-Agent: 12345678901234\r";
pub const BLANK: &[u8; 2] = b"\r\n";
pub const HOST_LINE: &[u8; 21] = b"Host: example.com\r\n\r\n";
pub const LONG: &[u8; 8] = b"X-Long: ";
pub const CR_NAME: &[u8; 15] = b"x\rtoken: ok\r\n\r\n";
pub const FOLD_NAME: &[u8; 15] = b"x-token\x1a ok\r\n\r\n";
pub const MIXED_NAME: &[u8; 15] = b"X-ToKeN: ok\r\n\r\n";
pub const UNORDERED: &[u8; 28] = b"/?limit=7&flag=true&count=41";
pub const ORDERED: &[u8; 18] = b"/?count=41&limit=7";
pub const QUERY_PREFIX: &[u8; 20] = b"countdown=41&limit=7";
pub const QUERY: &[u8; 8] = b"count=41";
pub const BAD_QUERY: &[u8; 23] = b"/path?count=41&flag=bad";

pub trait Headers {
    fn padded(&self, length: usize) -> Vec<u8>;
}

impl Headers for [u8] {
    fn padded(&self, length: usize) -> Vec<u8> {
        let mut field = Vec::from(self);
        field.resize(field.len() + length, b'a');
        field.extend_from_slice(TERMINATOR);
        field
    }
}
