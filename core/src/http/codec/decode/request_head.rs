use std::ops::Range;

#[derive(Clone, Copy, Debug)]
pub struct ParsedRequestHead<'a> {
    pub method: &'a [u8],
    pub target: &'a [u8],
    pub version: &'a [u8],
    pub headers_start: usize,
}

impl crate::http::codec::Parse {
    pub fn find_double_crlf(bytes: &[u8]) -> Option<Range<usize>> {
        memchr::memmem::find(bytes, b"\r\n\r\n").map(|s| s..s + 4)
    }
}
