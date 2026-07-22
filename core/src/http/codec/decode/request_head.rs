use std::ops::Range;

#[derive(Clone, Copy, Debug)]
pub struct ParsedRequestHead<'a> {
    pub method: &'a [u8],
    pub target: &'a [u8],
    pub version: &'a [u8],
    pub headers_start: usize,
}

impl ParsedRequestHead<'_> {
    pub fn parse(buf: &[u8]) -> Option<ParsedRequestHead<'_>> {
        let start_line_end = memchr::memchr(b'\r', buf)?;
        if start_line_end + 1 >= buf.len() || buf[start_line_end + 1] != b'\n' {
            return None;
        }
        let line = &buf[..start_line_end];
        if line.len() < 9 {
            return None;
        }
        let version_start = line.len() - 8;
        if version_start == 0 || line[version_start - 1] != b' ' {
            return None;
        }
        let method_end = line.iter().position(|&byte| byte == b' ')?;
        if method_end == 0 || method_end >= version_start - 1 {
            return None;
        }
        let method = &line[..method_end];
        let target = &line[method_end + 1..version_start - 1];
        let version = &line[version_start..];
        if target.is_empty()
            || (version != b"HTTP/1.1" && version != b"HTTP/1.0")
            || !crate::simd::request_target_is_valid(target)
        {
            return None;
        }
        Some(ParsedRequestHead {
            method,
            target,
            version,
            headers_start: start_line_end + 2,
        })
    }

    pub fn head_end(bytes: &[u8]) -> Option<Range<usize>> {
        memchr::memmem::find(bytes, b"\r\n\r\n").map(|s| s..s + 4)
    }
}
