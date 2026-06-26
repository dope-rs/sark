pub type ParsedHead<'buf> = sark_core::http::codec::decode::ParsedRequestHead<'buf>;

use crate::service::Key;

pub struct Http;

pub struct FusedHead<'buf> {
    pub head: ParsedHead<'buf>,
    pub method_key: Key,
}

impl Http {
    pub fn parse_head(buf: &[u8]) -> Option<ParsedHead<'_>> {
        let start_line_end = memchr::memchr(b'\r', buf)?;
        if start_line_end + 1 >= buf.len() || buf[start_line_end + 1] != b'\n' {
            return None;
        }
        let (method, target, version) = Self::split_start_line_parts(&buf[..start_line_end])?;
        Some(ParsedHead {
            method,
            target,
            version,
            headers_start: start_line_end + 2,
        })
    }

    pub fn parse_head_fused(buf: &[u8]) -> Option<FusedHead<'_>> {
        let cr = memchr::memchr(b'\r', buf)?;
        if cr + 1 >= buf.len() || buf[cr + 1] != b'\n' {
            return None;
        }
        let line = &buf[..cr];
        if line.len() < 9 {
            return None;
        }
        let (method_key, sp1) = Self::method_word(line);
        if sp1 >= line.len() {
            return None;
        }
        let method = &line[..sp1];
        if method.is_empty() {
            return None;
        }
        let target_start = sp1 + 1;
        let rel = memchr::memchr(b' ', &line[target_start..])?;
        let sp2 = target_start + rel;
        let target = &line[target_start..sp2];
        if target.is_empty() {
            return None;
        }
        if target.iter().any(|&b| b <= 0x20 || b == 0x7f) {
            return None;
        }
        let version = &line[sp2 + 1..];
        if !Self::version_ok(version) {
            return None;
        }
        Some(FusedHead {
            head: ParsedHead {
                method,
                target,
                version,
                headers_start: cr + 2,
            },
            method_key,
        })
    }

    fn method_word(line: &[u8]) -> (Key, usize) {
        // Caller (`parse_head_fused`) guarantees `line.len() >= 9`, so the
        // leading word and the trailing-space probes below are always in bounds.
        let w4 = u32::from_le_bytes([line[0], line[1], line[2], line[3]]);
        // `*_SP` constants fold the trailing space into the 4-byte word (3-char
        // methods); the rest match the word and probe the space separately.
        const GET_SP: u32 = u32::from_le_bytes(*b"GET ");
        const PUT_SP: u32 = u32::from_le_bytes(*b"PUT ");
        const POST: u32 = u32::from_le_bytes(*b"POST");
        const HEAD: u32 = u32::from_le_bytes(*b"HEAD");
        const PATC: u32 = u32::from_le_bytes(*b"PATC");
        const DELE: u32 = u32::from_le_bytes(*b"DELE");
        const OPTI: u32 = u32::from_le_bytes(*b"OPTI");
        match w4 {
            GET_SP => return (Key::Get, 3),
            PUT_SP => return (Key::Put, 3),
            POST if line[4] == b' ' => return (Key::Post, 4),
            HEAD if line[4] == b' ' => return (Key::Head, 4),
            PATC if line[5] == b' ' && line[4] == b'H' => return (Key::Patch, 5),
            DELE if line[6] == b' '
                && u16::from_le_bytes([line[4], line[5]]) == u16::from_le_bytes(*b"TE") =>
            {
                return (Key::Delete, 6);
            }
            OPTI if line[7] == b' '
                && u32::from_le_bytes([line[3], line[4], line[5], line[6]])
                    == u32::from_le_bytes(*b"IONS") =>
            {
                return (Key::Options, 7);
            }
            _ => {}
        }
        match memchr::memchr(b' ', line) {
            Some(sp1) => (Key::from_bytes(&line[..sp1]), sp1),
            None => (Key::Other, line.len()),
        }
    }

    fn version_ok(version: &[u8]) -> bool {
        if version.len() != 8 {
            return false;
        }
        let w = u64::from_le_bytes([
            version[0], version[1], version[2], version[3], version[4], version[5], version[6],
            version[7],
        ]);
        const PREFIX: u64 = u64::from_le_bytes(*b"HTTP/1.0");
        const MASK: u64 = 0x00ff_ffff_ffff_ffff;
        if w & MASK != PREFIX & MASK {
            return false;
        }
        let last = version[7];
        last == b'0' || last == b'1'
    }

    fn split_start_line_parts(line: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
        if line.len() < 9 {
            return None;
        }
        let sp2 = line.len() - 9;
        if line[sp2] != b' ' {
            return None;
        }
        let sp1 = line.iter().position(|&b| b == b' ')?;
        if sp1 >= sp2 {
            return None;
        }
        let method = &line[..sp1];
        let target = &line[sp1 + 1..sp2];
        let version = &line[sp2 + 1..];
        if version != b"HTTP/1.1" && version != b"HTTP/1.0" {
            return None;
        }
        if method.is_empty() || target.is_empty() {
            return None;
        }
        // origin-form target: all printable, non-space (reject SP/CTL/DEL).
        if target.iter().any(|&b| b <= 0x20 || b == 0x7f) {
            return None;
        }
        Some((method, target, version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(line: &str) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        Http::split_start_line_parts(line.as_bytes())
            .map(|(m, t, v)| (m.to_vec(), t.to_vec(), v.to_vec()))
    }

    fn parts_bytes(line: &[u8]) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        Http::split_start_line_parts(line).map(|(m, t, v)| (m.to_vec(), t.to_vec(), v.to_vec()))
    }

    #[test]
    fn valid_request_lines_parse() {
        let valid = [
            "GET /path?q=1 HTTP/1.1",
            "POST /a/b HTTP/1.0",
            "OPTIONS * HTTP/1.1",
            "GET / HTTP/1.1",
            "GET /a%20b%2Fc HTTP/1.1",
            "DELETE /resource/123 HTTP/1.0",
            "PUT /x?y=z&a=b%26c HTTP/1.1",
            "GET /a/very/long/path/that/keeps/going/and/going/and/going/forever HTTP/1.1",
            "PATCH /v1/items/42 HTTP/1.1",
        ];
        for line in valid {
            assert!(parts(line).is_some(), "expected parse: {line:?}");
        }
        let (m, t, v) = parts("GET /path?q=1 HTTP/1.1").unwrap();
        assert_eq!(m, b"GET");
        assert_eq!(t, b"/path?q=1");
        assert_eq!(v, b"HTTP/1.1");
    }

    #[test]
    fn rejects_bad_version() {
        for line in [
            "GET / HTTP/0.9",
            "GET / HTTP/2.0",
            "GET / GARBAGE!",
            "GET / http/1.1",
            "GET / HTTP/1.2",
            "GET / hTTP/1.1",
        ] {
            assert!(parts(line).is_none(), "expected reject: {line:?}");
        }
    }

    #[test]
    fn rejects_empty_version() {
        // 8 trailing bytes are all spaces => version is empty after the delimiter.
        assert!(parts_bytes(b"GET /         ").is_none());
    }

    #[test]
    fn rejects_target_with_space() {
        assert!(parts_bytes(b"GET /a b HTTP/1.1").is_none());
        assert!(parts_bytes(b"GET /a  /b HTTP/1.1").is_none());
    }

    #[test]
    fn rejects_target_with_control_and_special_bytes() {
        assert!(parts_bytes(b"GET /a\x00b HTTP/1.1").is_none());
        assert!(parts_bytes(b"GET /a\nb HTTP/1.1").is_none());
        assert!(parts_bytes(b"GET /a\rb HTTP/1.1").is_none());
        assert!(parts_bytes(b"GET /a\tb HTTP/1.1").is_none());
        assert!(parts_bytes(b"GET /a\x7fb HTTP/1.1").is_none());
        assert!(parts_bytes(b"GET /a\x01b HTTP/1.1").is_none());
        assert!(parts_bytes(b"GET /a\x1fb HTTP/1.1").is_none());
    }

    #[test]
    fn rejects_empty_method() {
        assert!(parts_bytes(b" / HTTP/1.1").is_none());
    }

    #[test]
    fn rejects_empty_target() {
        assert!(parts_bytes(b"GET  HTTP/1.1").is_none());
    }

    #[test]
    fn parse_head_bare_lf_in_target_rejected() {
        // memchr(\r) does not stop at an inner \n; the \n must be caught by target validation.
        let buf = b"GET /a\nb HTTP/1.1\r\nHost: x\r\n\r\n";
        assert!(Http::parse_head(buf).is_none());
    }

    #[test]
    fn parse_head_valid() {
        let buf = b"GET /index.html HTTP/1.1\r\nHost: x\r\n\r\n";
        let head = Http::parse_head(buf).unwrap();
        assert_eq!(head.method, b"GET");
        assert_eq!(head.target, b"/index.html");
        assert_eq!(head.version, b"HTTP/1.1");
    }
}
