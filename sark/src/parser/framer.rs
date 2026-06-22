pub type ParsedHead<'buf> = sark_core::http::codec::decode::ParsedRequestHead<'buf>;

pub struct Http;

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
        Some((&line[..sp1], &line[sp1 + 1..sp2], &line[sp2 + 1..]))
    }
}
