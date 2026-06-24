use http::{HeaderName, HeaderValue};

use crate::error::{Error, Result};
use crate::http::codec::Parse;

pub(super) type Trailers = Vec<(HeaderName, HeaderValue)>;

const MAX_TRAILER_COUNT: usize = 128;
const MAX_TRAILER_BYTES: usize = 16 * 1024;

pub(super) struct Framing;

impl Framing {
    pub(super) fn parse_trailers(buf: &[u8]) -> Result<Option<(Trailers, usize)>> {
        if buf.len() >= 2 && buf[0] == b'\r' && buf[1] == b'\n' {
            return Ok(Some((Vec::new(), 2)));
        }

        let end = match Parse::find_double_crlf(buf) {
            Some(r) => r.end,
            None => return Ok(None),
        };

        let trailer_block = &buf[..end];
        let mut trailers = Vec::new();
        let mut trailer_bytes: usize = 0;

        for line in trailer_block.split(|&b| b == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.is_empty() {
                continue;
            }
            trailer_bytes = trailer_bytes.saturating_add(line.len());
            if trailer_bytes > MAX_TRAILER_BYTES {
                return Err(Error::BadRequest("Trailer section too large".into()));
            }
            if let Some(colon) = line.iter().position(|&b| b == b':') {
                let name = &line[..colon];
                let value = &line[colon + 1..];
                let value = if value.first() == Some(&b' ') {
                    &value[1..]
                } else {
                    value
                };

                let name = HeaderName::from_bytes(name)
                    .map_err(|_| Error::BadRequest("Invalid trailer header name".into()))?;
                let value = HeaderValue::from_bytes(value)
                    .map_err(|_| Error::BadRequest("Invalid trailer header value".into()))?;
                if trailers.len() >= MAX_TRAILER_COUNT {
                    return Err(Error::BadRequest("Too many trailers".into()));
                }
                trailers.push((name, value));
            }
        }

        Ok(Some((trailers, end)))
    }

    pub(super) fn find_crlf(buf: &[u8]) -> Option<usize> {
        if buf.len() < 2 {
            return None;
        }
        let mut i = 0;
        while i + 1 < buf.len() {
            if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                return Some(i);
            }
            i += 1;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_trailers_accepted() {
        let (trailers, consumed) = Framing::parse_trailers(b"\r\n").unwrap().unwrap();
        assert!(trailers.is_empty());
        assert_eq!(consumed, 2);
    }

    #[test]
    fn single_trailer_accepted() {
        let (trailers, _) = Framing::parse_trailers(b"X-A: b\r\n\r\n").unwrap().unwrap();
        assert_eq!(trailers.len(), 1);
    }

    #[test]
    fn over_count_trailers_rejected() {
        let mut block = Vec::new();
        for i in 0..(MAX_TRAILER_COUNT + 5) {
            block.extend_from_slice(format!("X-{i}: v\r\n").as_bytes());
        }
        block.extend_from_slice(b"\r\n");
        assert!(Framing::parse_trailers(&block).is_err());
    }

    #[test]
    fn oversized_trailers_rejected() {
        let mut block = Vec::new();
        let big = "a".repeat(MAX_TRAILER_BYTES + 1);
        block.extend_from_slice(format!("X-Big: {big}\r\n").as_bytes());
        block.extend_from_slice(b"\r\n");
        assert!(Framing::parse_trailers(&block).is_err());
    }
}
