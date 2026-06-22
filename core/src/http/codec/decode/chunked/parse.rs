use http::{HeaderName, HeaderValue};

use crate::error::{Error, Result};
use crate::http::codec::Parse;

pub(super) type Trailers = Vec<(HeaderName, HeaderValue)>;

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

        for line in trailer_block.split(|&b| b == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.is_empty() {
                continue;
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
