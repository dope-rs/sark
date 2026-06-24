mod parse;

use http::{HeaderName, HeaderValue};
use parse::Framing;

use crate::error::{Error, Result};

const MAX_BODY_SIZE: usize = 100 * 1024 * 1024;

fn parse_chunk_size(bytes: &[u8]) -> Result<usize> {
    if bytes.is_empty() {
        return Err(Error::BadRequest("Empty chunk size".into()));
    }
    let mut size: usize = 0;
    for &b in bytes {
        let digit = match b {
            b'0'..=b'9' => (b - b'0') as usize,
            b'a'..=b'f' => (b - b'a' + 10) as usize,
            b'A'..=b'F' => (b - b'A' + 10) as usize,
            _ => return Err(Error::BadRequest("Invalid chunk size".into())),
        };
        size = size
            .checked_mul(16)
            .and_then(|s| s.checked_add(digit))
            .ok_or_else(|| Error::BadRequest("Chunk size overflow".into()))?;
    }
    Ok(size)
}

pub(crate) struct DecodeResult {
    pub(crate) body: Vec<u8>,
    pub(crate) trailers: Vec<(HeaderName, HeaderValue)>,
}

pub enum DecodeEvent {
    NeedMore,
    Chunk(Vec<u8>),
    Done(Vec<(HeaderName, HeaderValue)>),
}

enum State {
    SizeLine,
    Data(usize),
    Trailers,
    Done,
}

pub struct BodyDecoder {
    state: State,
    max_body: usize,
    body_len: usize,
}

impl Default for BodyDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BodyDecoder {
    pub fn new() -> Self {
        Self::with_limit(MAX_BODY_SIZE)
    }

    pub fn with_limit(max_body: usize) -> Self {
        Self {
            state: State::SizeLine,
            max_body,
            body_len: 0,
        }
    }

    pub fn decode(&mut self, buf: &[u8]) -> Result<(usize, DecodeEvent)> {
        let mut pos = 0;

        loop {
            match self.state {
                State::SizeLine => {
                    let crlf = match Framing::find_crlf(&buf[pos..]) {
                        Some(offset) => pos + offset,
                        None => return Ok((pos, DecodeEvent::NeedMore)),
                    };

                    let line = &buf[pos..crlf];
                    let size_bytes = match line.iter().position(|&b| b == b';') {
                        Some(semi) => &line[..semi],
                        None => line,
                    };
                    let chunk_size = parse_chunk_size(size_bytes)?;

                    if chunk_size > self.max_body || self.body_len + chunk_size > self.max_body {
                        return Err(Error::PayloadTooLarge(
                            "Chunked body exceeds size limit".into(),
                        ));
                    }

                    pos = crlf + 2;
                    if chunk_size == 0 {
                        self.state = State::Trailers;
                    } else {
                        self.state = State::Data(chunk_size);
                    }
                }
                State::Data(chunk_size) => {
                    if buf.len() < pos + chunk_size + 2 {
                        return Ok((pos, DecodeEvent::NeedMore));
                    }

                    let chunk = buf[pos..pos + chunk_size].to_vec();
                    if &buf[pos + chunk_size..pos + chunk_size + 2] != b"\r\n" {
                        return Err(Error::BadRequest("Invalid chunk terminator".into()));
                    }

                    pos += chunk_size + 2;
                    self.body_len += chunk_size;
                    self.state = State::SizeLine;
                    return Ok((pos, DecodeEvent::Chunk(chunk)));
                }
                State::Trailers => match Framing::parse_trailers(&buf[pos..])? {
                    Some((trailers, consumed)) => {
                        pos += consumed;
                        self.state = State::Done;
                        return Ok((pos, DecodeEvent::Done(trailers)));
                    }
                    None => return Ok((pos, DecodeEvent::NeedMore)),
                },
                State::Done => return Ok((0, DecodeEvent::Done(Vec::new()))),
            }
        }
    }
}

impl crate::http::codec::Parse {
    pub(crate) fn try_decode_chunked(buf: &[u8]) -> Result<Option<DecodeResult>> {
        Self::try_decode_chunked_limited(buf, MAX_BODY_SIZE)
    }

    pub(crate) fn try_decode_chunked_limited(
        buf: &[u8],
        max_body: usize,
    ) -> Result<Option<DecodeResult>> {
        let mut decoder = BodyDecoder::with_limit(max_body);
        let mut body = Vec::new();
        let mut input = buf;

        loop {
            let (consumed, event) = decoder.decode(input)?;
            input = &input[consumed..];

            match event {
                DecodeEvent::NeedMore => return Ok(None),
                DecodeEvent::Chunk(chunk) => body.extend_from_slice(&chunk),
                DecodeEvent::Done(trailers) => {
                    return Ok(Some(DecodeResult { body, trailers }));
                }
            }
        }
    }

    pub fn chunked_body(buf: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(Self::try_decode_chunked(buf)?.map(|r| r.body))
    }

    pub fn chunked_body_consumed(buf: &[u8], max_body: usize) -> Result<Option<(usize, Vec<u8>)>> {
        let mut decoder = BodyDecoder::with_limit(max_body);
        let mut body = Vec::new();
        let mut input = buf;
        let total = buf.len();

        loop {
            let (consumed, event) = decoder.decode(input)?;
            input = &input[consumed..];

            match event {
                DecodeEvent::NeedMore => return Ok(None),
                DecodeEvent::Chunk(chunk) => body.extend_from_slice(&chunk),
                DecodeEvent::Done(_) => {
                    let consumed_total = total - input.len();
                    return Ok(Some((consumed_total, body)));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_size_plus_prefix_rejected() {
        assert!(parse_chunk_size(b"+a").is_err());
    }

    #[test]
    fn chunk_size_minus_prefix_rejected() {
        assert!(parse_chunk_size(b"-a").is_err());
    }

    #[test]
    fn chunk_size_surrounding_whitespace_rejected() {
        assert!(parse_chunk_size(b" 5 ").is_err());
        assert!(parse_chunk_size(b"\t5").is_err());
        assert!(parse_chunk_size(b"5 ").is_err());
    }

    #[test]
    fn chunk_size_empty_rejected() {
        assert!(parse_chunk_size(b"").is_err());
    }

    #[test]
    fn chunk_size_valid_hex_accepted() {
        assert_eq!(parse_chunk_size(b"a").unwrap(), 10);
        assert_eq!(parse_chunk_size(b"1F").unwrap(), 31);
        assert_eq!(parse_chunk_size(b"0").unwrap(), 0);
    }

    #[test]
    fn chunk_size_huge_hex_overflows() {
        assert!(parse_chunk_size(b"ffffffffffffffffff").is_err());
    }

    #[test]
    fn chunk_extension_after_size_accepted() {
        let raw = b"5;name=value\r\nhello\r\n0\r\n\r\n";
        let body = crate::http::codec::Parse::chunked_body(raw)
            .unwrap()
            .unwrap();
        assert_eq!(body, b"hello");
    }

    #[test]
    fn normal_chunked_body_accepted() {
        let raw = b"5\r\nhello\r\n0\r\n\r\n";
        let body = crate::http::codec::Parse::chunked_body(raw)
            .unwrap()
            .unwrap();
        assert_eq!(body, b"hello");
    }

    #[test]
    fn chunk_size_whitespace_in_stream_rejected() {
        let raw = b" 5 \r\nhello\r\n0\r\n\r\n";
        assert!(crate::http::codec::Parse::chunked_body(raw).is_err());
    }
}
