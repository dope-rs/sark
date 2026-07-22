use http::{HeaderName, HeaderValue};

use crate::error::{Error, Result};
use crate::http::codec::ParsedRequestHead;

const MAX_BODY_SIZE: usize = 100 * 1024 * 1024;
const MAX_TRAILER_COUNT: usize = 128;
const MAX_TRAILER_BYTES: usize = 16 * 1024;

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

pub enum DecodeEvent<'a> {
    NeedMore,
    Chunk(&'a [u8]),
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
    pub const DEFAULT_MAX_BODY: usize = MAX_BODY_SIZE;

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

    pub fn decode<'a>(&mut self, buf: &'a [u8]) -> Result<(usize, DecodeEvent<'a>)> {
        let mut pos = 0;

        loop {
            match self.state {
                State::SizeLine => {
                    let crlf = match Self::find_crlf(&buf[pos..]) {
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

                    let chunk = &buf[pos..pos + chunk_size];
                    if &buf[pos + chunk_size..pos + chunk_size + 2] != b"\r\n" {
                        return Err(Error::BadRequest("Invalid chunk terminator".into()));
                    }

                    pos += chunk_size + 2;
                    self.body_len += chunk_size;
                    self.state = State::SizeLine;
                    return Ok((pos, DecodeEvent::Chunk(chunk)));
                }
                State::Trailers => match Self::parse_trailers(&buf[pos..])? {
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

    pub(crate) fn decode_all(buf: &[u8], max_body: usize) -> Result<Option<DecodeResult>> {
        let mut decoder = BodyDecoder::with_limit(max_body);
        let mut body = Vec::new();
        let mut input = buf;

        loop {
            let (consumed, event) = decoder.decode(input)?;
            input = &input[consumed..];

            match event {
                DecodeEvent::NeedMore => return Ok(None),
                DecodeEvent::Chunk(chunk) => body.extend_from_slice(chunk),
                DecodeEvent::Done(trailers) => {
                    return Ok(Some(DecodeResult { body, trailers }));
                }
            }
        }
    }

    pub fn body(buf: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(Self::decode_all(buf, MAX_BODY_SIZE)?.map(|result| result.body))
    }

    pub fn body_consumed(
        buf: &[u8],
        max_body: usize,
    ) -> Result<Option<(usize, o3::buffer::Shared)>> {
        let mut decoder = BodyDecoder::with_limit(max_body);
        let mut body = Vec::new();
        let mut input = buf;
        let total = buf.len();

        loop {
            let (consumed, event) = decoder.decode(input)?;
            input = &input[consumed..];

            match event {
                DecodeEvent::NeedMore => return Ok(None),
                DecodeEvent::Chunk(chunk) => body.extend_from_slice(chunk),
                DecodeEvent::Done(_) => {
                    let consumed_total = total - input.len();
                    return Ok(Some((consumed_total, o3::buffer::Shared::from(body))));
                }
            }
        }
    }

    fn parse_trailers(buf: &[u8]) -> Result<Option<(Vec<(HeaderName, HeaderValue)>, usize)>> {
        if buf.starts_with(b"\r\n") {
            return Ok(Some((Vec::new(), 2)));
        }

        let end = match ParsedRequestHead::head_end(buf) {
            Some(range) => range.end,
            None => return Ok(None),
        };
        let mut trailers = Vec::new();
        let mut trailer_bytes = 0usize;
        for line in buf[..end].split(|&byte| byte == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.is_empty() {
                continue;
            }
            trailer_bytes = trailer_bytes.saturating_add(line.len());
            if trailer_bytes > MAX_TRAILER_BYTES {
                return Err(Error::BadRequest("Trailer section too large".into()));
            }
            if let Some(colon) = line.iter().position(|&byte| byte == b':') {
                let name = HeaderName::from_bytes(&line[..colon])
                    .map_err(|_| Error::BadRequest("Invalid trailer header name".into()))?;
                let value = line[colon + 1..]
                    .strip_prefix(b" ")
                    .unwrap_or(&line[colon + 1..]);
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

    fn find_crlf(buf: &[u8]) -> Option<usize> {
        buf.windows(2).position(|window| window == b"\r\n")
    }
}
