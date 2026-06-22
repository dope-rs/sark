mod parse;

use http::{HeaderName, HeaderValue};
use parse::Framing;

use crate::error::{Error, Result};

const MAX_BODY_SIZE: usize = 100 * 1024 * 1024;

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

                    let size_str = std::str::from_utf8(&buf[pos..crlf])
                        .map_err(|_| Error::BadRequest("Invalid chunk size encoding".into()))?;
                    let size_str = size_str.split(';').next().unwrap_or("").trim();
                    let chunk_size = usize::from_str_radix(size_str, 16).map_err(|_| {
                        Error::BadRequest(format!("Invalid chunk size: {:?}", size_str).into())
                    })?;

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
