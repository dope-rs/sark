use http::{HeaderName, HeaderValue, StatusCode};

use crate::error::{Error, Result};
use crate::http::codec::Header;
use crate::http::codec::decode::HeaderScan;
use crate::http::codec::decode::chunked::BodyDecoder;
use crate::http::{Body, Response};

const MAX_HEADERS: usize = 100;

fn is_forbidden_trailer(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "content-length"
            | "transfer-encoding"
            | "host"
            | "trailer"
            | "connection"
            | "keep-alive"
            | "te"
            | "upgrade"
            | "proxy-authenticate"
            | "proxy-authorization"
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeMode {
    Response,
    Head,
}

impl DecodeMode {
    const fn is_head(self) -> bool {
        matches!(self, Self::Head)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyKind {
    NoBody,
    ContentLength(usize),
    Chunked,
    UntilEof,
}

pub struct DecodedHead {
    pub status: StatusCode,
    pub headers: Vec<(HeaderName, HeaderValue)>,
    pub header_len: usize,
    pub body_kind: BodyKind,
}

impl DecodedHead {
    pub fn into_response<B, I>(self, body: B, trailers: I) -> Response
    where
        B: Into<Body<'static>>,
        I: IntoIterator<Item = (HeaderName, HeaderValue)>,
    {
        let mut response = Response::new(self.status);
        for (name, value) in self.headers {
            response.headers_mut().insert(name, value);
        }
        for (name, value) in trailers {
            if is_forbidden_trailer(&name) {
                continue;
            }
            response.headers_mut().insert(name, value);
        }
        response.set_body(body);
        response
    }
}

pub struct ResponseDecoder {
    mode: DecodeMode,
}

impl ResponseDecoder {
    pub const fn new(mode: DecodeMode) -> Self {
        Self { mode }
    }

    fn status_has_no_body(status: StatusCode) -> bool {
        let code = status.as_u16();
        code < 200 || code == 204 || code == 304
    }

    fn parse(&self, buf: &[u8]) -> Result<Option<DecodedHead>> {
        let mut raw = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut parsed = httparse::Response::new(&mut raw);

        match parsed.parse(buf)? {
            httparse::Status::Partial => Ok(None),
            httparse::Status::Complete(header_len) => {
                let status_code = parsed
                    .code
                    .ok_or_else(|| Error::BadRequest("Missing status code".into()))?;
                let status = StatusCode::from_u16(status_code)
                    .map_err(|_| Error::BadRequest("Invalid status code".into()))?;

                let mut headers = Vec::with_capacity(parsed.headers.len());
                let mut content_length = None;
                let mut is_chunked = false;
                let mut has_transfer_encoding = false;

                for h in parsed.headers.iter() {
                    let name = HeaderName::from_bytes(h.name.as_bytes()).map_err(|_| {
                        Error::BadRequest(format!("Invalid header name: {}", h.name).into())
                    })?;
                    let value = HeaderValue::from_bytes(h.value).map_err(|_| {
                        Error::BadRequest(format!("Invalid header value for: {}", h.name).into())
                    })?;

                    if name == "content-length" {
                        let len = Header::content_length(value.as_bytes())?;
                        if let Some(existing) = content_length
                            && existing != len
                        {
                            return Err(Error::BadRequest("Conflicting Content-Length".into()));
                        }
                        content_length = Some(len);
                    }
                    if name == "transfer-encoding" {
                        let te = HeaderScan::transfer_encoding(value.as_bytes())?;
                        has_transfer_encoding = has_transfer_encoding || te.has_transfer_encoding;
                        if te.is_chunked {
                            is_chunked = true;
                        }
                    }
                    headers.push((name, value));
                }

                if has_transfer_encoding && content_length.is_some() {
                    return Err(Error::BadRequest(
                        "Content-Length with Transfer-Encoding is not allowed".into(),
                    ));
                }

                let body_kind = if self.mode.is_head() || Self::status_has_no_body(status) {
                    BodyKind::NoBody
                } else if is_chunked {
                    BodyKind::Chunked
                } else if let Some(len) = content_length {
                    BodyKind::ContentLength(len)
                } else {
                    BodyKind::UntilEof
                };

                Ok(Some(DecodedHead {
                    status,
                    headers,
                    header_len,
                    body_kind,
                }))
            }
        }
    }

    pub fn head(&self, buf: &[u8]) -> Result<Option<DecodedHead>> {
        self.parse(buf)
    }

    pub fn response(&self, buf: &[u8]) -> Result<Option<Response>> {
        let head = match self.parse(buf)? {
            Some(h) => h,
            None => return Ok(None),
        };
        let body_data = &buf[head.header_len..];

        match head.body_kind {
            BodyKind::NoBody => Ok(Some(head.into_response(&[][..], []))),
            BodyKind::Chunked => {
                match BodyDecoder::decode_all(body_data, BodyDecoder::DEFAULT_MAX_BODY)? {
                    Some(result) => Ok(Some(head.into_response(result.body, result.trailers))),
                    None => Ok(None),
                }
            }
            BodyKind::ContentLength(len) if body_data.len() < len => Ok(None),
            BodyKind::ContentLength(len) => Ok(Some(head.into_response(&body_data[..len], []))),
            BodyKind::UntilEof => Ok(None),
        }
    }

    pub fn response_after_eof(&self, buf: &[u8]) -> Result<Response> {
        let head = self
            .parse(buf)?
            .ok_or_else(|| Error::BadRequest("Incomplete HTTP response".into()))?;
        let body_data = &buf[head.header_len..];

        match head.body_kind {
            BodyKind::Chunked => {
                let result = BodyDecoder::decode_all(body_data, BodyDecoder::DEFAULT_MAX_BODY)?
                    .ok_or_else(|| Error::BadRequest("Incomplete chunked response".into()))?;
                Ok(head.into_response(result.body, result.trailers))
            }
            BodyKind::ContentLength(len) if body_data.len() < len => {
                Err(Error::BadRequest("Incomplete HTTP response body".into()))
            }
            BodyKind::ContentLength(len) => Ok(head.into_response(&body_data[..len], [])),
            BodyKind::NoBody => Ok(head.into_response(&[][..], [])),
            BodyKind::UntilEof => Ok(head.into_response(body_data, [])),
        }
    }
}
