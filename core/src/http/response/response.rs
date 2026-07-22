use http::{HeaderName, HeaderValue, StatusCode};
use o3::buffer::Shared;
use serde::Serialize;

use super::{Body, FixedResponse, HeaderList, MonoResponseInner};

#[derive(Clone)]
pub struct Response {
    pub(super) status: StatusCode,
    pub(super) headers: HeaderList,
    pub(super) wire_headers: Vec<u8>,
    pub(super) body: Body<'static>,
    pub(super) chunked_parts: Option<Vec<Shared>>,
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .field("wire_headers_len", &self.wire_headers.len())
            .field("body_len", &self.body.len())
            .field("chunked", &self.chunked_parts.is_some())
            .finish()
    }
}

impl Response {
    pub fn new(status: StatusCode) -> Self {
        Self {
            status,
            headers: HeaderList::new(),
            wire_headers: Vec::new(),
            body: Body::empty(),
            chunked_parts: None,
        }
    }

    pub fn ok() -> Self {
        Self::new(StatusCode::OK)
    }

    pub fn text(body: &str) -> Self {
        Self::text_with_status(StatusCode::OK, body)
    }

    pub fn text_with_status(status: StatusCode, body: &str) -> Self {
        let mut resp = Self::new(status);
        resp.content_type("text/plain");
        resp.set_body_str(body);
        resp
    }

    pub fn json<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        Self::json_with_status(StatusCode::OK, value)
    }

    pub fn json_with_status<T: Serialize>(
        status: StatusCode,
        value: &T,
    ) -> Result<Self, serde_json::Error> {
        let s = serde_json::to_string(value)?;
        let mut resp = Self::new(status);
        resp.content_type("application/json");
        resp.set_body_str(&s);
        Ok(resp)
    }

    pub fn insert_header(&mut self, name: HeaderName, value: HeaderValue) -> &mut Self {
        let _ = self.headers.insert(name, value);
        self
    }

    pub fn append_wire_header(&mut self, name: &'static str, value: &str) -> &mut Self {
        Self::assert_wire_header_name(name);
        assert!(
            !value.as_bytes().iter().any(|b| *b == b'\r' || *b == b'\n'),
            "wire header value must not contain CR/LF"
        );
        self.append_wire_header_bytes(name, value.as_bytes())
    }

    pub fn append_wire_header_static(
        &mut self,
        name: &'static str,
        value: &'static str,
    ) -> &mut Self {
        Self::assert_wire_header_name(name);
        self.append_wire_header_bytes(name, value.as_bytes())
    }

    fn append_wire_header_bytes(&mut self, name: &str, value: &[u8]) -> &mut Self {
        self.wire_headers.extend_from_slice(name.as_bytes());
        self.wire_headers.extend_from_slice(b": ");
        self.wire_headers.extend_from_slice(value);
        self.wire_headers.extend_from_slice(b"\r\n");
        self
    }

    pub fn content_type(&mut self, value: &'static str) -> &mut Self {
        let _ = self
            .headers
            .insert(http::header::CONTENT_TYPE, HeaderValue::from_static(value));
        self
    }

    pub fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND)
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn set_status(&mut self, status: StatusCode) {
        self.status = status;
    }

    pub fn headers(&self) -> &HeaderList {
        &self.headers
    }

    pub fn headers_mut(&mut self) -> &mut HeaderList {
        &mut self.headers
    }

    pub fn wire_headers(&self) -> &[u8] {
        self.wire_headers.as_ref()
    }

    pub fn has_wire_headers(&self) -> bool {
        !self.wire_headers.is_empty()
    }

    pub fn body(&self) -> &[u8] {
        self.body.as_bytes()
    }

    pub fn body_is_shared(&self) -> bool {
        self.body.is_shared()
    }

    pub fn body_mut(&mut self) -> &mut Vec<u8> {
        self.body.as_owned_mut()
    }

    pub fn into_body(self) -> Vec<u8> {
        self.body.into_owned()
    }

    pub fn into_body_bytes(self) -> Shared {
        self.body.into_bytes()
    }

    pub fn set_body<B>(&mut self, body: B)
    where
        B: Into<Body<'static>>,
    {
        self.body = body.into();
    }

    pub fn set_body_str(&mut self, body: &str) -> &mut Self {
        self.body = Body::Owned(body.as_bytes().to_vec());
        self
    }

    pub fn push_chunk(&mut self, data: impl Into<Shared>) {
        self.chunked_parts
            .get_or_insert_with(Vec::new)
            .push(data.into());
    }

    pub fn chunked_parts(&self) -> Option<&[Shared]> {
        self.chunked_parts.as_deref()
    }

    pub fn is_chunked(&self) -> bool {
        self.chunked_parts.is_some()
    }

    fn assert_wire_header_name(name: &str) {
        assert!(
            sark_protocol::validate_response_header_name(name).is_ok(),
            "invalid or managed wire header name: {name}"
        );
    }
}

impl<const N: usize> From<MonoResponseInner<'static, N>> for Response {
    fn from(response: MonoResponseInner<'static, N>) -> Self {
        Self {
            status: response.status,
            headers: response.headers.map(|h| *h).unwrap_or_default(),
            wire_headers: response.head.into_bytes().as_ref().to_vec(),
            body: Body::from(response.body),
            chunked_parts: None,
        }
    }
}

impl<const N: usize> From<FixedResponse<'static, N>> for Response {
    fn from(response: FixedResponse<'static, N>) -> Self {
        Self {
            status: response.status,
            headers: HeaderList::new(),
            wire_headers: response.wire_headers().as_ref().to_vec(),
            body: Body::Shared(response.body),
            chunked_parts: None,
        }
    }
}
