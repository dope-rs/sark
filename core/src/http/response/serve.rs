use http::StatusCode;

use super::{Chunked, FixedResponseInner, HotBodyInner, HotHeadInner, MonoResponseInner, Response};

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum ServeInner<'req> {
    Chunked(Chunked),
    Fixed(FixedResponseInner<'req>),
    Mono(MonoResponseInner<'req>),
}

pub type Serve = ServeInner<'static>;

impl<'req> ServeInner<'req> {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Chunked(response) => response.status(),
            Self::Fixed(response) => response.status(),
            Self::Mono(response) => response.status(),
        }
    }
}

impl ServeInner<'static> {
    pub fn into_response(self) -> Response {
        match self {
            Self::Chunked(response) => response.into_response(),
            Self::Fixed(response) => Response::from(response),
            Self::Mono(response) => Response::from(response),
        }
    }
}

impl From<Response> for ServeInner<'static> {
    fn from(value: Response) -> Self {
        if let Some(parts) = value.chunked_parts {
            return Self::Chunked(Chunked::from_parts(
                value.status,
                value.headers,
                value.wire_headers.freeze(),
                parts,
            ));
        }
        let headers = if value.headers.is_empty() {
            None
        } else {
            Some(Box::new(value.headers))
        };
        Self::Mono(MonoResponseInner {
            status: value.status,
            headers,
            head: HotHeadInner::Wire(value.wire_headers.freeze()),
            body: HotBodyInner::from(value.body),
        })
    }
}

impl<'req> From<MonoResponseInner<'req>> for ServeInner<'req> {
    fn from(value: MonoResponseInner<'req>) -> Self {
        Self::Mono(value)
    }
}

impl<'req> From<FixedResponseInner<'req>> for ServeInner<'req> {
    fn from(value: FixedResponseInner<'req>) -> Self {
        Self::Fixed(value)
    }
}

pub trait IntoServeResponse<'req> {
    fn into_serve_response(self) -> ServeInner<'req>;
}

pub trait IntoServeResponseStatic<'req> {
    fn into_serve_response_static(self) -> ServeInner<'req>;
}

impl<'req> IntoServeResponse<'req> for ServeInner<'req> {
    fn into_serve_response(self) -> Self {
        self
    }
}

impl IntoServeResponse<'static> for Response {
    fn into_serve_response(self) -> ServeInner<'static> {
        ServeInner::from(self)
    }
}

impl<'req> IntoServeResponse<'req> for MonoResponseInner<'req> {
    fn into_serve_response(self) -> ServeInner<'req> {
        ServeInner::Mono(self)
    }
}

impl<'req> IntoServeResponse<'req> for FixedResponseInner<'req> {
    fn into_serve_response(self) -> ServeInner<'req> {
        ServeInner::Fixed(self)
    }
}
