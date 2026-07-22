use http::StatusCode;

use super::{
    Chunked, DEFAULT_HEADER_CAPACITY, FixedResponseInner, HotBodyInner, HotHeadInner,
    MonoResponseInner, Response,
};

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum ServeInner<'req, const N: usize = DEFAULT_HEADER_CAPACITY> {
    Chunked(Chunked),
    Fixed(FixedResponseInner<'req, N>),
    Mono(MonoResponseInner<'req, N>),
}

pub type Serve = ServeInner<'static>;

impl<'req, const N: usize> ServeInner<'req, N> {
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
                o3::buffer::Shared::from(value.wire_headers),
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
            head: HotHeadInner::Wire(o3::buffer::Shared::from(value.wire_headers)),
            body: HotBodyInner::from(value.body),
        })
    }
}

impl<'req, const N: usize> From<MonoResponseInner<'req, N>> for ServeInner<'req, N> {
    fn from(value: MonoResponseInner<'req, N>) -> Self {
        Self::Mono(value)
    }
}

impl<'req, const N: usize> From<FixedResponseInner<'req, N>> for ServeInner<'req, N> {
    fn from(value: FixedResponseInner<'req, N>) -> Self {
        Self::Fixed(value)
    }
}

pub trait IntoServeResponse<'req, const N: usize = 4> {
    fn into_serve_response(self) -> ServeInner<'req, N>;
}

impl<'req, const N: usize> IntoServeResponse<'req, N> for ServeInner<'req, N> {
    fn into_serve_response(self) -> Self {
        self
    }
}

impl IntoServeResponse<'static> for Response {
    fn into_serve_response(self) -> ServeInner<'static> {
        ServeInner::from(self)
    }
}

impl<'req, const N: usize> IntoServeResponse<'req, N> for MonoResponseInner<'req, N> {
    fn into_serve_response(self) -> ServeInner<'req, N> {
        ServeInner::Mono(self)
    }
}

impl<'req, const N: usize> IntoServeResponse<'req, N> for FixedResponseInner<'req, N> {
    fn into_serve_response(self) -> ServeInner<'req, N> {
        ServeInner::Fixed(self)
    }
}
