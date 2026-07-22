use http::StatusCode;

use super::{
    Chunked, DEFAULT_HEADER_CAPACITY, FixedResponse, HotBodyInner, HotHeadInner, MonoResponseInner,
    Response,
};

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum Serve<'req, const N: usize = DEFAULT_HEADER_CAPACITY> {
    Chunked(Chunked),
    Fixed(FixedResponse<'req, N>),
    Mono(MonoResponseInner<'req, N>),
}

impl<'req, const N: usize> Serve<'req, N> {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Chunked(response) => response.status(),
            Self::Fixed(response) => response.status(),
            Self::Mono(response) => response.status(),
        }
    }
}

impl Serve<'static> {
    pub fn into_response(self) -> Response {
        match self {
            Self::Chunked(response) => response.into_response(),
            Self::Fixed(response) => Response::from(response),
            Self::Mono(response) => Response::from(response),
        }
    }
}

impl From<Response> for Serve<'static> {
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

impl<'req, const N: usize> From<MonoResponseInner<'req, N>> for Serve<'req, N> {
    fn from(value: MonoResponseInner<'req, N>) -> Self {
        Self::Mono(value)
    }
}

impl<'req, const N: usize> From<FixedResponse<'req, N>> for Serve<'req, N> {
    fn from(value: FixedResponse<'req, N>) -> Self {
        Self::Fixed(value)
    }
}
