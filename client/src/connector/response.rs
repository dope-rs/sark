use o3::buffer::Shared;
use sark_core::http::{HeaderList, Response};

/// A body-free HTTP response head.
#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct ResponseHead(Response);

const _: () = assert!(
    std::mem::size_of::<ResponseHead>() == std::mem::size_of::<Response>()
        && std::mem::align_of::<ResponseHead>() == std::mem::align_of::<Response>()
);

impl ResponseHead {
    pub(crate) fn new(response: Response) -> Self {
        debug_assert!(response.body().is_empty());
        Self(response)
    }

    pub fn status(&self) -> http::StatusCode {
        self.0.status()
    }

    pub fn headers(&self) -> &HeaderList {
        self.0.headers()
    }

    pub fn into_response(self) -> Response {
        self.0
    }

    pub(crate) fn as_response(&self) -> &Response {
        &self.0
    }
}

/// One typed event from an HTTP/1 response.
#[derive(Clone, Debug)]
pub enum ResponseEvent {
    /// A non-final 1xx response preceding the final response head.
    Informational(ResponseHead),
    /// The final response head. Its body is always empty.
    Head(ResponseHead),
    /// A transfer-decoded response body segment.
    ///
    /// Content codings such as gzip remain encoded in the streaming API.
    Data(Shared),
    /// Allowed HTTP trailer fields.
    Trailers(HeaderList),
}
