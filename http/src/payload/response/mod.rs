//! Response state, encoding, and ownership handoff.

use o3::buffer::write::{Sink, consume};

use crate::payload::response::status::Code;

mod body;
pub mod bounded;
pub mod dispatch;
pub mod encoded;
pub mod exact;
pub mod headers;
pub mod output;
pub mod status;

pub use body::Body;

pub type Fixed<H, B> = output::Output<H, B, output::Fixed>;
pub type Static<'body, H> = output::Output<H, crate::payload::fixed::Body<'body>, output::Static>;
pub type Exact<H, B> = output::Output<H, B, output::Encoded<true>>;
pub type Bounded<H, B> = output::Output<H, B, output::Encoded<false>>;

/// Request methods whose semantics affect response content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Method {
    Other,
    Head,
    Connect,
}

impl Method {
    pub fn from_bytes(method: &[u8]) -> Self {
        match method.len() {
            4 if method == b"HEAD" => Self::Head,
            7 if method == b"CONNECT" => Self::Connect,
            0..=3 | 5..=6 | 8.. => Self::Other,
            4 | 7 => Self::Other,
        }
    }
}

/// Protocol-independent content semantics of a response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Content {
    /// Content and body-length framing fields are forbidden.
    Forbidden,
    /// Content is omitted, but representation length metadata is permitted.
    Suppressed,
    /// A successful CONNECT establishes a tunnel rather than message content.
    Tunnel,
    /// Content must be empty, while empty-message framing is permitted.
    Empty,
    Allowed,
}

impl Content {
    pub const fn new(method: Method, status: u16) -> Self {
        if matches!(method, Method::Connect) && status >= 200 && status < 300 {
            Self::Tunnel
        } else if status < 200 || status == 204 {
            Self::Forbidden
        } else if matches!(method, Method::Head) || status == 304 {
            Self::Suppressed
        } else if status == 205 {
            Self::Empty
        } else {
            Self::Allowed
        }
    }
}

/// Consumes one body into the admitted output without repeating its size check.
pub trait Encode: Sized {
    fn encode<W>(self, out: &mut W) -> Result<(), consume::Error>
    where
        W: Sink<Error = consume::Error>;
}

use std::borrow::Cow;

use headers::{List, Name};

pub struct Response<'field, B = [u8; 0], T = (), S = Code, N = Name<Cow<'field, str>>> {
    pub(crate) status: S,
    pub(crate) headers: List<'field, N>,
    pub(crate) trailers: T,
    pub(crate) body: B,
}

impl<'field, B, T, S, N> Response<'field, B, T, S, N> {
    pub fn status(&self) -> S
    where
        S: Copy,
    {
        self.status
    }

    pub fn headers(&self) -> &List<'field, N> {
        &self.headers
    }

    pub fn body(&self) -> &B {
        &self.body
    }

    pub fn body_mut(&mut self) -> &mut B {
        &mut self.body
    }

    pub fn into_body(self) -> B {
        self.body
    }

    pub fn with_body<U>(self, body: U) -> Response<'field, U, T, S, N> {
        Response {
            status: self.status,
            headers: self.headers,
            trailers: self.trailers,
            body,
        }
    }
}

impl<'field, B, S, N> Response<'field, B, List<'field>, S, N> {
    pub fn trailers(&self) -> &List<'field> {
        &self.trailers
    }
}

impl<'field> Response<'field, [u8; 0]> {
    pub fn new(status: Code) -> Self {
        Self {
            status,
            headers: List::new(),
            trailers: (),
            body: [],
        }
    }
}

impl<'field, 'req, 'd, B> dispatch::Complete<'req, 'd> for Response<'field, B, (), Code>
where
    B: Body<'d>,
{
    type Bound = o3::buffer::resident::bound::Unbounded;

    const BODY: crate::payload::Kind = crate::payload::Kind::Inline;

    fn complete<S: dispatch::Accept<'d>>(self, sink: S) -> Result<S::Output, S::Error> {
        let Self {
            status,
            headers,
            trailers: (),
            body,
        } = self;
        sink.fixed(status, headers, body)
    }
}

const _: () = assert!(
    core::mem::size_of::<Response<'static, [u8; 0]>>()
        < core::mem::size_of::<Response<'static, [u8; 0], List<'static>, status::Code>>()
);
