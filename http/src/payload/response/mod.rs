//! Response state, encoding, and ownership handoff.

use o3::buffer::{
    storage,
    write::{Sink, Slice, Split, consume},
};

use crate::payload::response::status::Code;

mod body;
pub mod bounded;
pub mod dispatch;
pub mod exact;
pub mod fixed;
pub mod headers;
pub mod statics;
pub mod status;

pub use body::Body;

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

fn try_encode<'writer, 'target, L: std::borrow::BorrowMut<usize>>(
    out: &'writer mut Slice<'target, L>,
    limit: usize,
    body: impl Encode,
) -> Result<Split<'writer, 'target, L>, storage::BuildError<consume::Error>> {
    let mut split = out
        .try_split(0, limit)
        .map_err(storage::BuildError::Build)?;
    body.encode(&mut split.body())
        .map_err(storage::BuildError::Build)?;
    Ok(split)
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

impl<'field, 'req, 'd, B> dispatch::IntoOutput<'req, 'd> for Response<'field, B, (), Code>
where
    B: Body<'d>,
{
    type Output = fixed::Direct<List<'field, Name<Cow<'field, str>>>, B>;

    fn into_output(self) -> Self::Output {
        let Self {
            status,
            headers,
            trailers: (),
            body,
        } = self;
        fixed::Direct::new(status, headers, body)
    }
}

const _: () = assert!(
    core::mem::size_of::<Response<'static, [u8; 0]>>()
        < core::mem::size_of::<Response<'static, [u8; 0], List<'static>, status::Code>>()
);
