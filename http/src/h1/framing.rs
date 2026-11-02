//! HTTP/1 message framing and final admission plans.

use crate::{
    field::text::Ascii,
    h1::{
        field::{self, expect},
        switch, target,
    },
    payload::response::Method,
    request::{self, span},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    ContentLengthWithTransferEncoding,
    UnframedRequestTransferEncoding,
    Http10TransferEncoding,
    ForbiddenResponseFraming,
    SuccessfulConnectIsTransition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Response {
    Framing(Error),
    Transition(switch::Error),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RequestError {
    Authority(target::Error),
    Framing(Error),
    Expectation(expect::Error),
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Transfer {
    None,
    Chunked,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Message {
    content_length: Option<usize>,
    transfer: Transfer,
}

impl Message {
    pub const fn new(length: Option<usize>, transfer: Transfer) -> Self {
        Self {
            content_length: length,
            transfer,
        }
    }
}

/// Borrow-free state for fields that determine message framing.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Fields {
    content_length: Option<usize>,
    transfer: field::Transfer,
}

impl Fields {
    pub fn try_length(mut self, value: &[u8]) -> Result<Self, field::Error> {
        if self.content_length.is_some() {
            return Err(field::Error::DuplicateContentLength);
        }
        if self.transfer.has_encoding() {
            return Err(field::Error::ContentLengthWithTransferEncoding);
        }
        let length = Ascii::new(crate::field::Error::trim_ows(value))
            .parse_usize()
            .map_err(field::Error::InvalidContentLength)?;
        self.content_length = Some(length);
        Ok(self)
    }

    /// Commits a Content-Length already parsed by a scanner's specialized
    /// integer fast path.
    pub fn try_set(mut self, length: usize) -> Result<Self, field::Error> {
        if self.content_length.is_some() {
            return Err(field::Error::DuplicateContentLength);
        }
        if self.transfer.has_encoding() {
            return Err(field::Error::ContentLengthWithTransferEncoding);
        }
        self.content_length = Some(length);
        Ok(self)
    }

    pub fn try_coding(mut self, value: &[u8]) -> Result<Self, field::Error> {
        if self.content_length.is_some() {
            return Err(field::Error::ContentLengthWithTransferEncoding);
        }
        let transfer = self
            .transfer
            .parse(value)
            .map_err(field::Error::InvalidTransferEncoding)?;
        self.transfer = transfer;
        Ok(self)
    }

    /// Commits one field value already validated by a scanner's specialized
    /// Transfer-Encoding path.
    pub fn try_append(self, value: field::Transfer) -> Result<Self, field::Error> {
        if self.content_length.is_some() {
            return Err(field::Error::ContentLengthWithTransferEncoding);
        }
        let transfer = self
            .transfer
            .try_join(value)
            .map_err(field::Error::InvalidTransferEncoding)?;
        Ok(Self {
            content_length: self.content_length,
            transfer,
        })
    }

    pub const fn length(self) -> Option<usize> {
        self.content_length
    }

    pub const fn has_coding(self) -> bool {
        self.transfer.has_encoding()
    }

    pub const fn is_chunked(self) -> bool {
        self.transfer.is_chunked()
    }

    pub(in crate::h1) const fn has_other(self) -> bool {
        self.transfer.has_other()
    }

    pub const fn finish(self) -> Message {
        Message::new(self.content_length, self.transfer.framing())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestBody {
    Empty,
    Fixed(usize),
    Chunked,
}

/// The protocol-semantic result required before routing may commit an HTTP/1
/// response. It owns only compact metadata; all byte views remain derived from
/// the retained request owner through [`target::Target`] spans.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Admission {
    target: target::Target,
    authority: target::Plan,
    body: RequestBody,
    expectation: expect::Decision,
}

impl Admission {
    pub const fn target(self) -> target::Target {
        self.target
    }

    pub const fn authority(self) -> target::Plan {
        self.authority
    }

    pub const fn body(self) -> RequestBody {
        self.body
    }

    pub const fn expectation(self) -> expect::Decision {
        self.expectation
    }

    pub fn layout(
        self,
        target: span::Span,
        host: Option<span::Span>,
    ) -> Result<request::Layout, target::Error> {
        let span = self.authority.span(self.target, host)?;
        self.target.layout(target, span)
    }

    pub fn admit(
        fields: Fields,
        auth: target::Request,
        expect: expect::State,
        target: target::Target,
        version: &[u8],
        policy: target::Policy,
    ) -> Result<Self, RequestError> {
        let plan = auth
            .finish_plan(target, version)
            .and_then(|plan| match policy {
                target::Policy::AllowEmpty => Ok(plan),
                target::Policy::Require => plan.require(),
            })
            .map_err(RequestError::Authority)?;
        let body = fields
            .finish()
            .try_version(version)
            .and_then(Message::request)
            .map_err(RequestError::Framing)?;
        let decision = if version == b"HTTP/1.0" {
            expect::Decision::None
        } else {
            expect.finish(body).map_err(RequestError::Expectation)?
        };
        Ok(Self {
            target,
            authority: plan,
            body,
            expectation: decision,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ResponseBody {
    None,
    Fixed(usize),
    Chunked,
    UntilEof,
    Empty(Empty),
}

/// Borrow-free transport decision produced at the same boundary as response
/// framing. Consumers never need to traverse materialized headers again.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct Control {
    keep_alive_timeout_secs: u64,
    connection: field::Connection,
    reuse: Reuse,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Reuse {
    Close = 1,
    Reusable = 2,
    Timed = 3,
}

impl Control {
    pub const fn new(
        version: u8,
        status: u16,
        body: ResponseBody,
        conn: field::Connection,
        peer: field::KeepAlive,
    ) -> Self {
        let persist = version == 1 || version == 0 && conn.is_keep();
        let (secs, reuse) = if status == 101
            || !persist
            || conn.is_close()
            || conn.is_upgrade()
            || matches!(
                body,
                ResponseBody::UntilEof | ResponseBody::Empty(Empty::UntilEof)
            ) {
            (0, Reuse::Close)
        } else {
            match peer.timeout_secs() {
                Some(timeout) => (timeout, Reuse::Timed),
                None => (0, Reuse::Reusable),
            }
        };
        Self {
            keep_alive_timeout_secs: secs,
            connection: conn,
            reuse,
        }
    }

    pub const fn connection(self) -> field::Connection {
        self.connection
    }

    pub const fn is_reusable(self) -> bool {
        matches!(self.reuse, Reuse::Reusable | Reuse::Timed)
    }

    pub const fn timeout(self) -> Option<u64> {
        match self.reuse {
            Reuse::Timed => Some(self.keep_alive_timeout_secs),
            Reuse::Close | Reuse::Reusable => None,
        }
    }
}

/// Complete semantic result of admitting one HTTP/1 response head.
/// A protocol switch is not an HTTP message body and therefore cannot be
/// accidentally consumed by a normal body decoder.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Informational,
    Message(ResponseBody),
    Switch(switch::Kind),
}

/// Wire framing that still has to be consumed for a response whose status
/// semantics require the decoded content to remain empty (currently 205).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Empty {
    Fixed,
    Chunked,
    UntilEof,
}

impl Message {
    pub(in crate::h1) fn try_version(self, version: &[u8]) -> Result<Self, Error> {
        if version == b"HTTP/1.0" && !matches!(self.transfer, Transfer::None) {
            return Err(Error::Http10TransferEncoding);
        }
        Ok(self)
    }

    const fn is_unframed(self) -> bool {
        self.content_length.is_none() && matches!(self.transfer, Transfer::None)
    }

    pub const fn request(self) -> Result<RequestBody, Error> {
        if !matches!(self.transfer, Transfer::None) {
            if self.content_length.is_some() {
                return Err(Error::ContentLengthWithTransferEncoding);
            }
            return match self.transfer {
                Transfer::Chunked => Ok(RequestBody::Chunked),
                Transfer::Other => Err(Error::UnframedRequestTransferEncoding),
                Transfer::None => Ok(RequestBody::Empty),
            };
        }
        match self.content_length {
            None | Some(0) => Ok(RequestBody::Empty),
            Some(length) => Ok(RequestBody::Fixed(length)),
        }
    }

    pub const fn response(self, request: Method, status: u16) -> Result<ResponseBody, Error> {
        if self.content_length.is_some() && !matches!(self.transfer, Transfer::None) {
            return Err(Error::ContentLengthWithTransferEncoding);
        }
        use crate::payload::response::Content;
        match Content::new(request, status) {
            Content::Forbidden => {
                if !self.is_unframed() {
                    return Err(Error::ForbiddenResponseFraming);
                }
                return Ok(ResponseBody::None);
            }
            Content::Suppressed => return Ok(ResponseBody::None),
            Content::Tunnel => return Err(Error::SuccessfulConnectIsTransition),
            Content::Empty => {
                if !matches!(self.content_length, None | Some(0)) {
                    return Err(Error::ForbiddenResponseFraming);
                }
                let empty = match self.transfer {
                    Transfer::None if matches!(self.content_length, Some(0)) => Empty::Fixed,
                    Transfer::None => Empty::UntilEof,
                    Transfer::Chunked => Empty::Chunked,
                    Transfer::Other => Empty::UntilEof,
                };
                return Ok(ResponseBody::Empty(empty));
            }
            Content::Allowed => {}
        }
        if !matches!(self.transfer, Transfer::None) {
            return Ok(match self.transfer {
                Transfer::Chunked => ResponseBody::Chunked,
                Transfer::Other => ResponseBody::UntilEof,
                Transfer::None => ResponseBody::UntilEof,
            });
        }
        match self.content_length {
            Some(length) => Ok(ResponseBody::Fixed(length)),
            None => Ok(ResponseBody::UntilEof),
        }
    }
}

/// Single parser-neutral response commit point for body framing and protocol
/// transitions. Callers must carry the returned plan through their transport
/// submission barrier before entering raw duplex mode.
impl Outcome {
    pub const fn admit(
        request: Method,
        intent: switch::Intent,
        status: u16,
        framing: Message,
        conn: field::Connection,
        upgrade: Option<switch::Selection>,
    ) -> Result<Self, Response> {
        if status == 101 {
            return match intent {
                switch::Intent::Upgrade => {
                    if !conn.is_upgrade() {
                        Err(Response::Transition(
                            switch::Error::MissingConnectionUpgrade,
                        ))
                    } else if upgrade.is_none() {
                        Err(Response::Transition(switch::Error::MissingSelection))
                    } else if !framing.is_unframed() {
                        Err(Response::Framing(Error::ForbiddenResponseFraming))
                    } else {
                        Ok(Self::Switch(switch::Kind::Upgrade))
                    }
                }
                switch::Intent::None | switch::Intent::Connect => {
                    Err(Response::Transition(switch::Error::UnexpectedSwitch))
                }
            };
        }

        if status < 200 {
            if !framing.is_unframed() {
                return Err(Response::Framing(Error::ForbiddenResponseFraming));
            }
            return Ok(Self::Informational);
        }

        if matches!(intent, switch::Intent::Connect) && status < 300 {
            if !framing.is_unframed() {
                return Err(Response::Framing(Error::ForbiddenResponseFraming));
            }
            return Ok(Self::Switch(switch::Kind::Connect));
        }

        match framing.response(request, status) {
            Ok(body) => Ok(Self::Message(body)),
            Err(error) => Err(Response::Framing(error)),
        }
    }
}

const _: () = assert!(core::mem::size_of::<Fields>() <= 3 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Admission>() <= 7 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Control>() == 16);
const _: () = assert!(core::mem::size_of::<Option<Control>>() == 16);
