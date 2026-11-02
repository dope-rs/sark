use std::{mem, num::NonZero};

use header::Policy as _;
use o3::buffer::storage;
use sark_core::route::{self, field::header};
use sark_http::{
    field::head::Flags,
    h1::{
        field::{expect, syntax},
        framing::RequestBody,
        head::{
            Semantic,
            line::{Request, Version},
            request::{self, Fields},
        },
        target,
    },
    request::Layout,
};

use crate::wire::{parsing, recv, response};

pub mod line;
mod pending;
pub(crate) use pending::{Partial, Pending, Progress};

pub enum Error<R: route::Schema> {
    Storage(storage::AllocError),
    Syntax(syntax::Error),
    Line(line::Error<R>),
    Fields {
        actual: usize,
        limit: usize,
    },
    Length {
        len: usize,
    },
    Target(Semantic),
    Method(Semantic),
    Layout(target::Error),
    UnsupportedTransferEncoding,
    Offset {
        base: usize,
        relative: usize,
        suffix: usize,
        available: usize,
    },
}

/// One scanned HTTP/1 field block before request-target admission.
#[doc(hidden)]
pub struct Block<H> {
    headers: H,
    fields: Fields,
    flags: Flags,
    len: NonZero<u32>,
}

pub(crate) enum Scanned<'input, H> {
    Complete(Block<H>),
    Pending(Partial<'input, H>),
}

/// Persistence selected by one admitted HTTP/1 request head.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Connection {
    KeepAlive,
    Close,
}

impl Connection {
    pub(crate) const fn join(self, other: Self) -> Self {
        match (self, other) {
            (Self::KeepAlive, Self::KeepAlive) => Self::KeepAlive,
            (Self::KeepAlive | Self::Close, Self::Close) | (Self::Close, Self::KeepAlive) => {
                Self::Close
            }
        }
    }
}

/// An admitted HTTP/1 request head with route-specific raw fields.
#[doc(hidden)]
pub struct Parsed<H> {
    headers: H,
    len: NonZero<u32>,
    body: RequestBody,
    expectation: expect::Decision,
    layout: Layout,
    connection: Connection,
    response: response::Context,
}

/// An admitted request head paired with its permitted response coding.
#[doc(hidden)]
pub struct Gzip<H> {
    head: Parsed<H>,
    coding: request::Coding,
}

impl<H> Parsed<H> {
    pub const fn response(&self) -> response::Context {
        self.response
    }

    pub(crate) fn into_headers(self) -> H {
        self.headers
    }

    pub const fn len(&self) -> NonZero<u32> {
        self.len
    }

    pub const fn body(&self) -> RequestBody {
        self.body
    }

    pub const fn expectation(&self) -> expect::Decision {
        self.expectation
    }

    pub const fn layout(&self) -> Layout {
        self.layout
    }

    pub(crate) const fn connection(&self) -> Connection {
        self.connection
    }
}

impl<H> Block<H> {
    pub fn into_headers(self) -> H {
        self.headers
    }

    pub const fn coding(&self) -> request::Coding {
        self.fields.message.coding()
    }

    fn advance<R: route::Schema>(
        input: &[u8],
        base: usize,
        relative: usize,
    ) -> Result<usize, Error<R>> {
        const SUFFIX: usize = 2;

        let next = relative
            .checked_add(SUFFIX)
            .and_then(|offset| base.checked_add(offset))
            .filter(|next| *next <= input.len())
            .ok_or(Error::Offset {
                base,
                relative,
                suffix: SUFFIX,
                available: input.len(),
            })?;
        Ok(next)
    }

    /// Scans one route-specific HTTP/1 field block.
    #[doc(hidden)]
    pub fn parse<R>(
        input: &[u8],
        start: usize,
        limit: usize,
        coding: request::Coding,
        version: Version,
    ) -> Result<Option<Self>, Error<R>>
    where
        H: Default,
        R: route::Schema<RawHeaders = H>,
    {
        Ok(
            match Self::scan::<R>(input, start, limit, coding, version)? {
                Scanned::Complete(block) => Some(block),
                Scanned::Pending(_) => None,
            },
        )
    }

    pub(crate) fn scan<'input, R>(
        input: &'input [u8],
        start: usize,
        limit: usize,
        coding: request::Coding,
        version: Version,
    ) -> Result<Scanned<'input, H>, Error<R>>
    where
        H: Default,
        R: route::Schema<RawHeaders = H>,
    {
        const {
            assert!(mem::size_of::<Result<(), Error<R>>>() == mem::size_of::<Error<R>>());
            assert!(mem::size_of::<line::Step<R>>() == mem::size_of::<line::Error<R>>());
        }
        let mut headers = H::default();
        let mut fields = Fields::default();
        let mut flags = Flags::default();
        let mut count = 0usize;
        let mut pos = start;
        let len = loop {
            let Some(rest) = input.get(pos..) else {
                break None;
            };
            let prefix = line::Scan::<R> {
                headers: &mut headers,
                input,
                rest,
                start: pos,
                fields: &mut fields,
                flags: &mut flags,
                coding,
                version,
            };
            match R::Header::prefix(prefix) {
                line::Step::Complete(0) => break Some(Self::advance::<R>(input, pos, 0)?),
                line::Step::Complete(relative) => {
                    if count >= limit {
                        return Err(Error::Fields {
                            actual: count.saturating_add(1),
                            limit,
                        });
                    }
                    count += 1;
                    pos = Self::advance::<R>(input, pos, relative)?;
                }
                line::Step::NeedMore => break None,
                line::Step::Bad(cause) => return Err(Error::Line(cause)),
            }
        };
        let Some(len) = len else {
            return Ok(Scanned::Pending(Partial {
                input,
                headers,
                fields,
                flags,
                count,
                pos,
            }));
        };
        const { assert!(recv::HEAD_CAPACITY <= u32::MAX as usize) };
        if len > recv::HEAD_CAPACITY {
            return Err(Error::Length { len });
        }
        let len = NonZero::new(len as u32).ok_or(Error::Length { len })?;
        Ok(Scanned::Complete(Self {
            headers,
            fields,
            flags,
            len,
        }))
    }
}

impl<H> Parsed<H> {
    /// Parses and admits one route-specific HTTP/1 request head.
    #[doc(hidden)]
    pub fn parse<R>(source: &parsing::Head<'_>, limit: usize) -> Result<Option<Self>, Error<R>>
    where
        H: Default,
        R: route::Schema<RawHeaders = H>,
    {
        let input = source.input().as_slice();
        let request = source.request();
        let Some(block) = Block::<H>::parse::<R>(
            input,
            request.end(),
            limit,
            request::Coding::Plain,
            request.version(),
        )?
        else {
            return Ok(None);
        };
        Self::from_block::<R>(block, request).map(Some)
    }

    pub(crate) fn from_block<R>(block: Block<H>, request: &Request<'_>) -> Result<Self, Error<R>>
    where
        R: route::Schema<RawHeaders = H>,
    {
        let Block {
            headers,
            fields,
            flags,
            len,
        } = block;
        let admit = fields
            .admit_target(
                request.plan(),
                request.version().as_bytes(),
                target::Policy::Require,
            )
            .map_err(Error::Target)?;
        let response = response::Context::from_request(request);
        fields
            .transition(response.method(), request.version())
            .map_err(Error::Method)?;
        let layout = admit
            .layout(request.target_span(), fields.routing.host_span())
            .map_err(Error::Layout)?;
        if fields.message.has_other_transfer_coding() {
            return Err(Error::UnsupportedTransferEncoding);
        }
        let conn = if flags.is_close(request.version().as_bytes()) {
            Connection::Close
        } else {
            Connection::KeepAlive
        };
        Ok(Self {
            headers,
            len,
            body: admit.body(),
            expectation: admit.expectation(),
            layout,
            connection: conn,
            response: response.with_connection(conn),
        })
    }
}

impl<H> Gzip<H> {
    #[doc(hidden)]
    pub fn parse<R>(
        source: &parsing::Head<'_>,
        limit: usize,
        coding: request::Coding,
    ) -> Result<Option<Self>, Error<R>>
    where
        H: Default,
        R: route::Schema<RawHeaders = H>,
    {
        let input = source.input().as_slice();
        let request = source.request();
        let Some(block) =
            Block::<H>::parse::<R>(input, request.end(), limit, coding, request.version())?
        else {
            return Ok(None);
        };
        let coding = block.fields.message.coding();
        Parsed::from_block::<R>(block, request).map(|head| Some(Self { head, coding }))
    }

    pub(crate) fn into_parts(self) -> (Parsed<H>, request::Coding) {
        (self.head, self.coding)
    }
}

const _: () = assert!(core::mem::size_of::<Parsed<()>>() <= 7 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Gzip<()>>() <= 7 * core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Connection>() == core::mem::size_of::<bool>());
