//! Request routing context and HTTP/1 body framing.
use core::convert::Infallible;
use std::num::NonZero;

use o3::buffer::{bytes::window::Snapshot, storage};
use sark_core::{request, route, route::field::query};
use sark_http::{
    h1::{
        codec::canned,
        field::syntax,
        head::{
            Boundary, Limit, ParseError,
            line::{self, error},
            request::{scan, scan::line::Progress},
        },
    },
    payload::fixed,
    request::span,
};
use storage::range;

use crate::{
    Input, session,
    wire::{
        Failure, InputError,
        body::{chunked, ingress},
        head, recv, response,
    },
};

pub mod framing;
pub mod raw;

pub enum Request<R: route::Schema> {
    Wire(Failure),
    Head(head::Error<R>),
    Query(R::Error),
}

impl<R: route::Schema> Request<R> {
    pub(in crate::wire::parsing) const fn bad() -> Self {
        Self::Wire(Failure::BadRequest)
    }

    pub(in crate::wire::parsing) const fn too_large() -> Self {
        Self::Wire(Failure::PayloadTooLarge)
    }

    pub(in crate::wire::parsing) fn ingress(error: ingress::Error) -> Self {
        Self::Wire(Failure::Ingress(error))
    }

    pub(in crate::wire::parsing) fn chunked(error: chunked::Decode) -> Self {
        Self::Wire(Failure::Chunk(error))
    }

    pub(in crate::wire::parsing) fn input(error: InputError) -> Self {
        Self::Wire(Failure::Input(error))
    }

    pub(in crate::wire::parsing) fn range(error: range::Error) -> Self {
        Self::input(InputError::Range(error))
    }

    pub(in crate::wire::parsing) fn bind(error: request::BindError) -> Self {
        Self::Wire(Failure::Bind(error))
    }

    pub fn into_consume<'d, D>(self) -> crate::session::Outcome<'d, D> {
        match self {
            Self::Wire(error) => crate::session::Outcome::Failed(error),
            Self::Head(head::Error::Storage(error)) => {
                crate::session::Outcome::Failed(Failure::Input(InputError::Storage(error)))
            }
            Self::Head(head::Error::UnsupportedTransferEncoding) => {
                crate::session::Outcome::Close(canned::NOT_IMPLEMENTED)
            }
            Self::Head(_) | Self::Query(_) => crate::session::Outcome::Close(canned::BAD_REQUEST),
        }
    }
}

/// One HTTP/1 request line bound to the exact input window that produced it.
///
#[derive(Clone, Copy)]
pub struct Head<'a> {
    input: crate::Input<'a>,
    request: line::Request<'a>,
}

impl<'a> Head<'a> {
    pub fn response(&self) -> response::Context {
        response::Context::from_request(&self.request)
    }

    pub fn parse(input: crate::Input<'a>, methods: u8) -> Result<Option<Self>, error::Request> {
        line::Request::parse_with(input.as_slice(), methods)
            .map(|request| request.map(|request| Self { input, request }))
    }

    pub const fn request(&self) -> &line::Request<'a> {
        &self.request
    }

    pub const fn input(self) -> crate::Input<'a> {
        self.input
    }

    fn try_head<R>(
        &self,
        raw: &mut <R::Request as route::Schema>::RawHeaders,
        len: NonZero<u32>,
    ) -> Result<&'a [u8], Request<R::Request>>
    where
        R: route::Spec,
    {
        let head = self
            .input
            .try_get(0..len.get() as usize)
            .map_err(Request::range)?;
        self.parse_query::<R>(raw)?;
        Ok(head.as_slice())
    }

    fn try_frame<R>(
        &self,
        head: NonZero<u32>,
        total: usize,
    ) -> Result<(&'a [u8], Frame<'a>), Request<R::Request>>
    where
        R: route::Spec,
    {
        let wire = self
            .input
            .try_get(0..total)
            .map_err(Request::range)?
            .as_slice();
        let Some((head, body)) = wire.split_at_checked(head.get() as usize) else {
            return Err(Request::bad());
        };
        Ok((head, Frame { body, wire }))
    }

    fn parse_query<R: route::Spec>(
        &self,
        raw: &mut <R::Request as route::Schema>::RawHeaders,
    ) -> Result<(), Request<R::Request>> {
        if let Some(query) = self.request.plan().query_span() {
            <<R::Request as route::Schema>::Query as query::Fields<R::Request>>::parse(
                query::Input::new(raw, self.input.as_slice(), query.start()..query.end()),
            )
            .map_err(Request::Query)?;
        }
        Ok(())
    }
}

pub(crate) struct Frame<'a> {
    body: &'a [u8],
    wire: &'a [u8],
}

impl<'a> Frame<'a> {
    pub(crate) const fn body(&self) -> &'a [u8] {
        self.body
    }

    pub(crate) const fn wire(&self) -> &'a [u8] {
        self.wire
    }
}

/// Append-only ownership of a fragmented request head.
pub type Source = Snapshot<{ recv::BODY_CAPACITY }>;
/// A parsed request line bound to its append-only owner.
pub type Owned = line::Owned<{ recv::BODY_CAPACITY }>;

/// Mandatory admission retained until a cold route rejection can be emitted.
pub struct Rejected<'d> {
    source: scan::State<Source>,
    reply: fixed::Body<'d>,
}

impl<'d> Rejected<'d> {
    pub fn len(&self) -> usize {
        self.source.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn start<P, D, H>(
        head: &Head<'_>,
        permit: session::Permit,
        reply: fixed::Body<'d>,
    ) -> session::RouteOutcome<'d, P, D, H, Self> {
        let source = match scan::State::try_request(
            *head.request(),
            Limit::DEFAULT.with_bytes(recv::HEAD_CAPACITY),
        ) {
            Ok(source) => source,
            Err(error) => return session::Outcome::Close(rejection(error)).into(),
        };
        match source.advance(|_| Ok::<_, Infallible>(())) {
            Ok(scan::Progress::Complete(_)) => session::Outcome::Close(reply).into(),
            Err((_, error)) => session::Outcome::Close(rejection(error)).into(),
            Ok(scan::Progress::More(source)) => {
                let capacity = NonZero::new(source.as_ref().len()).unwrap_or(NonZero::<usize>::MIN);
                match source.try_snapshot(capacity) {
                    Ok(source) => session::RouteOutcome::AwaitReject {
                        permit,
                        pending: Self { source, reply },
                    },
                    Err(error) => failed(error).into(),
                }
            }
        }
    }

    pub fn start_owned<P, D, H>(
        source: Owned,
        permit: session::Permit,
        reply: fixed::Body<'d>,
    ) -> session::RouteOutcome<'d, P, D, H, Self> {
        let source =
            match scan::State::try_line(source, Limit::DEFAULT.with_bytes(recv::HEAD_CAPACITY)) {
                Ok(source) => source,
                Err(rejected) => {
                    let (_, error) = rejected.into_parts();
                    return session::Outcome::Close(rejection(error)).into();
                }
            };
        Self { source, reply }.advance(permit)
    }

    pub fn resume<P, D, H>(
        mut self,
        input: Input<'_>,
        permit: session::Permit,
    ) -> session::RouteOutcome<'d, P, D, H, Self> {
        if let Err(error) = self.source.try_extend(input.as_slice()) {
            return failed(error).into();
        }
        self.advance(permit)
    }

    fn advance<P, D, H>(self, permit: session::Permit) -> session::RouteOutcome<'d, P, D, H, Self> {
        let Self { source, reply } = self;
        match source.advance(|_| Ok::<_, Infallible>(())) {
            Ok(scan::Progress::Complete(_)) => session::Outcome::Close(reply).into(),
            Err((_, error)) => session::Outcome::Close(rejection(error)).into(),
            Ok(scan::Progress::More(source)) => session::RouteOutcome::AwaitReject {
                permit,
                pending: Self { source, reply },
            },
        }
    }
}

fn failed<'d, D>(error: storage::AllocError) -> session::Outcome<'d, D> {
    session::Outcome::Failed(Failure::Input(InputError::Storage(error)))
}

pub(super) fn rejection<'d>(cause: ParseError<Infallible>) -> fixed::Body<'d> {
    match cause {
        ParseError::Boundary(
            Boundary::TooLarge { .. }
            | Boundary::Span(span::Error::Overflow { .. } | span::Error::Bounds { .. }),
        )
        | ParseError::Field(
            syntax::Error::TooLarge | syntax::Error::TooMany | syntax::Error::TooLong,
        ) => canned::PAYLOAD_TOO_LARGE,
        ParseError::Boundary(
            Boundary::Invalid
            | Boundary::Span(span::Error::Order { .. } | span::Error::Shift { .. }),
        )
        | ParseError::Field(
            syntax::Error::Invalid
            | syntax::Error::Name { .. }
            | syntax::Error::Value { .. }
            | syntax::Error::Ending { .. },
        )
        | ParseError::Semantic(_) => canned::BAD_REQUEST,
        ParseError::Sink(never) => match never {},
    }
}

/// Append-only ownership while the initial request line is incomplete.
pub(crate) struct Line {
    source: scan::State<Source>,
}

#[doc(hidden)]
pub enum LineError<'d> {
    Close(fixed::Body<'d>),
    Failed(Failure),
}

impl<'d> LineError<'d> {
    pub fn response(self, failure: impl FnOnce(&Failure) -> fixed::Body<'d>) -> fixed::Body<'d> {
        match self {
            Self::Close(body) => body,
            Self::Failed(error) => failure(&error),
        }
    }
}

pub(crate) enum LineProgress {
    Pending(Line),
    Complete { source: Source, prior: usize },
}

impl Line {
    pub(crate) fn start<'d>(input: Input<'_>) -> Result<Self, LineError<'d>> {
        let source = scan::State::pending_line(
            input.as_slice(),
            Limit::DEFAULT.with_bytes(recv::HEAD_CAPACITY),
        )
        .map_err(|(_, error)| LineError::Close(rejection(error)))?;
        let capacity = NonZero::new(source.as_ref().len()).unwrap_or(NonZero::<usize>::MIN);
        let source = source.try_snapshot(capacity).map_err(line_failure)?;
        Ok(Self { source })
    }

    pub(crate) fn len(&self) -> usize {
        self.source.as_ref().len()
    }

    pub(crate) fn advance<'d>(mut self, input: Input<'_>) -> Result<LineProgress, LineError<'d>> {
        let prior = self.len();
        self.source
            .try_extend(input.as_slice())
            .map_err(line_failure)?;
        match self
            .source
            .advance_line()
            .map_err(|(_, error)| LineError::Close(rejection(error)))?
        {
            Progress::More(source) => Ok(LineProgress::Pending(Self { source })),
            Progress::Complete(source) => Ok(LineProgress::Complete { source, prior }),
        }
    }
}

fn line_failure<'d>(error: storage::AllocError) -> LineError<'d> {
    LineError::Failed(Failure::Input(InputError::Storage(error)))
}
