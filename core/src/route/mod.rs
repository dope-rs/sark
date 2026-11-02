use std::{fmt, num::NonZero};

use sark_http::{
    payload::{owned, response::dispatch},
    request::Id,
};

use crate::{
    body::{
        self,
        admit::{Retention, Stream},
    },
    request,
    request::frame::proof,
};

mod error;
mod input;
mod never;
mod reject;

pub use error::Error;
pub use input::Input;
pub use never::Never;
pub use reject::Reject;
pub mod field;
pub mod guard;
pub mod raw;
pub mod table;

use field::{header, query};

/// Fiber-free, cooperatively stepped construction of an owned request.
pub trait TaskBuild<'d>: Sized {
    type Task;
    type StepError: fmt::Debug;

    fn reject(error: Self::StepError) -> Reject;

    /// Commits the accepted logical body length and closes the input stream.
    fn finish_body(self, len: usize) -> Self;

    /// Consumes one construction state and returns its exact successor.
    fn step(
        self,
        budget: NonZero<usize>,
    ) -> body::Step<Self, Built<'d, Self::Task>, Self::StepError>;
}

/// Capability to transfer retained body ownership into a task builder.
pub trait Push<'d>: TaskBuild<'d> {
    type Error;

    /// Borrows the exact local failure without consuming retained input.
    fn observe<C: Copy, S: Copy, O: crate::life::Observer<C, S>>(
        error: &Self::Error,
        observer: &mut O,
    );

    fn reject_push(error: Self::Error) -> Reject;

    fn push_body(self, segment: owned::Segment<'d>) -> body::Transfer<Self, Self::Error>;
}

/// A completed owned request paired with its ingress identity.
pub struct Built<'d, T> {
    id: Id<'d>,
    request: T,
}

impl<'d, T> Built<'d, T> {
    #[must_use]
    pub const fn new(id: Id<'d>, request: T) -> Self {
        Self { id, request }
    }

    #[must_use]
    pub fn into_parts(self) -> (Id<'d>, T) {
        (self.id, self.request)
    }
}

/// Wire-level request schema shared by every transport and execution mode.
pub trait Schema: Sized {
    type Header: header::Policy<Self> + header::Fields<Self>;
    type Query: query::Fields<Self>;
    type RawHeaders: Copy + Default;
    type Error;
    type RawParams: RawParams;
    type Body: body::Project;
    type Frame: proof::Proof;

    const FULL: bool = false;
    /// Whether the generated handler projects the effective request
    /// authority and therefore requires compressed protocols to retain it.
    const AUTHORITY: bool = false;
    const BODY_RETENTION: Retention = <Self::Body as body::Project>::RETENTION;
}

pub trait Fields: Schema<Error = Error<Self::Identity>> {
    type Identity: Copy;
}

/// Borrowed request projection used by synchronous and duplex routes.
pub trait Project: Schema {
    type Params<'req>;
    type Headers<'req>;
    type Error<'req>;
    type View<'req, 'd>
    where
        'd: 'req;
    type ParsedBody<'req, 'd>
    where
        'd: 'req;

    fn reject<'req>(error: <Self as Project>::Error<'req>) -> Reject;

    fn from_params<'req, 'body>(
        request: &request::raw::Parsed<request::Head<&'req [u8], Self::Frame>, Self>,
    ) -> Result<Self::Params<'req>, <Self as Project>::Error<'body>>;

    fn parse_body<'req, 'd>(
        body: <Self::Body as body::Project>::View<'req, 'd>,
    ) -> Result<Self::ParsedBody<'req, 'd>, <Self as Project>::Error<'req>>
    where
        'd: 'req;

    fn from_parts<'req, 'd>(
        params: Self::Params<'req>,
        headers: Self::Headers<'req>,
        body: Self::ParsedBody<'req, 'd>,
        request: &request::View<'req, 'd, Self::Frame>,
    ) -> Self::View<'req, 'd>
    where
        'd: 'req;

    fn from_headers<'req, 'body>(
        request: &request::raw::Parsed<request::Head<&'req [u8], Self::Frame>, Self>,
    ) -> Result<Self::Headers<'req>, <Self as Project>::Error<'body>>;
}

/// Body-input strategy selected by an owned request schema.
pub trait Mode: sealed::Mode {
    const STREAM: Stream;
}

/// The builder consumes segments before the complete message body arrives.
#[derive(Debug)]
pub struct Streaming;

/// The builder requires the complete body before construction.
#[derive(Debug)]
pub struct Buffered;

impl Mode for Streaming {
    const STREAM: Stream = Stream::Streaming;
}

impl Mode for Buffered {
    const STREAM: Stream = Stream::Buffered;
}

mod sealed {
    pub trait Mode {}

    impl Mode for super::Streaming {}
    impl Mode for super::Buffered {}
}

/// Retained request construction used by cooperatively scheduled routes.
pub trait Own: Schema {
    type Mode: Mode;

    /// User-visible request value backed by retained ingress ownership.
    type Task<'d>;
    type Error<'d>;
    type TaskBuilder<'d>: TaskBuild<'d, Task = Self::Task<'d>>;

    /// Optionally constructs an input-independent request before task admission.
    /// Return `None` when construction or destruction has observable effects.
    fn empty_task<'d>(_: Id<'d>) -> Option<Built<'d, Self::Task<'d>>> {
        None
    }

    fn reject<'d>(error: <Self as Own>::Error<'d>) -> Reject;

    fn start_task<'d>(
        request: request::raw::Parsed<request::Retained<'d, Self::Frame>, Self>,
    ) -> Result<Self::TaskBuilder<'d>, <Self as Own>::Error<'d>>;

    fn start_stream<'d>(
        request: request::raw::Parsed<request::Parts<'d, Self::Frame>, Self>,
    ) -> Result<Self::TaskBuilder<'d>, <Self as Own>::Error<'d>>;
}

pub trait RawParams: Copy {
    type Captures;

    /// Canonically sorted request path-field names.
    const PATH_NAMES: &'static [&'static [u8]];
}

/// Raw path fields constructed directly from a trusted route match.
pub trait Params: RawParams {
    fn from_match(captures: Self::Captures) -> Self;
}

impl RawParams for () {
    type Captures = ();
    const PATH_NAMES: &'static [&'static [u8]] = &[];
}

pub trait Spec {
    type Kind;

    type Request: Project;
    type Stream<'d>;
    const MAX_BODY: usize = 1024 * 1024;
}

/// A route whose task retains its response until driver-local completion.
pub trait Task<'d, S>: Spec {
    type Output: dispatch::IntoOutput<'d, 'd, Output: dispatch::Complete<'d, 'd>>;
}
