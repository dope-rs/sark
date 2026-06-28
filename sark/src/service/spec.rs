use std::ops::Range;

use super::PathProbe;
use super::request_impl::RouteRequestImpl;
use crate::{Request, request};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PathCapture {
    pub start: usize,
    pub end: usize,
}

impl PathCapture {
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub const fn as_range(self) -> Range<usize> {
        self.start..self.end
    }
}

pub trait RouteParams: Sized {
    type Raw: Default;
    type Captures;

    fn from_raw(req: &Request, raw: Self::Raw) -> Option<Self>;

    fn from_captures<P: PathProbe>(path: &P, captures: Self::Captures) -> Option<Self>;
}

pub struct NoParams;

impl RouteParams for NoParams {
    type Raw = Self;
    type Captures = ();

    fn from_raw(_req: &Request, _raw: Self::Raw) -> Option<Self> {
        Some(Self)
    }

    fn from_captures<P: PathProbe>(_path: &P, _captures: Self::Captures) -> Option<Self> {
        Some(Self)
    }
}

pub trait RouteParamsRef<'req>: RouteParams {
    fn from_raw_ref(req: &request::Ref<'req, ()>, raw: <Self as RouteParams>::Raw) -> Option<Self>;
}

impl<'req> RouteParamsRef<'req> for NoParams {
    fn from_raw_ref(_req: &request::Ref<'req, ()>, _raw: Self::Raw) -> Option<Self> {
        Some(Self)
    }
}

impl Default for NoParams {
    fn default() -> Self {
        Self
    }
}

pub trait HeaderParams: Sized {}

#[derive(Default)]
pub struct NoHeaders;

impl HeaderParams for NoHeaders {}

pub struct EmptyParamsInner<'req> {
    #[doc(hidden)]
    pub __sark_m: core::marker::PhantomData<&'req ()>,
}

#[derive(Default)]
pub struct EmptyParamsRaw;

impl RouteParams for EmptyParamsRaw {
    type Raw = Self;
    type Captures = ();

    fn from_raw(_req: &crate::Request, raw: Self::Raw) -> Option<Self> {
        Some(raw)
    }

    fn from_captures<P: PathProbe>(_path: &P, _captures: Self::Captures) -> Option<Self> {
        Some(Self)
    }
}

impl<'req> RouteParams for EmptyParamsInner<'req> {
    type Raw = EmptyParamsRaw;
    type Captures = ();

    fn from_raw(_req: &crate::Request, _raw: Self::Raw) -> Option<Self> {
        Some(Self {
            __sark_m: core::marker::PhantomData,
        })
    }

    fn from_captures<P: PathProbe>(_path: &P, _captures: Self::Captures) -> Option<Self> {
        Some(Self {
            __sark_m: core::marker::PhantomData,
        })
    }
}

impl<'req> RouteParamsRef<'req> for EmptyParamsInner<'req> {
    fn from_raw_ref(_req: &request::Ref<'req, ()>, _raw: Self::Raw) -> Option<Self> {
        Some(Self {
            __sark_m: core::marker::PhantomData,
        })
    }
}

pub trait RouteSpec {
    type Request: RouteRequestImpl<
            HeaderSlot = Self::HeaderSlot,
            RawHeaders = Self::RawHeaders,
            RawParams = Self::RawParams,
        > + for<'req> RouteRequestImpl<
            ParamsInner<'req> = Self::Params<'req>,
            HeadersInner<'req> = Self::Headers<'req>,
        >;

    type Params<'req>: RouteParams + RouteParamsRef<'req>;
    type RawParams: Default;
    type Headers<'req>: HeaderParams;
    type RawHeaders: Default;
    type HeaderSlot: Copy;
    type Response<'req>: sark_core::http::Shape<'req>;
    type ParsedBody<'req>;

    type Captures;

    const STATIC_RESPONSE: bool = false;
    const RESPONSE_BODY_KIND: sark_core::http::body_kind::ResponseKind =
        sark_core::http::body_kind::ResponseKind::Inline;
    const REQUEST_BODY_KIND: sark_core::http::body_kind::RequestKind =
        sark_core::http::body_kind::RequestKind::Inline;
    const STREAMING_BODY: bool = matches!(
        Self::REQUEST_BODY_KIND,
        sark_core::http::body_kind::RequestKind::Stream,
    );
    const MAX_BODY: usize = 1024 * 1024;

    /// Emit the `Date` response header. `#[skip(date)]` sets this `false` for a
    /// route, trimming the header from its (static) responses.
    const EMIT_DATE: bool = true;
    /// Emit the `Server` response header. `#[skip(server)]` sets this `false`.
    const EMIT_SERVER: bool = true;

    fn parse_body<'req>(raw: &'req [u8]) -> sark_core::error::Result<Self::ParsedBody<'req>>;

    fn from_captures<P: PathProbe>(path: &P, captures: Self::Captures) -> Option<Self::RawParams>;
}
