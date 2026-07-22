use std::ops::Range;

use super::PathProbe;
use super::request_impl::RouteRequestImpl;
use crate::request;

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

pub trait RawRouteParams: Default {
    type Captures;

    fn from_captures<P: PathProbe>(path: &P, captures: Self::Captures) -> Option<Self>;
}

impl RawRouteParams for () {
    type Captures = ();

    fn from_captures<P: PathProbe>(_path: &P, _captures: Self::Captures) -> Option<Self> {
        Some(())
    }
}

pub trait RouteParams<'req>: Sized {
    type Raw: Default;

    fn from_raw(req: &request::Ref<'req>, raw: Self::Raw) -> Option<Self>;
}

impl<'req> RouteParams<'req> for () {
    type Raw = ();

    fn from_raw(_req: &request::Ref<'req>, _raw: Self::Raw) -> Option<Self> {
        Some(())
    }
}

pub trait RouteSpec {
    type Kind;

    type Request: RouteRequestImpl<
            HeaderSlot = Self::HeaderSlot,
            RawHeaders = Self::RawHeaders,
            RawParams = Self::RawParams,
        > + for<'req> RouteRequestImpl<
            Params<'req> = Self::Params<'req>,
            Headers<'req> = Self::Headers<'req>,
        >;

    type Params<'req>: RouteParams<'req, Raw = Self::RawParams>;
    type RawParams: RawRouteParams<Captures = Self::Captures>;
    type Headers<'req>;
    type RawHeaders: Default;
    type HeaderSlot: Copy;
    type Response<'req>: sark_core::http::Shape<'req>;
    type AsyncResponse: sark_core::http::OwnedShape;
    type Stream: 'static;
    type ParsedBody<'req>;

    type Captures;

    const STATIC_RESPONSE: bool = false;
    const RESPONSE_BODY_KIND: sark_core::http::body_kind::ResponseKind =
        sark_core::http::body_kind::ResponseKind::Inline;
    const BODY_POLICY: super::BodyPolicy = <Self::Request as RouteRequestImpl>::BODY_POLICY;
    const MAX_BODY: usize = 1024 * 1024;

    const EMIT_DATE: bool = true;
    const EMIT_SERVER: bool = true;

    fn parse_body<'req>(raw: &'req [u8]) -> sark_core::error::Result<Self::ParsedBody<'req>>;

    fn from_captures<P: PathProbe>(path: &P, captures: Self::Captures) -> Option<Self::RawParams>;
}
