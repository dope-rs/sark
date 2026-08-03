#![allow(clippy::too_many_arguments)]

use std::ops::Range;

use sark_core::error::Result;
use sark_core::http::codec::BodyFraming;
use sark_core::http::head::Flags;

use super::plan::HeaderValue;
use super::spec::RouteParams;
use crate::dispatch::BodySource;
use crate::request;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyPolicy {
    Buffered,
    Discarded,
}

mod body_mode {
    use super::BodyPolicy;
    use crate::dispatch::BodySource;

    pub trait Sealed {
        const POLICY: BodyPolicy;

        fn bytes<B: BodySource>(body: &mut B) -> &[u8];
    }

    pub enum Buffered {}

    impl Sealed for Buffered {
        const POLICY: BodyPolicy = BodyPolicy::Buffered;

        fn bytes<B: BodySource>(body: &mut B) -> &[u8] {
            body.contiguous()
        }
    }

    pub enum Discarded {}

    impl Sealed for Discarded {
        const POLICY: BodyPolicy = BodyPolicy::Discarded;

        fn bytes<B: BodySource>(_body: &mut B) -> &[u8] {
            &[]
        }
    }
}

pub trait BodyMode: body_mode::Sealed {
    const POLICY: BodyPolicy = <Self as body_mode::Sealed>::POLICY;

    fn bytes<B: BodySource>(body: &mut B) -> &[u8] {
        <Self as body_mode::Sealed>::bytes(body)
    }
}

impl BodyMode for body_mode::Buffered {}
impl BodyMode for body_mode::Discarded {}

pub use body_mode::{Buffered, Discarded};

pub enum HeaderParse<H> {
    NeedMore,
    Bad,
    Ready {
        headers: H,
        head_len: usize,
        body_framing: BodyFraming,
        flags: Flags,
        accept_gzip: bool,
    },
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderLineOutcome {
    Complete(usize),
    NeedMore,
    Bad,
}

pub trait HeaderSlot: Copy {
    fn into_tag(self) -> u16;
    fn from_tag(tag: u16) -> Option<Self>;
}

pub trait RouteRequestImpl {
    type HeaderSlot: HeaderSlot;
    type RawHeaders: Default;
    type RawParams: Default;
    type Params<'req>: RouteParams<'req, Raw = Self::RawParams>;
    type Headers<'req>;
    type ParsedBody<'req>;
    type BodyMode: BodyMode;

    const FULL: bool = false;
    const BODY_POLICY: BodyPolicy = <Self::BodyMode as BodyMode>::POLICY;

    fn parse_body<'req>(raw: &'req [u8]) -> Result<Self::ParsedBody<'req>>;

    fn header_slot_bytes(_name: &[u8]) -> Option<Self::HeaderSlot> {
        None
    }

    fn parse_headers<const PARSE_ACCEPT_ENCODING: bool>(
        req_bytes: &[u8],
        headers_start: usize,
        max_header_count: usize,
    ) -> HeaderParse<Self::RawHeaders>;

    fn set_header_raw<V: HeaderValue>(
        _headers: &mut Self::RawHeaders,
        _slot: Self::HeaderSlot,
        _value: &V,
    ) -> Result<()> {
        Ok(())
    }

    fn set_query_name_raw<V: HeaderValue>(
        _headers: &mut Self::RawHeaders,
        _name: &[u8],
        _value: &V,
    ) -> Result<()> {
        Ok(())
    }

    fn set_query_slice_raw(
        _headers: &mut Self::RawHeaders,
        _name: &[u8],
        _input: &[u8],
        _range: Range<usize>,
    ) -> Result<()> {
        Ok(())
    }

    fn parse_query_raw(
        _headers: &mut Self::RawHeaders,
        _input: &[u8],
        _range: Range<usize>,
    ) -> Result<()> {
        Ok(())
    }

    fn build_headers<'req>(
        req: &request::Ref<'req>,
        headers: Self::RawHeaders,
    ) -> Result<Self::Headers<'req>>;

    fn build_params<'req>(
        req: &request::Ref<'req>,
        params: Self::RawParams,
    ) -> Option<Self::Params<'req>>
    where
        Self::Params<'req>: RouteParams<'req>,
    {
        <Self::Params<'req> as RouteParams<'req>>::from_raw(req, params)
    }
}
