#![allow(clippy::too_many_arguments)]

use std::ops::Range;

use sark_core::error::Result;
use sark_core::http::codec::HeaderScan;
use sark_core::http::head::{
    BytesScan, Flags, HeadInput, HeaderLineScan, apply_well_known_header,
    apply_well_known_header_contig,
};

use super::plan::HeaderValue;
use super::spec::{HeaderParams, RouteParams, RouteParamsRef};
use crate::{Request, request};

pub trait RouteRequestImpl {
    type HeaderSlot: Copy;
    type RawHeaders: Default;
    type RawParams: Default;
    type ParamsInner<'req>: RouteParams<Raw = Self::RawParams>;
    type HeadersInner<'req>: HeaderParams;

    const NEED_HEADER: bool = false;
    const NEED_KNOWN_HEADER: bool = false;
    const NEED_QUERY: bool = false;

    fn header_slot_bytes(_name: &[u8]) -> Option<Self::HeaderSlot> {
        None
    }

    fn header_slot_u8(_name: &[u8]) -> Option<u8> {
        None
    }

    fn scan_header_contig(rest: &[u8]) -> Result<Option<HeaderLineScan>> {
        Ok(BytesScan::find_header_line_from(rest, 0))
    }

    fn apply_header_contig<I: HeadInput + ?Sized>(
        _headers: &mut Self::RawHeaders,
        _input: &I,
        rest: &[u8],
        _line_start: usize,
        scan: &mut HeaderScan,
        flags: &mut Flags,
        header_count: &mut usize,
        max_header_count: usize,
    ) -> Result<Option<usize>> {
        apply_well_known_header_contig(rest, scan, flags, &mut (), header_count, max_header_count)
    }

    fn apply_header<I: HeadInput + ?Sized>(
        _headers: &mut Self::RawHeaders,
        input: &I,
        line: &[u8],
        line_start: usize,
        colon_idx: usize,
        pretrim_start: Option<usize>,
        pretrim_end: Option<usize>,
        scan: &mut HeaderScan,
        flags: &mut Flags,
        scan_info: Option<&HeaderLineScan>,
    ) -> Result<()> {
        apply_well_known_header(
            input,
            line,
            line_start,
            colon_idx,
            pretrim_start,
            pretrim_end,
            scan,
            flags,
            scan_info,
        )
    }

    fn set_header_raw<V: HeaderValue>(
        _headers: &mut Self::RawHeaders,
        _slot: Self::HeaderSlot,
        _value: &V,
    ) -> Result<()> {
        Ok(())
    }

    fn set_header_name_raw<V: HeaderValue>(
        _headers: &mut Self::RawHeaders,
        _name: &[u8],
        _value: &V,
    ) -> Result<()> {
        Ok(())
    }

    fn set_header_u8<V: HeaderValue>(
        _headers: &mut Self::RawHeaders,
        _slot: u8,
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

    fn build_headers(
        req: &Request,
        headers: Self::RawHeaders,
    ) -> Result<Self::HeadersInner<'static>>;

    fn build_params(req: &Request, params: Self::RawParams) -> Option<Self::ParamsInner<'static>> {
        <Self::ParamsInner<'static> as RouteParams>::from_raw(req, params)
    }

    fn build_headers_ref<'req>(
        req: &request::Ref<'req, ()>,
        headers: Self::RawHeaders,
    ) -> Result<Self::HeadersInner<'req>>;

    fn build_params_ref<'req>(
        req: &request::Ref<'req, ()>,
        params: Self::RawParams,
    ) -> Option<Self::ParamsInner<'req>>
    where
        Self::ParamsInner<'req>: RouteParamsRef<'req>,
    {
        <Self::ParamsInner<'req> as RouteParamsRef<'req>>::from_raw_ref(req, params)
    }
}
