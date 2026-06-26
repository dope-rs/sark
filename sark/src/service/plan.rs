use std::ops::Range;

use sark_core::error::Result;
use sark_core::http::LocalFrameBytes;
use sark_core::utils::bytes::Ascii;

use crate::routes::path::seg_next;

pub trait HeaderValue {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn eq_bytes(&self, expected: &[u8]) -> bool;

    fn eq_ignore_ascii_case(&self, expected: &[u8]) -> bool;

    fn as_range(&self) -> Range<usize>;

    fn copy_local(&self) -> LocalFrameBytes;

    fn parse_usize(&self) -> Result<usize>;

    fn parse_u64(&self) -> Result<u64>;
}

impl HeaderValue for LocalFrameBytes {
    fn len(&self) -> usize {
        self.len()
    }

    fn eq_bytes(&self, expected: &[u8]) -> bool {
        self.as_bytes() == expected
    }

    fn eq_ignore_ascii_case(&self, expected: &[u8]) -> bool {
        self.as_bytes().eq_ignore_ascii_case(expected)
    }

    fn as_range(&self) -> Range<usize> {
        0..self.len()
    }

    fn copy_local(&self) -> LocalFrameBytes {
        self.clone()
    }

    fn parse_usize(&self) -> Result<usize> {
        Ascii::parse_usize(self.as_bytes())
            .ok_or_else(sark_core::error::Error::invalid_integer_header)
    }

    fn parse_u64(&self) -> Result<u64> {
        Ascii::parse_u64(self.as_bytes())
            .ok_or_else(sark_core::error::Error::invalid_integer_header)
    }
}

pub struct SliceValue<'a> {
    raw: &'a [u8],
    start: usize,
    end: usize,
}

impl<'a> SliceValue<'a> {
    pub const fn new(raw: &'a [u8], range: Range<usize>) -> Self {
        Self {
            raw,
            start: range.start,
            end: range.end,
        }
    }

    fn bytes(&self) -> &[u8] {
        self.raw.get(self.start..self.end).unwrap_or(&[])
    }
}

impl HeaderValue for SliceValue<'_> {
    fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    fn eq_bytes(&self, expected: &[u8]) -> bool {
        self.bytes() == expected
    }

    fn eq_ignore_ascii_case(&self, expected: &[u8]) -> bool {
        self.bytes().eq_ignore_ascii_case(expected)
    }

    fn as_range(&self) -> Range<usize> {
        self.start..self.end
    }

    fn copy_local(&self) -> LocalFrameBytes {
        LocalFrameBytes::from_shared(o3::buffer::Shared::copy_from_slice(self.bytes()))
    }

    fn parse_usize(&self) -> Result<usize> {
        Ascii::parse_usize(self.bytes()).ok_or_else(sark_core::error::Error::invalid_integer_header)
    }

    fn parse_u64(&self) -> Result<u64> {
        Ascii::parse_u64(self.bytes()).ok_or_else(sark_core::error::Error::invalid_integer_header)
    }
}

pub trait FieldValue: Sized {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self>;

    fn parse_value_bytes(value: &[u8], abs_start: usize) -> Result<Self>;

    fn parse_path<P: PathProbe>(_path: &P, _start: usize, _end: usize) -> Option<Self> {
        None
    }
}

impl FieldValue for Range<usize> {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self> {
        Ok(value.as_range())
    }

    fn parse_value_bytes(value: &[u8], abs_start: usize) -> Result<Self> {
        Ok(abs_start..abs_start + value.len())
    }

    fn parse_path<P: PathProbe>(_path: &P, start: usize, end: usize) -> Option<Self> {
        Some(start..end)
    }
}

impl FieldValue for LocalFrameBytes {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self> {
        Ok(value.copy_local())
    }

    fn parse_value_bytes(value: &[u8], _abs_start: usize) -> Result<Self> {
        Ok(LocalFrameBytes::from_shared(
            o3::buffer::Shared::copy_from_slice(value),
        ))
    }

    fn parse_path<P: PathProbe>(path: &P, start: usize, end: usize) -> Option<Self> {
        path.copy_range_local(start, end)
    }
}

impl FieldValue for usize {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self> {
        value.parse_usize()
    }

    fn parse_value_bytes(value: &[u8], _abs_start: usize) -> Result<Self> {
        Ascii::parse_usize(value).ok_or_else(sark_core::error::Error::invalid_integer_header)
    }

    fn parse_path<P: PathProbe>(path: &P, start: usize, end: usize) -> Option<Self> {
        path.parse_range_usize(start, end)
    }
}

impl FieldValue for u64 {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self> {
        value.parse_u64()
    }

    fn parse_value_bytes(value: &[u8], _abs_start: usize) -> Result<Self> {
        Ascii::parse_u64(value).ok_or_else(sark_core::error::Error::invalid_integer_header)
    }

    fn parse_path<P: PathProbe>(path: &P, start: usize, end: usize) -> Option<Self> {
        path.parse_range_u64(start, end)
    }
}

impl FieldValue for bool {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self> {
        if value.eq_ignore_ascii_case(b"true") || value.eq_bytes(b"1") {
            return Ok(true);
        }
        if value.eq_ignore_ascii_case(b"false") || value.eq_bytes(b"0") {
            return Ok(false);
        }
        Err(sark_core::error::Error::BadRequest(
            "Invalid boolean field".into(),
        ))
    }

    fn parse_value_bytes(value: &[u8], _abs_start: usize) -> Result<Self> {
        if value.eq_ignore_ascii_case(b"true") || value == b"1" {
            return Ok(true);
        }
        if value.eq_ignore_ascii_case(b"false") || value == b"0" {
            return Ok(false);
        }
        Err(sark_core::error::Error::BadRequest(
            "Invalid boolean field".into(),
        ))
    }

    fn parse_path<P: PathProbe>(path: &P, start: usize, end: usize) -> Option<Self> {
        if path.eq_range_ignore_ascii_case(start, end, b"true") || path.eq_range(start, end, b"1") {
            return Some(true);
        }
        if path.eq_range_ignore_ascii_case(start, end, b"false") || path.eq_range(start, end, b"0")
        {
            return Some(false);
        }
        None
    }
}

pub trait PathProbe {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn eq_bytes(&self, expected: &[u8]) -> bool;

    fn eq_range(&self, start: usize, end: usize, expected: &[u8]) -> bool;

    fn eq_range_ignore_ascii_case(&self, start: usize, end: usize, expected: &[u8]) -> bool;

    fn parse_range_usize(&self, start: usize, end: usize) -> Option<usize>;

    fn parse_range_u64(&self, start: usize, end: usize) -> Option<u64>;

    fn copy_range_local(&self, start: usize, end: usize) -> Option<LocalFrameBytes>;

    fn next_seg(&self, idx: usize) -> Option<(usize, usize, usize)>;

    fn probe_literal(&self, cur: usize, lit: &[u8]) -> Option<usize> {
        let (start, end, nx) = self.next_seg(cur)?;
        if self.eq_range(start, end, lit) {
            Some(nx)
        } else {
            None
        }
    }

    fn first_seg(&self) -> Option<(usize, usize)> {
        self.next_seg(0).map(|(start, end, _)| (start, end))
    }
}

pub struct SlicePath<'a> {
    raw: &'a [u8],
}

impl<'a> SlicePath<'a> {
    pub const fn new(raw: &'a [u8]) -> Self {
        Self { raw }
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.raw
    }
}

impl PathProbe for SlicePath<'_> {
    fn len(&self) -> usize {
        self.raw.len()
    }

    fn eq_bytes(&self, expected: &[u8]) -> bool {
        self.raw == expected
    }

    fn eq_range(&self, start: usize, end: usize, expected: &[u8]) -> bool {
        if end < start || end > self.raw.len() || expected.len() != end - start {
            return false;
        }
        self.raw[start..end] == *expected
    }

    fn eq_range_ignore_ascii_case(&self, start: usize, end: usize, expected: &[u8]) -> bool {
        if end < start || end > self.raw.len() || expected.len() != end - start {
            return false;
        }
        self.raw[start..end].eq_ignore_ascii_case(expected)
    }

    fn parse_range_usize(&self, start: usize, end: usize) -> Option<usize> {
        if end < start || end > self.raw.len() {
            return None;
        }
        Ascii::parse_usize(&self.raw[start..end])
    }

    fn parse_range_u64(&self, start: usize, end: usize) -> Option<u64> {
        if end < start || end > self.raw.len() {
            return None;
        }
        Ascii::parse_u64(&self.raw[start..end])
    }

    fn copy_range_local(&self, start: usize, end: usize) -> Option<LocalFrameBytes> {
        if end < start || end > self.raw.len() {
            return None;
        }
        Some(LocalFrameBytes::from_shared(
            o3::buffer::Shared::copy_from_slice(&self.raw[start..end]),
        ))
    }

    fn next_seg(&self, idx: usize) -> Option<(usize, usize, usize)> {
        seg_next(self.raw, idx)
    }

    fn probe_literal(&self, cur: usize, lit: &[u8]) -> Option<usize> {
        let start = cur + 1;
        let end = start + lit.len();
        if end > self.raw.len() || self.raw[start..end] != *lit {
            return None;
        }
        if end < self.raw.len() && self.raw[end] != b'/' {
            return None;
        }
        Some(end)
    }
}

impl PathProbe for crate::request::PathView<'_> {
    fn len(&self) -> usize {
        (*self).len()
    }

    fn eq_bytes(&self, expected: &[u8]) -> bool {
        (*self).eq_bytes(expected)
    }

    fn eq_range(&self, start: usize, end: usize, expected: &[u8]) -> bool {
        (*self).eq_range(start, end, expected)
    }

    fn eq_range_ignore_ascii_case(&self, start: usize, end: usize, expected: &[u8]) -> bool {
        (*self).eq_range_ignore_ascii_case(start, end, expected)
    }

    fn parse_range_usize(&self, start: usize, end: usize) -> Option<usize> {
        (*self).parse_usize_range(start, end)
    }

    fn parse_range_u64(&self, start: usize, end: usize) -> Option<u64> {
        (*self).parse_u64_range(start, end)
    }

    fn copy_range_local(&self, start: usize, end: usize) -> Option<LocalFrameBytes> {
        (*self).copy_range_local(start, end)
    }

    fn next_seg(&self, idx: usize) -> Option<(usize, usize, usize)> {
        (*self).next_seg(idx)
    }
}

pub trait HeadPlan {
    type RouteKey;

    fn route<P: PathProbe>(&self, method: &http::Method, path: &P) -> Self::RouteKey {
        self.route_key_probe(crate::service::Key::from_method(method), path)
    }

    fn route_key_probe<P: PathProbe>(
        &self,
        method_key: crate::service::Key,
        path: &P,
    ) -> Self::RouteKey;
}

pub struct FullHeadPlan;

impl HeadPlan for FullHeadPlan {
    type RouteKey = ();

    fn route_key_probe<P: PathProbe>(
        &self,
        _method_key: crate::service::Key,
        _path: &P,
    ) -> Self::RouteKey {
    }
}

pub trait HeadParts<K>: Sized {
    const NEED_FIELDS: bool;
    const NEED_HEADER: bool = false;
    const NEED_KNOWN_HEADER: bool = false;
    const NEED_QUERY: bool = false;

    fn new(route: K) -> Self;
    fn wants_query(&self) -> bool {
        false
    }
    fn route_tag(&self) -> u64 {
        0
    }

    fn set_header_name<V>(&mut self, name: &[u8], value: &V) -> Result<()>
    where
        V: HeaderValue;

    #[allow(clippy::too_many_arguments)]
    fn apply_header<I: sark_core::http::head::HeadInput + ?Sized>(
        &mut self,
        input: &I,
        line: &[u8],
        line_start: usize,
        colon_idx: usize,
        pretrim_start: Option<usize>,
        pretrim_end: Option<usize>,
        scan: &mut sark_core::http::codec::HeaderScan,
        flags: &mut sark_core::http::head::Flags,
        scan_info: Option<&sark_core::http::head::HeaderLineScan>,
    ) -> Result<()> {
        crate::parser::head::HeaderApply::generic::<I, K, Self>(
            input,
            line,
            line_start,
            colon_idx,
            pretrim_start,
            pretrim_end,
            self,
            scan,
            flags,
            scan_info,
        )
    }

    fn set_header<V>(&mut self, _slot: u8, _value: &V) -> Result<()>
    where
        V: HeaderValue,
    {
        Ok(())
    }

    fn set_query_name<V>(&mut self, name: &[u8], value: &V) -> Result<()>
    where
        V: HeaderValue;

    fn set_query_slice(&mut self, name: &[u8], input: &[u8], range: Range<usize>) -> Result<()> {
        let value = SliceValue::new(input, range);
        self.set_query_name(name, &value)
    }

    fn parse_query(&mut self, input: &[u8], range: Range<usize>) -> Result<()> {
        crate::parser::head::query::parse_query_fields_input::<K, Self>(input, range, self)
    }
}

impl<K: Copy> HeadParts<K> for () {
    const NEED_FIELDS: bool = true;
    const NEED_HEADER: bool = false;
    const NEED_KNOWN_HEADER: bool = false;
    const NEED_QUERY: bool = false;

    fn new(_route: K) -> Self {}

    fn set_header_name<V>(&mut self, _name: &[u8], _value: &V) -> Result<()>
    where
        V: HeaderValue,
    {
        Ok(())
    }

    fn set_query_name<V>(&mut self, _name: &[u8], _value: &V) -> Result<()>
    where
        V: HeaderValue,
    {
        Ok(())
    }
}
