use std::ops::Range;

use o3::buffer::{Bytes, Retained};
use sark_core::{
    error::{Error, Result},
    utils::bytes::Ascii,
};

pub trait HeaderValue {
    fn eq_bytes(&self, expected: &[u8]) -> bool;

    fn eq_ignore_ascii_case(&self, expected: &[u8]) -> bool;

    fn as_range(&self) -> Range<usize>;

    fn copy_frame(&self) -> Bytes<Retained>;

    fn parse_usize(&self) -> Result<usize>;

    fn parse_u64(&self) -> Result<u64>;
}

impl HeaderValue for Bytes<Retained> {
    fn eq_bytes(&self, expected: &[u8]) -> bool {
        self.as_slice() == expected
    }

    fn eq_ignore_ascii_case(&self, expected: &[u8]) -> bool {
        self.as_slice().eq_ignore_ascii_case(expected)
    }

    fn as_range(&self) -> Range<usize> {
        0..self.len()
    }

    fn copy_frame(&self) -> Bytes<Retained> {
        self.clone()
    }

    fn parse_usize(&self) -> Result<usize> {
        Ascii::parse_usize(self.as_slice()).ok_or_else(Error::invalid_integer_header)
    }

    fn parse_u64(&self) -> Result<u64> {
        Ascii::parse_u64(self.as_slice()).ok_or_else(Error::invalid_integer_header)
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
    fn eq_bytes(&self, expected: &[u8]) -> bool {
        self.bytes() == expected
    }

    fn eq_ignore_ascii_case(&self, expected: &[u8]) -> bool {
        self.bytes().eq_ignore_ascii_case(expected)
    }

    fn as_range(&self) -> Range<usize> {
        self.start..self.end
    }

    fn copy_frame(&self) -> Bytes<Retained> {
        Bytes::<Retained>::copy_from_slice(self.bytes())
    }

    fn parse_usize(&self) -> Result<usize> {
        Ascii::parse_usize(self.bytes()).ok_or_else(Error::invalid_integer_header)
    }

    fn parse_u64(&self) -> Result<u64> {
        Ascii::parse_u64(self.bytes()).ok_or_else(Error::invalid_integer_header)
    }
}

pub trait FieldValue: Sized {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self>;

    fn parse_path<P: PathProbe>(_path: &P, _start: usize, _end: usize) -> Option<Self> {
        None
    }
}

impl FieldValue for Range<usize> {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self> {
        Ok(value.as_range())
    }

    fn parse_path<P: PathProbe>(_path: &P, start: usize, end: usize) -> Option<Self> {
        Some(start..end)
    }
}

impl FieldValue for Bytes<Retained> {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self> {
        Ok(value.copy_frame())
    }

    fn parse_path<P: PathProbe>(path: &P, start: usize, end: usize) -> Option<Self> {
        path.copy_range_frame(start, end)
    }
}

impl FieldValue for usize {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self> {
        value.parse_usize()
    }

    fn parse_path<P: PathProbe>(path: &P, start: usize, end: usize) -> Option<Self> {
        path.parse_range_usize(start, end)
    }
}

impl FieldValue for u64 {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self> {
        value.parse_u64()
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
        Err(Error::BadRequest("Invalid boolean field".into()))
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
    fn is_end(&self, idx: usize) -> bool;

    fn eq_bytes(&self, expected: &[u8]) -> bool;

    fn eq_range(&self, start: usize, end: usize, expected: &[u8]) -> bool;

    fn eq_range_ignore_ascii_case(&self, start: usize, end: usize, expected: &[u8]) -> bool;

    fn parse_range_usize(&self, start: usize, end: usize) -> Option<usize>;

    fn parse_range_u64(&self, start: usize, end: usize) -> Option<u64>;

    fn copy_range_frame(&self, start: usize, end: usize) -> Option<Bytes<Retained>>;

    fn next_seg(&self, idx: usize) -> Option<(usize, usize, usize)>;

    fn probe_literal(&self, cur: usize, lit: &[u8]) -> Option<usize> {
        let (start, end, nx) = self.next_seg(cur)?;
        if self.eq_range(start, end, lit) {
            Some(nx)
        } else {
            None
        }
    }
}

pub struct TargetPath<'a> {
    raw: &'a [u8],
}

impl<'a> TargetPath<'a> {
    pub const fn new(raw: &'a [u8]) -> Self {
        Self { raw }
    }
}

impl PathProbe for TargetPath<'_> {
    fn is_end(&self, idx: usize) -> bool {
        idx >= self.raw.len() || self.raw[idx] == b'?'
    }

    fn eq_bytes(&self, expected: &[u8]) -> bool {
        self.raw.starts_with(expected) && self.is_end(expected.len())
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

    fn copy_range_frame(&self, start: usize, end: usize) -> Option<Bytes<Retained>> {
        if end < start || end > self.raw.len() {
            return None;
        }
        Some(Bytes::<Retained>::copy_from_slice(&self.raw[start..end]))
    }

    fn next_seg(&self, idx: usize) -> Option<(usize, usize, usize)> {
        if self.is_end(idx) || self.raw[idx] != b'/' {
            return None;
        }
        let start = idx + 1;
        let mut end = start;
        while end < self.raw.len() && self.raw[end] != b'/' && self.raw[end] != b'?' {
            end += 1;
        }
        Some((start, end, end))
    }

    fn probe_literal(&self, cur: usize, lit: &[u8]) -> Option<usize> {
        let start = cur.checked_add(1)?;
        let end = start.checked_add(lit.len())?;
        if end > self.raw.len() || self.raw[start..end] != *lit {
            return None;
        }
        if end < self.raw.len() && self.raw[end] != b'/' && self.raw[end] != b'?' {
            return None;
        }
        Some(end)
    }
}
