//! Parsed request field contracts.

use std::{mem, ops};

use o3::{
    alloc,
    buffer::{storage, write::consume},
};
use sark_http::{
    field::text::{Ascii, IntError},
    h1::field::Entry,
    request::span,
};

use crate::request::path;

/// Receives one generated field-name prefix match.
pub trait Prefix<S: Copy>: Sized {
    type Output;

    fn rest(&self) -> &[u8];

    fn user(self, slot: S, colon: usize) -> Self::Output;

    fn miss(self, start: usize) -> Self::Output;

    fn short(self) -> Self::Output;
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Capacity(consume::Error),
    Exhausted(alloc::Exhausted),
    Path(path::Error),
    Span(span::Error),
    Integer {
        start: usize,
        end: usize,
        cause: IntError,
    },
    Boolean {
        start: usize,
        end: usize,
    },
    Order {
        start: usize,
        end: usize,
        capacity: usize,
    },
    Bounds {
        start: usize,
        end: usize,
        capacity: usize,
    },
}

impl From<storage::AllocError> for Error {
    fn from(error: storage::AllocError) -> Self {
        match error {
            storage::AllocError::Capacity(error) => Self::Capacity(error),
            storage::AllocError::Exhausted(error) => Self::Exhausted(error),
        }
    }
}

pub trait HeaderValue: AsRef<[u8]> {
    fn try_span(&self) -> Result<span::Span, Error>;
}

impl HeaderValue for storage::Shared {
    fn try_span(&self) -> Result<span::Span, Error> {
        span::Span::checked(0, self.len()).map_err(Error::Span)
    }
}

pub struct SliceValue<'a> {
    value: &'a [u8],
    span: span::Span,
}

impl<'a> From<Entry<'a>> for SliceValue<'a> {
    fn from(entry: Entry<'a>) -> Self {
        Self {
            value: entry.value(),
            span: entry.value_span(),
        }
    }
}

impl<'a> SliceValue<'a> {
    pub fn try_slice(value: &'a [u8], start: usize) -> Result<Self, Error> {
        let span = span::Span::checked(start, value.len()).map_err(Error::Span)?;
        Ok(Self { value, span })
    }

    pub fn try_new(raw: &'a [u8], range: ops::Range<usize>) -> Result<Self, Error> {
        let start = range.start;
        let end = range.end;
        let capacity = raw.len();
        if start > end {
            return Err(Error::Order {
                start,
                end,
                capacity,
            });
        }
        let value = raw.get(start..end).ok_or(Error::Bounds {
            start,
            end,
            capacity,
        })?;
        let span = span::Span::from_range(start..end).map_err(Error::Span)?;
        Ok(Self { value, span })
    }

    pub fn try_at(raw: &'a [u8], span: span::Span) -> Result<Self, Error> {
        let start = span.start();
        let end = span.end();
        let value = raw.get(start..end).ok_or(Error::Bounds {
            start,
            end,
            capacity: raw.len(),
        })?;
        Ok(Self { value, span })
    }
}

impl HeaderValue for SliceValue<'_> {
    fn try_span(&self) -> Result<span::Span, Error> {
        Ok(self.span)
    }
}

impl AsRef<[u8]> for SliceValue<'_> {
    fn as_ref(&self) -> &[u8] {
        self.value
    }
}

pub trait Value: Sized {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self, Error>;
}

pub trait Path: Sized {
    fn try_parse(path: &path::Target<'_>, range: ops::Range<usize>) -> Result<Self, Error> {
        Self::parse(path.try_capture(range).map_err(Error::Path)?)
    }

    fn parse(capture: path::Capture<'_>) -> Result<Self, Error>;
}

impl Value for storage::Shared {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self, Error> {
        storage::Shared::try_copy(value.as_ref()).map_err(Error::from)
    }
}

impl Value for usize {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self, Error> {
        let span = value.try_span()?;
        integer(
            Ascii::new(value.as_ref()).parse_usize(),
            span.start()..span.end(),
        )
    }
}

impl Path for usize {
    fn parse(capture: path::Capture<'_>) -> Result<Self, Error> {
        integer(
            Ascii::new(capture.as_bytes()).parse_usize(),
            capture.range(),
        )
    }
}

impl Value for u64 {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self, Error> {
        let span = value.try_span()?;
        integer(
            Ascii::new(value.as_ref()).parse_u64(),
            span.start()..span.end(),
        )
    }
}

impl Path for u64 {
    fn parse(capture: path::Capture<'_>) -> Result<Self, Error> {
        integer(Ascii::new(capture.as_bytes()).parse_u64(), capture.range())
    }
}

impl Value for bool {
    fn parse_value<V: HeaderValue>(value: &V) -> Result<Self, Error> {
        let span = value.try_span()?;
        boolean(value.as_ref(), span.start()..span.end())
    }
}

impl Path for bool {
    fn parse(capture: path::Capture<'_>) -> Result<Self, Error> {
        boolean(capture.as_bytes(), capture.range())
    }
}

fn integer<T>(value: Result<T, IntError>, range: ops::Range<usize>) -> Result<T, Error> {
    value.map_err(|cause| Error::Integer {
        start: range.start,
        end: range.end,
        cause,
    })
}

fn boolean(value: &[u8], range: ops::Range<usize>) -> Result<bool, Error> {
    if value.eq_ignore_ascii_case(b"true") || value == b"1" {
        return Ok(true);
    }
    if value.eq_ignore_ascii_case(b"false") || value == b"0" {
        return Ok(false);
    }
    Err(Error::Boolean {
        start: range.start,
        end: range.end,
    })
}

const _: () = {
    assert!(mem::size_of::<SliceValue<'_>>() == mem::size_of::<usize>() * 3);
    assert!(mem::size_of::<Option<SliceValue<'_>>>() == mem::size_of::<SliceValue<'_>>());
    assert!(mem::size_of::<Error>() == mem::size_of::<usize>() * 5);
    assert!(mem::size_of::<Option<Error>>() == mem::size_of::<Error>());
    assert!(mem::size_of::<Result<(), Error>>() == mem::size_of::<Error>());
    assert!(!mem::needs_drop::<SliceValue<'_>>());
    assert!(!mem::needs_drop::<Error>());
};
