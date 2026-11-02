use core::{mem, ops};

use sark_http::{field::scan::Match, request::span};

use crate::{request::field, route::Schema};

pub use crate::request::path::Error;

/// A query range admitted once against its immutable request storage.
pub struct Entries<'a> {
    rest: &'a [u8],
    start: usize,
}

impl<'a> Entries<'a> {
    pub fn try_new(raw: &'a [u8], range: ops::Range<usize>) -> Result<Self, Error> {
        let start = range.start;
        let rest = crate::request::path::Target::new(raw).get(range)?;
        Ok(Self { rest, start })
    }
}

/// Exact name and value slices retaining their original request offsets.
pub struct Entry<'a> {
    name: &'a [u8],
    value: &'a [u8],
    start: usize,
    offset: usize,
}

impl<'a> Entry<'a> {
    pub const fn name(&self) -> &'a [u8] {
        self.name
    }

    pub const fn name_range(&self) -> ops::Range<usize> {
        self.start..self.start + self.name.len()
    }

    pub fn try_span(&self) -> Result<span::Span, span::Error> {
        span::Span::checked(self.offset, self.value.len())
    }

    pub fn try_value(&self) -> Result<field::SliceValue<'a>, field::Error> {
        field::SliceValue::try_slice(self.value, self.offset)
    }
}

impl<'a> Iterator for Entries<'a> {
    type Item = Entry<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while !self.rest.is_empty() {
            let start = self.start;
            let (name, value, rest, offset) = match Match::find2(b'=', b'&', self.rest) {
                Some(found) if found.byte() == b'=' => {
                    let tail = found.suffix();
                    let (value, rest) = Match::find(b'&', tail).map_or_else(
                        || tail.split_at(tail.len()),
                        |end| (end.prefix(), end.suffix()),
                    );
                    (found.prefix(), value, rest, start + found.end().get())
                }
                Some(found) => {
                    let name = found.prefix();
                    let (_, value) = name.split_at(name.len());
                    (name, value, found.suffix(), start + name.len())
                }
                None => {
                    let (name, value) = self.rest.split_at(self.rest.len());
                    (name, value, value, start + name.len())
                }
            };
            self.start += self.rest.len() - rest.len();
            self.rest = rest;
            if start == offset {
                continue;
            }
            return Some(Entry {
                name,
                value,
                start,
                offset,
            });
        }
        None
    }
}

pub struct Input<'a, R: Schema> {
    headers: &'a mut R::RawHeaders,
    bytes: &'a [u8],
    range: ops::Range<usize>,
}

impl<'a, R: Schema> Input<'a, R> {
    pub fn new(headers: &'a mut R::RawHeaders, bytes: &'a [u8], range: ops::Range<usize>) -> Self {
        Self {
            headers,
            bytes,
            range,
        }
    }

    pub fn into_parts(self) -> (&'a mut R::RawHeaders, &'a [u8], ops::Range<usize>) {
        (self.headers, self.bytes, self.range)
    }
}

pub trait Fields<R: Schema> {
    fn parse(input: Input<'_, R>) -> Result<(), R::Error>;
}

#[derive(Clone, Copy)]
pub enum Discard {}

impl<R: Schema<Query = Self>> Fields<R> for Discard {
    fn parse(_: Input<'_, R>) -> Result<(), R::Error> {
        Ok(())
    }
}

const _: () = assert!(mem::size_of::<Discard>() == 0);
