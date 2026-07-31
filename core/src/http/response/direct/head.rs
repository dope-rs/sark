use std::slice;

use o3::buffer::{ByteSink, Owned, Shared};

use super::headers::{DEFAULT_HEADER_CAPACITY, Headers};
use crate::http::Field;
use crate::http::response::wire_emit::{HeaderSection, WireWriter};

#[derive(Clone, Debug)]
pub struct HeadInner<'req, const N: usize = DEFAULT_HEADER_CAPACITY, S = StaticHeaderBytes> {
    static_headers: S,
    headers: Headers<'req, N>,
}

#[derive(Clone, Copy, Debug)]
pub struct StaticHeaderBytes(&'static [u8]);

impl StaticHeaderBytes {
    pub const fn new(wire: &'static [u8]) -> Self {
        Self(wire)
    }
}

#[derive(Debug)]
pub struct HeaderTemplate {
    wire: &'static [u8],
    fields: &'static [Field<'static>],
}

impl HeaderTemplate {
    pub const fn new(wire: &'static [u8], fields: &'static [Field<'static>]) -> Self {
        Self { wire, fields }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StaticHeaderFields(&'static HeaderTemplate);

impl StaticHeaderFields {
    pub const fn new(template: &'static HeaderTemplate) -> Self {
        Self(template)
    }
}

#[doc(hidden)]
pub trait StaticHeaders {
    type Fields<'a>: Iterator<Item = Field<'a>>
    where
        Self: 'a;

    fn wire(&self) -> &'static [u8];

    fn fields(&self) -> Self::Fields<'_>;
}

pub struct WireHeaderFields<'a> {
    remaining: &'a [u8],
}

impl<'a> WireHeaderFields<'a> {
    pub fn new(wire: &'a [u8]) -> Self {
        Self { remaining: wire }
    }
}

impl<'a> Iterator for WireHeaderFields<'a> {
    type Item = Field<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (line, remaining) = match self.remaining.iter().position(|&b| b == b'\n') {
                Some(end) => (&self.remaining[..end], &self.remaining[end + 1..]),
                None => (self.remaining, &[][..]),
            };
            self.remaining = remaining;
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.is_empty() {
                if self.remaining.is_empty() {
                    return None;
                }
                continue;
            }
            let Some(colon) = line.iter().position(|&b| b == b':') else {
                if self.remaining.is_empty() {
                    return None;
                }
                continue;
            };
            let name = &line[..colon];
            let mut value = &line[colon + 1..];
            while let Some((&b' ', rest)) = value.split_first() {
                value = rest;
            }
            return Some(Field::new(name, value));
        }
    }
}

pub struct TemplateHeaderFields<'a> {
    fields: slice::Iter<'a, Field<'static>>,
}

impl<'a> Iterator for TemplateHeaderFields<'a> {
    type Item = Field<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fields
            .next()
            .map(|field| Field::new(field.name, field.value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.fields.size_hint()
    }
}

impl ExactSizeIterator for TemplateHeaderFields<'_> {}

impl StaticHeaders for StaticHeaderBytes {
    type Fields<'a> = WireHeaderFields<'a>;

    fn wire(&self) -> &'static [u8] {
        self.0
    }

    fn fields(&self) -> Self::Fields<'_> {
        WireHeaderFields::new(self.0)
    }
}

impl StaticHeaders for StaticHeaderFields {
    type Fields<'a> = TemplateHeaderFields<'a>;

    fn wire(&self) -> &'static [u8] {
        self.0.wire
    }

    fn fields(&self) -> Self::Fields<'_> {
        TemplateHeaderFields {
            fields: self.0.fields.iter(),
        }
    }
}

impl<'req, const N: usize> HeadInner<'req, N> {
    pub fn new(static_headers: &'static [u8], headers: Headers<'req, N>) -> Self {
        Self {
            static_headers: StaticHeaderBytes::new(static_headers),
            headers,
        }
    }
}

impl<'req, const N: usize> HeadInner<'req, N, StaticHeaderFields> {
    pub fn structured(static_headers: &'static HeaderTemplate, headers: Headers<'req, N>) -> Self {
        Self {
            static_headers: StaticHeaderFields::new(static_headers),
            headers,
        }
    }
}

impl<'req, const N: usize, S> HeadInner<'req, N, S>
where
    S: StaticHeaders,
{
    pub fn static_headers(&self) -> &'static [u8] {
        self.static_headers.wire()
    }

    pub fn headers(&self) -> &Headers<'req, N> {
        &self.headers
    }

    pub(super) fn headers_mut(&mut self) -> &mut Headers<'req, N> {
        &mut self.headers
    }

    pub fn wire_len(&self) -> usize {
        self.static_headers.wire().len() + self.headers.wire_len()
    }

    pub(crate) fn wire_headers(&self) -> Shared {
        if self.headers.is_empty() {
            return Shared::from_static(self.static_headers.wire());
        }
        let capacity = self.wire_len();
        match Owned::try_build_exact(capacity, |out| self.write_into(out)) {
            Ok(out) => out.freeze(),
            Err(_) => {
                let mut out = Vec::with_capacity(capacity);
                match self.write_into(&mut out) {
                    Ok(()) => Shared::from(out),
                    Err(error) => match error {},
                }
            }
        }
    }

    fn write_into<W: ByteSink>(&self, out: &mut W) -> Result<(), W::Error> {
        out.write_slice(self.static_headers.wire())?;
        self.headers.write_into(out)
    }

    pub(crate) fn fields(&self) -> impl Iterator<Item = Field<'_>> + '_ {
        self.static_headers.fields().chain(self.headers.fields())
    }

    pub fn write_slice(&self, out: &mut [u8]) -> Option<usize> {
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        let static_headers = self.static_headers.wire();
        let n = static_headers.len();
        out[..n].copy_from_slice(static_headers);
        let m = self.headers.write(&mut out[n..]);
        Some(n + m)
    }
}

impl<const N: usize, S> HeaderSection for HeadInner<'_, N, S>
where
    S: StaticHeaders,
{
    fn header_len(&self) -> usize {
        Self::wire_len(self)
    }
    fn write_headers(&self, out: &mut WireWriter<'_>) {
        out.put(self.static_headers.wire());
        self.headers.write_wire(out);
    }
}
