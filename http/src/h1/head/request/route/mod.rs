use std::{num::NonZero, str};

use o3::{buffer, buffer::write};
use write::{CopyError, SourceExt as _};

use crate::{
    h1::{field::expect, head::request::pool, switch},
    request::{self, span},
};

mod line;

pub(in crate::h1::head::request) use line::Line;

/// Admitted request routing ranges and their immutable source.
#[derive(PartialEq, Eq)]
pub struct Route<S> {
    pub(in crate::h1::head::request) source: S,
    pub(in crate::h1::head::request) line: Line,
    pub(in crate::h1::head::request) end: NonZero<u32>,
    pub(in crate::h1::head::request) offers: switch::Offers,
}

impl Copy for Route<&[u8]> {}

impl Clone for Route<&[u8]> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Route<S> {
    pub(in crate::h1::head::request) const fn new(
        source: S,
        line: Line,
        end: NonZero<u32>,
        offers: switch::Offers,
    ) -> Self {
        Self {
            source,
            line,
            end,
            offers,
        }
    }

    pub fn into_source(self) -> S {
        self.source
    }

    pub fn transition(&self) -> switch::Intent {
        self.line.transition()
    }

    pub fn take_expect(&mut self) -> expect::Decision {
        self.line.take_expect()
    }

    pub const fn head_end(&self) -> NonZero<u32> {
        self.end
    }
}

impl<'d> Route<pool::Buf<'d>> {
    pub fn seal(self) -> pool::Meta<'d> {
        let route = self.as_ref();
        let target = &route.line.target;
        let layout = pool::Layout::new(
            [
                route.method().len() as u16,
                target.start() as u16,
                target.end() as u16,
                self.end.get() as u16,
            ],
            pool::Source::Head,
            route.transition(),
        );
        let mut bytes = self.source.bytes;
        unsafe {
            bytes
                .as_mut_slice()
                .get_unchecked_mut(..pool::Layout::BYTES)
                .copy_from_slice(&layout.encode());
        }
        pool::Meta { bytes }
    }
}

impl<S: buffer::raw::Source> Route<S> {
    pub fn as_ref(&self) -> Route<&[u8]> {
        Route {
            source: self.source.as_ref(),
            line: self.line,
            end: self.end,
            offers: self.offers,
        }
    }

    pub fn try_copy<D>(&self, target: D) -> Result<Route<D>, (D, CopyError<D::Error>)>
    where
        D: buffer::raw::Source + write::Sink,
    {
        let target = target.try_copy(self.source.as_ref())?;
        Ok(Route {
            source: target,
            line: self.line,
            end: self.end,
            offers: self.offers,
        })
    }
}

impl<S: write::Sink> Route<S> {
    pub fn try_extend(&mut self, bytes: &[u8]) -> Result<(), S::Error> {
        self.source.write_slice(bytes)
    }
}

impl<'a> Route<&'a [u8]> {
    pub const fn bytes(self) -> &'a [u8] {
        self.source
    }

    pub fn body(self) -> &'a [u8] {
        unsafe { self.source.get_unchecked(self.end.get() as usize..) }
    }

    pub fn method(self) -> &'a str {
        self.text(self.line.method())
    }

    pub fn target(self) -> &'a str {
        self.text(span::Span::from_target(self.line.target))
    }

    pub fn authority(self) -> &'a str {
        self.text(self.line.auth)
    }

    pub fn route_path(self) -> Option<&'a str> {
        match self.line.target.form() {
            request::Form::Root => Some("/"),
            request::Form::Asterisk | request::Form::Authority => Some("*"),
            request::Form::Path | request::Form::Extended => {
                self.line.route.map(|path| self.text(path))
            }
        }
    }

    fn text(self, span: span::Span) -> &'a str {
        unsafe { str::from_utf8_unchecked(self.source.get_unchecked(span.start()..span.end())) }
    }
}

const _: () = assert!(size_of::<Route<&[u8]>>() == 7 * size_of::<usize>());
