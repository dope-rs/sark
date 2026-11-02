//! Trusted request projection bindings.

use std::{mem, ops};

use o3::{buffer, buffer::resident, mem::rc};
use request::{Projection, field, frame::proof, path};
use sark_http::request::{Bytes, span};

use crate::{request, route};

pub struct Path<'a, R> {
    req: &'a R,
    cap: route::raw::PathCapture,
}

impl<'a, R> Path<'a, R> {
    /// Binds a generated capture to its exact request.
    ///
    /// # Safety
    /// `cap` must have been matched from `req`.
    pub const unsafe fn new(req: &'a R, cap: route::raw::PathCapture) -> Self {
        Self { req, cap }
    }
}

impl<'req, R: route::Schema>
    Path<'_, request::raw::Parsed<request::Head<&'req [u8], R::Frame>, R>>
{
    pub fn get(self) -> &'req [u8] {
        let path = self.req.owner().path();
        let range = self.cap.start()..self.cap.end();
        unsafe { path.get_unchecked(range) }
    }

    pub fn try_parse<T: field::Path>(self) -> Result<T, field::Error> {
        let start = self.cap.start();
        T::parse(path::Capture::new(self.get(), start))
    }
}

impl<'d, R: route::Schema> Path<'_, request::raw::Parsed<request::Parts<'d, R::Frame>, R>> {
    /// Retains a capture within this route, whose end fits the immutable owner.
    pub fn try_get(self) -> Result<Bytes<'d>, rc::Error> {
        let range = self.cap.start()..self.cap.end();
        Ok(match self.req.owner().head.route() {
            Projection::Frame(path) => {
                let start = path.start + range.start;
                let end = path.start + range.end;
                let owner = self.req.owner().head.owner();
                let frame = unsafe { buffer::raw::Segment::try_project(owner, start..end) }?;
                Bytes::from_shared(self.req.owner().id, frame)
            }
            Projection::Root => self.static_path(b"/", range),
            Projection::Star => self.static_path(b"*", range),
        })
    }

    pub fn try_parse<T: field::Path>(self) -> Result<T, field::Error> {
        let path = self.req.owner().head.borrow().path();
        let range = self.cap.start()..self.cap.end();
        // SAFETY: this capture was matched from this exact retained request.
        let value = unsafe { path.get_unchecked(range) };
        T::parse(path::Capture::new(value, self.cap.start()))
    }

    fn static_path(self, path: &'d [u8], range: ops::Range<usize>) -> Bytes<'d> {
        let frame = unsafe { path.get_unchecked(range) };
        Bytes::new(self.req.owner().id, resident::Shared::from(frame))
    }
}

pub struct Bound<'a, R> {
    req: &'a R,
    span: span::Span,
}

impl<'a, R> Bound<'a, R> {
    /// Binds a parsed frame to its exact request head.
    ///
    /// # Safety
    /// `span` must have been parsed from `req`.
    pub const unsafe fn new(req: &'a R, span: span::Span) -> Self {
        Self { req, span }
    }
}

impl<'req, R: route::Schema>
    Bound<'_, request::raw::Parsed<request::Head<&'req [u8], R::Frame>, R>>
{
    pub fn get(self) -> &'req [u8] {
        let range = self.span.start()..self.span.end();
        let owner = *self.req.owner().owner();
        unsafe { owner.get_unchecked(range) }
    }
}

impl<'a, 'req, R: route::Schema<Frame = proof::Peer>>
    Bound<'a, request::raw::Parsed<request::Head<&'req [u8], proof::Peer>, R>>
{
    pub fn required(
        req: &'a request::raw::Parsed<request::Head<&'req [u8], proof::Peer>, R>,
    ) -> Self {
        Self {
            req,
            span: req.owner().peer(),
        }
    }
}

impl<'d, R: route::Schema> Bound<'_, request::raw::Parsed<request::Parts<'d, R::Frame>, R>> {
    pub fn try_get(self) -> Result<Bytes<'d>, rc::Error> {
        let range = self.span.start()..self.span.end();
        let owner = self.req.owner().head.owner();
        let frame = unsafe { buffer::raw::Segment::try_project(owner, range) }?;
        Ok(Bytes::from_shared(self.req.owner().id, frame))
    }
}

impl<'a, 'd, R: route::Schema<Frame = proof::Peer>>
    Bound<'a, request::raw::Parsed<request::Parts<'d, proof::Peer>, R>>
{
    pub fn required(req: &'a request::raw::Parsed<request::Parts<'d, proof::Peer>, R>) -> Self {
        Self {
            req,
            span: req.owner().head.peer(),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Path<'_, ()>>() == 3 * mem::size_of::<usize>());
    assert!(mem::size_of::<Bound<'_, ()>>() == 2 * mem::size_of::<usize>());
};
