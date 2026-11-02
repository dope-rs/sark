use std::ops::ControlFlow;

use o3::{
    buffer::{self, Complete as _, bytes::window::Snapshot, storage, write},
    mem::Rejected,
};

use crate::{
    h1::{
        head::line::{Request, Version, error},
        target,
    },
    method,
    request::span,
};

#[derive(Clone, Copy)]
pub(super) struct Parts {
    pub(super) target: span::Span,
    pub(super) plan: target::Target,
    pub(super) version: Version,
    pub(super) matched: Option<method::Known>,
}

/// An append-only input retaining the exact request line already admitted.
pub struct Owned<const MAX: usize> {
    pub(super) source: Snapshot<MAX>,
    pub(super) parts: Parts,
}

impl<const MAX: usize> Owned<MAX> {
    /// Parses once and binds the admitted line to this unchanged input.
    pub fn try_parse(
        source: Snapshot<MAX>,
        methods: u8,
    ) -> Result<ControlFlow<Snapshot<MAX>, Self>, Rejected<Snapshot<MAX>, error::Request>> {
        let parts = match Request::parse_with(source.as_ref(), methods) {
            Ok(Some(request)) => request.parts,
            Ok(None) => return Ok(ControlFlow::Break(source)),
            Err(error) => return Err(Rejected::new(source, error)),
        };
        Ok(ControlFlow::Continue(Self { source, parts }))
    }

    /// Borrows the admitted line from its exact retained input.
    pub fn request(&self) -> Request<'_> {
        Request {
            source: self.source.as_ref(),
            parts: self.parts,
        }
    }

    pub(in crate::h1::head) fn into_source(self) -> Snapshot<MAX> {
        self.source
    }

    /// Transfers the same backing while binding line views to its immutable loan.
    pub fn with_shared<T>(
        self,
        emit: impl for<'a> FnOnce(buffer::View<'a, storage::Shared>, Request<'a>) -> T,
    ) -> T {
        let source = self.source.freeze();
        let request = Request {
            source: source.as_ref(),
            parts: self.parts,
        };
        emit(source.view(), request)
    }
}

impl<const MAX: usize> AsRef<[u8]> for Owned<MAX> {
    fn as_ref(&self) -> &[u8] {
        self.source.as_ref()
    }
}

impl<const MAX: usize> write::Sink for Owned<MAX> {
    type Error = storage::AllocError;

    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.source.write_slice(bytes)
    }

    fn write_slices<const N: usize>(&mut self, slices: [&[u8]; N]) -> Result<(), Self::Error> {
        self.source.write_slices(slices)
    }
}

// SAFETY: the private Snapshot retains the exact initialized source; shared access
// preserves its prefix, and every write::Sink operation only appends.
unsafe impl<const MAX: usize> buffer::raw::Source for Owned<MAX> {}
