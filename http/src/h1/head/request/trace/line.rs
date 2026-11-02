use std::num::NonZero;

use crate::request::span::Span;

pub(in crate::h1::head::request) struct Line {
    pub(super) start: NonZero<u32>,
    pub(super) end: u32,
}

impl Line {
    pub(in crate::h1::head::request) fn from_span(span: Span) -> Self {
        Self {
            start: unsafe { NonZero::new_unchecked(span.start() as u32 + 1) },
            end: span.end() as u32,
        }
    }
}
