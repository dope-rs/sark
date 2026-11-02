use std::mem;

use crate::{
    h1::{field::expect, switch},
    request,
    request::span,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::h1::head::request) struct Line {
    pub(in crate::h1::head::request) target: request::Target,
    /// The request-line method length; the method range is the block prefix.
    method: u32,
    pub(in crate::h1::head::request) auth: span::Span,
    pub(super) route: Option<span::Span>,
    intent: switch::Intent,
    expect: expect::Decision,
}

impl Line {
    pub(in crate::h1::head::request) const fn new(
        layout: request::Layout,
        method: u32,
        auth: span::Span,
        intent: switch::Intent,
        expect: expect::Decision,
    ) -> Self {
        Self {
            target: layout.target(),
            method,
            auth,
            route: layout.route(),
            intent,
            expect,
        }
    }

    pub(in crate::h1::head::request) const fn layout(self) -> request::Layout {
        let auth = match self.target.form() {
            request::Form::Authority => None,
            request::Form::Path
            | request::Form::Root
            | request::Form::Asterisk
            | request::Form::Extended => Some(self.auth),
        };
        request::Layout::new(self.target, auth, self.route)
    }

    /// The method range, measured once by the request-line parse.
    pub(super) const fn method(self) -> span::Span {
        span::Span::from_start(self.method)
    }

    pub(in crate::h1::head::request) const fn transition(self) -> switch::Intent {
        self.intent
    }

    pub(super) fn take_expect(&mut self) -> expect::Decision {
        mem::replace(&mut self.expect, expect::Decision::None)
    }
}

const _: () = assert!(size_of::<Line>() == size_of::<[u32; 8]>());
