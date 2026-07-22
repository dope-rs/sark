use http::Method;
use sark_core::http::codec::ParsedRequestHead;

pub struct Ctx<'a> {
    pub method: &'a Method,
    pub head_bytes: &'a [u8],
    pub head: &'a ParsedRequestHead<'a>,
    pub date: &'a [u8; 29],
}

pub struct Capture {
    reason: &'static [u8],
}

impl Capture {
    pub fn new() -> Self {
        Self { reason: b"" }
    }

    pub fn reason(&self) -> &'static [u8] {
        self.reason
    }

    pub fn close(&mut self, reason: &'static [u8]) {
        self.reason = reason;
    }
}

impl Default for Capture {
    fn default() -> Self {
        Self::new()
    }
}

pub trait Middleware {
    type State: 'static;

    fn before(ctx: &mut Ctx<'_>, st: &Self::State, capture: &mut Capture) -> bool;
}
