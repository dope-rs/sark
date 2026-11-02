use sark_http::request::{self, span};

use crate::request::frame::{self, proof};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Maybe(Option<span::Span>);

impl frame::Seal for Maybe {}

impl proof::Proof for Maybe {
    fn try_from(layout: request::Layout) -> Result<Self, request::Error> {
        Ok(Self(layout.authority()))
    }

    fn span(self) -> Option<span::Span> {
        self.0
    }
}
