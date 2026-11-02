use sark_http::request::{self, span};

use crate::request::frame::{self, proof};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Peer(span::Span);

impl frame::Seal for Peer {}

impl proof::Proof for Peer {
    fn try_from(layout: request::Layout) -> Result<Self, request::Error> {
        match layout.authority() {
            Some(peer) => Ok(Self(peer)),
            None => Err(request::Error::MissingAuthority),
        }
    }

    fn span(self) -> Option<span::Span> {
        Some(self.0)
    }
}

impl Peer {
    pub(in crate::request) const fn get(self) -> span::Span {
        self.0
    }
}
