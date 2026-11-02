use std::num::NonZero;

use crate::{h1::framing, payload::response::status::Code, request::span};

/// Complete owner-free HTTP/1 response head.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Admitted {
    pub(super) coding: super::Coding,
    pub(super) fields: span::Span,
    pub(super) body: framing::ResponseBody,
    pub(super) control: framing::Control,
    pub(super) wire: NonZero<u32>,
    pub(super) status: Code,
    pub(super) version: u8,
}

impl Admitted {
    pub const fn coding(self) -> super::Coding {
        self.coding
    }

    pub const fn fields(self) -> span::Span {
        self.fields
    }

    pub const fn body(self) -> framing::ResponseBody {
        self.body
    }

    pub const fn control(self) -> framing::Control {
        self.control
    }

    pub const fn wire_len(self) -> NonZero<u32> {
        self.wire
    }

    pub const fn status(self) -> Code {
        self.status
    }

    pub const fn version(self) -> u8 {
        self.version
    }
}

const _: () = assert!(core::mem::size_of::<Admitted>() <= 8 * core::mem::size_of::<usize>());
