use std::str;

use crate::common::client::{pool, runtime::peer::Error};

/// Borrowed view of one fully framed HTTP/1 request.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Request<'a> {
    wire: &'a [u8],
    head: &'a str,
    method_end: u32,
    target_end: u32,
}

impl<'a> Request<'a> {
    pub(super) fn parse<E>(
        wire: &'a [u8],
        head_end: usize,
        method: u32,
        target: u32,
    ) -> Result<Self, Error<E>> {
        let head = wire.get(..head_end).ok_or(Error::Boundary {
            wire: wire.len(),
            head: head_end,
        })?;
        let text = str::from_utf8(head).map_err(Error::Utf8)?;
        Ok(Self {
            wire,
            head: text,
            method_end: method,
            target_end: target,
        })
    }

    /// Request method as received on the wire.
    #[must_use]
    pub fn method(self) -> &'a str {
        &self.head[..self.method_end as usize]
    }

    /// Request target, including any query component.
    #[must_use]
    pub fn target(self) -> &'a str {
        &self.head[self.method_end as usize + 1..self.target_end as usize]
    }

    /// Fully framed request body.
    #[must_use]
    pub fn body(self) -> &'a [u8] {
        &self.wire[self.head.len()..]
    }

    /// Exact framed request bytes.
    #[must_use]
    pub const fn wire(self) -> &'a [u8] {
        self.wire
    }

    pub fn is_effect(&self) -> bool {
        self.method() == "POST" && self.target() == "/effect" && self.body() == b"effect"
    }

    pub fn try_line<E>(self) -> Result<(), Error<E>> {
        let expected = format!("GET {} HTTP/1.1\r\n", pool::SATURATION_PATH);
        if self.wire().starts_with(expected.as_bytes()) {
            Ok(())
        } else {
            Err(Error::Line {
                actual: self.head.to_owned(),
                expected,
            })
        }
    }
}
