use std::ops;

use sark_http::request::body;

use crate::request::{Head, frame, frame::proof};

pub struct View<'req, 'd: 'req, P = proof::Maybe> {
    pub(in crate::request) head: Head<&'req [u8], P>,
    body: body::Ref<'req, 'd>,
}

impl<'req, 'd: 'req, P: proof::Proof> View<'req, 'd, P> {
    #[doc(hidden)]
    pub const fn from_head(head: Head<&'req [u8], P>, body: body::Ref<'req, 'd>) -> Self {
        Self { head, body }
    }

    pub fn try_path(&self, range: ops::Range<usize>) -> Result<&'req [u8], frame::Error> {
        try_slice(self.head.path(), range)
    }

    pub fn try_frame_at(&self, range: ops::Range<usize>) -> Result<&'req [u8], frame::Error> {
        try_slice(self.head.owner(), range)
    }

    pub const fn body(&self) -> body::Ref<'req, 'd> {
        self.body
    }

    pub const fn body_len(&self) -> usize {
        self.body.len()
    }
}

fn try_slice(bytes: &[u8], range: ops::Range<usize>) -> Result<&[u8], frame::Error> {
    let start = range.start;
    let end = range.end;
    let capacity = bytes.len();
    if start > end {
        return Err(frame::Error::Order {
            start,
            end,
            capacity,
        });
    }
    bytes.get(range).ok_or(frame::Error::Bounds {
        start,
        end,
        capacity,
    })
}
