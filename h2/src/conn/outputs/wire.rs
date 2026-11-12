use o3::buffer::{pool, storage, write::consume};
use sark_core::manifold::stream::Boundary;
use sark_http::field;

use crate::{
    frame,
    frame::{layout, numbers},
    stream,
};

/// Bounded HPACK-to-frame writer backed by one fixed pool slot.
pub(super) struct Wire {
    bytes: pool::Span,
    max_frame: numbers::Limit,
    continuation: [u8; frame::HEADER_LEN],
    frame_len: usize,
    error: Option<consume::Error>,
}

/// One complete framed header block.
pub(super) struct Block(pool::Span);

impl Wire {
    pub(super) fn new(
        mut bytes: pool::Span,
        max: numbers::Limit,
        target: stream::raw::Marked,
    ) -> Result<Self, consume::Error> {
        let mut header = layout::Header {
            length: max.take(max.into_usize()),
            kind: numbers::Type::Headers,
            flags: numbers::Flags(
                u8::from(matches!(target.boundary(), Boundary::End)) * numbers::Flags::END_STREAM,
            ),
            stream_id: target.key().id(),
        }
        .wire_bytes();
        bytes.try_append(&header)?.commit();
        header[3] = numbers::Type::Continuation as u8;
        header[4] = 0;
        Ok(Self {
            bytes,
            max_frame: max,
            continuation: header,
            frame_len: 0,
            error: None,
        })
    }

    fn advance(&mut self) -> bool {
        if self.frame_len == self.max_frame.into_usize() {
            if let Err(error) = self
                .bytes
                .try_append(&self.continuation)
                .map(|append| append.commit())
            {
                self.error = Some(error);
                return false;
            }
            self.frame_len = 0;
        }
        true
    }

    pub(super) fn finish(mut self) -> Result<Block, consume::Error> {
        match self.error {
            Some(error) => Err(error),
            None => {
                let end = self.bytes.len() - self.frame_len;
                // SAFETY: each frame starts with its header; payload writes
                // advance the span length and frame_len together.
                let last = unsafe {
                    self.bytes
                        .as_mut_slice()
                        .get_unchecked_mut(end - frame::HEADER_LEN..end)
                };
                last[..3].copy_from_slice(&self.max_frame.take(self.frame_len).wire_bytes());
                last[4] |= numbers::Flags::END_HEADERS;
                Ok(Block(self.bytes))
            }
        }
    }
}

impl Extend<u8> for Wire {
    fn extend<I: IntoIterator<Item = u8>>(&mut self, iter: I) {
        for byte in iter {
            field::Writer::write_byte(self, byte);
            if self.error.is_some() {
                break;
            }
        }
    }
}

impl field::Writer for Wire {
    fn write_byte(&mut self, byte: u8) {
        if self.error.is_some() || !self.advance() {
            return;
        }
        if let Err(error) = self.bytes.try_append(&[byte]).map(|append| append.commit()) {
            self.error = Some(error);
            return;
        }
        self.frame_len += 1;
    }

    fn write_slice(&mut self, bytes: &[u8]) {
        if self.error.is_some() {
            return;
        }
        let max = self.max_frame.into_usize();
        let mut rest = bytes;
        while !rest.is_empty() {
            if !self.advance() {
                return;
            }
            let take = rest.len().min(max - self.frame_len);
            if let Err(error) = self
                .bytes
                .try_append(&rest[..take])
                .map(|append| append.commit())
            {
                self.error = Some(error);
                return;
            }
            self.frame_len += take;
            rest = &rest[take..];
        }
    }
}

impl Block {
    pub(super) fn into_shared(self) -> storage::Shared {
        self.0.freeze().into_shared()
    }
}

impl AsRef<[u8]> for Block {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

const _: () = assert!(core::mem::size_of::<Block>() == core::mem::size_of::<pool::Span>());
