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
    block_len: usize,
    frame_len: usize,
    error: Option<consume::Error>,
}

/// A complete wire owner whose stream-dependent frame headers are patchable.
pub(super) struct Block {
    bytes: pool::Span,
    max_frame: numbers::Limit,
    block_len: usize,
}

impl Wire {
    pub(super) fn new(mut bytes: pool::Span, max: numbers::Limit) -> Result<Self, consume::Error> {
        bytes.try_append(&[0; frame::HEADER_LEN])?.commit();
        Ok(Self {
            bytes,
            max_frame: max,
            block_len: 0,
            frame_len: 0,
            error: None,
        })
    }

    pub(super) fn finish(self) -> Result<Block, consume::Error> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(Block {
                bytes: self.bytes,
                max_frame: self.max_frame,
                block_len: self.block_len,
            }),
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
        if self.error.is_some() {
            return;
        }
        if self.frame_len == self.max_frame.into_usize() {
            if let Err(error) = self
                .bytes
                .try_append(&[0; frame::HEADER_LEN])
                .map(|append| append.commit())
            {
                self.error = Some(error);
                return;
            }
            self.frame_len = 0;
        }
        if let Err(error) = self.bytes.try_append(&[byte]).map(|append| append.commit()) {
            self.error = Some(error);
            return;
        }
        self.frame_len += 1;
        self.block_len += 1;
    }

    fn write_slice(&mut self, bytes: &[u8]) {
        if self.error.is_some() {
            return;
        }
        let max = self.max_frame.into_usize();
        let mut rest = bytes;
        while !rest.is_empty() {
            if self.frame_len == max {
                if let Err(error) = self
                    .bytes
                    .try_append(&[0; frame::HEADER_LEN])
                    .map(|append| append.commit())
                {
                    self.error = Some(error);
                    return;
                }
                self.frame_len = 0;
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
            self.block_len += take;
            rest = &rest[take..];
        }
    }
}

impl Block {
    pub(super) fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Every payload frame is preceded by one fixed header and the block counts
    /// payload bytes only, so the buffer is `[header][payload]` pairs strided by
    /// `max_frame + HEADER_LEN`.
    pub(super) fn seal(&mut self, id: stream::Id, boundary: Boundary) {
        let step = self.max_frame.into_usize() + frame::HEADER_LEN;
        let mut left = self.block_len;
        let mut first = true;
        for chunk in self.bytes.as_mut_slice().chunks_mut(step) {
            let length = self.max_frame.take(left);
            let len = length.into_usize();
            let chain = if len == left {
                frame::Block::End
            } else {
                frame::Block::More
            };
            let flags = (u8::from(first && matches!(boundary, Boundary::End))
                * numbers::Flags::END_STREAM)
                | match chain {
                    frame::Block::More => 0,
                    frame::Block::End => numbers::Flags::END_HEADERS,
                };
            let header = layout::Header {
                length,
                kind: if first {
                    numbers::Type::Headers
                } else {
                    numbers::Type::Continuation
                },
                flags: numbers::Flags(flags),
                stream_id: id,
            }
            .wire_bytes();
            // SAFETY: `Limit` admits at least 16384, so `step` exceeds
            // `HEADER_LEN`; `Wire` writes one header per frame and advances
            // `block_len` only by payload bytes, so every full chunk is one
            // header plus `max_frame` payload bytes and the final chunk is one
            // header plus the remainder, which is never shorter than the header.
            let (target, _) = unsafe { chunk.split_at_mut_unchecked(frame::HEADER_LEN) };
            target.copy_from_slice(&header);
            left -= len;
            if matches!(chain, frame::Block::End) {
                break;
            }
            first = false;
        }
    }

    pub(super) fn into_shared(self) -> storage::Shared {
        self.bytes.freeze().into_shared()
    }
}

impl AsRef<[u8]> for Block {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

const _: () = assert!(core::mem::size_of::<Block>() <= 5 * core::mem::size_of::<usize>());
