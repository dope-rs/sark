use std::ops;

use o3::buffer::{segments::queue, write::consume};
use sark_core::life::session;

use crate::conn::{
    self,
    ingress::input::{self, Frame, Source as _},
};

/// Header bytes either remain in the queue or borrow the current receive turn.
pub(crate) enum Input<'a, 'bytes, C: session::Storage> {
    Queued(&'a mut input::FrameInput<C>),
    Borrowed {
        bytes: &'bytes [u8],
        consumed: &'a mut bool,
    },
}

pub(crate) enum Bytes<'a, C> {
    Queued(queue::Slice<'a, C>),
    Borrowed(&'a [u8]),
}

impl<C: session::Storage> Input<'_, '_, C> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Queued(input) => input.bytes.len(),
            Self::Borrowed { bytes, .. } => bytes.len(),
        }
    }

    pub(crate) fn get(&self, range: ops::Range<usize>) -> Result<Bytes<'_, C>, conn::Error> {
        let error = conn::Error::InputRange {
            offset: range.start,
            need: range.len(),
            have: self.len(),
        };
        match self {
            Self::Queued(input) => input.bytes.get(range).map(Bytes::Queued),
            Self::Borrowed { bytes, .. } => bytes.get(range).map(Bytes::Borrowed),
        }
        .ok_or(error)
    }

    pub(crate) fn prepare(&mut self, total: usize) -> Result<Frame<'_, C>, conn::Error> {
        match self {
            Self::Queued(input) => input.prepare(total),
            Self::Borrowed { bytes, consumed } => {
                consume::Error::check(total, bytes.len()).map_err(conn::Error::Consume)?;
                Ok(Frame::Headers(consumed))
            }
        }
    }

    pub(crate) fn consume(&mut self, total: usize) -> Result<(), conn::Error> {
        self.prepare(total)?.commit();
        Ok(())
    }
}

impl<'a, C: session::Storage> Bytes<'a, C> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Queued(bytes) => bytes.len(),
            Self::Borrowed(bytes) => bytes.len(),
        }
    }

    pub(crate) fn split(&self) -> (&'a [u8], Option<queue::Iter<'a, C>>) {
        match self {
            Self::Queued(bytes) => {
                let mut rest = bytes.fragments();
                let first = rest.next().unwrap_or_default();
                (first, (first.len() != bytes.len()).then_some(rest))
            }
            Self::Borrowed(bytes) => (*bytes, None),
        }
    }

    pub(crate) fn fragments(&self) -> impl Iterator<Item = &'a [u8]> {
        let (one, many) = match self {
            Self::Queued(bytes) => (None, Some(bytes.fragments())),
            Self::Borrowed(bytes) => (Some(*bytes), None),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
}
