pub(crate) mod chunked;
pub(crate) mod fixed;
pub(crate) mod ingress;
mod shape;
mod view;

pub(crate) use view::View;

use core::num::NonZero;

use sark_core::body::admit::Length;

/// A complete body retaining its exact decoder or completed storage borrow.
pub(crate) enum Payload<'frame, 'd, H = ()> {
    Inline(usize, H),
    Chunked(chunked::Borrowed<'frame>, H),
    Streamed(ingress::Finished<'frame, 'd>, H),
    Discarded(usize, H),
}

impl<'frame, 'd, H> Payload<'frame, 'd, H> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Inline(len, _) | Self::Discarded(len, _) => *len,
            Self::Chunked(body, _) => body.len(),
            Self::Streamed(body, _) => body.len(),
        }
    }

    pub(crate) fn length(&self) -> Length {
        match self {
            Self::Inline(len, _) => Length::Known(*len),
            Self::Chunked(..) | Self::Streamed(..) | Self::Discarded(..) => Length::Unknown,
        }
    }

    pub(crate) fn into_parts(self) -> (Payload<'frame, 'd>, H) {
        match self {
            Self::Inline(len, head) => (Payload::Inline(len, ()), head),
            Self::Chunked(body, head) => (Payload::Chunked(body, ()), head),
            Self::Streamed(body, head) => (Payload::Streamed(body, ()), head),
            Self::Discarded(len, head) => (Payload::Discarded(len, ()), head),
        }
    }
}

impl<'frame, 'd> Payload<'frame, 'd> {
    /// Packs the same source's admitted head length beside the completion tag.
    pub(crate) fn with_head(self, head: NonZero<u32>) -> Payload<'frame, 'd, NonZero<u32>> {
        match self {
            Self::Inline(len, ()) => Payload::Inline(len, head),
            Self::Chunked(body, ()) => Payload::Chunked(body, head),
            Self::Streamed(body, ()) => Payload::Streamed(body, head),
            Self::Discarded(len, ()) => Payload::Discarded(len, head),
        }
    }
}

const _: () = {
    assert!(size_of::<Payload<'static, 'static>>() == 2 * size_of::<usize>());
    assert!(size_of::<Payload<'static, 'static, NonZero<u32>>>() == 2 * size_of::<usize>());
    assert!(!core::mem::needs_drop::<Payload<'static, 'static>>());
};
