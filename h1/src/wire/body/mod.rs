pub(crate) mod chunked;
pub(crate) mod fixed;
pub(crate) mod ingress;
mod shape;
mod view;

pub(crate) use view::View;

use sark_core::body::admit::Length;

/// A complete body retaining its exact decoder or completed storage borrow.
pub(crate) struct Frame<'frame, 'd> {
    pub(crate) consumed: usize,
    pub(crate) body: Payload<'frame, 'd>,
}

pub(crate) enum Payload<'frame, 'd> {
    Inline(usize),
    Chunked(chunked::Borrowed<'frame>),
    Streamed(ingress::Finished<'frame, 'd>),
    Discarded(usize),
}

impl Payload<'_, '_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Inline(len) | Self::Discarded(len) => *len,
            Self::Chunked(body) => body.len(),
            Self::Streamed(body) => body.len(),
        }
    }

    pub(crate) fn length(&self) -> Length {
        match self {
            Self::Inline(len) => Length::Known(*len),
            Self::Chunked(..) | Self::Streamed(..) | Self::Discarded(..) => Length::Unknown,
        }
    }
}

const _: () = {
    assert!(size_of::<Payload<'static, 'static>>() == 2 * size_of::<usize>());
    assert!(!core::mem::needs_drop::<Payload<'static, 'static>>());
};
