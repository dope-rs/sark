use core::num::NonZero;

use crate::h1::chunk::{Boundary, Fragment, Run, trailer};

pub(super) mod state;

pub(in crate::h1) enum Decoded<'input> {
    More(usize),
    Data {
        consumed: NonZero<usize>,
        bytes: &'input [u8],
        boundary: Boundary,
    },
    Trailer(trailer::Partial<'input>),
    Complete {
        consumed: NonZero<usize>,
        bytes: &'input [u8],
    },
}

impl<'input> Decoded<'input> {
    pub(super) fn fragment(self) -> Fragment<'input> {
        match self {
            Self::More(consumed) => Fragment::More(consumed),
            Self::Data {
                consumed,
                bytes,
                boundary,
            } => Fragment::Data {
                consumed,
                bytes,
                boundary,
            },
            Self::Trailer(partial) => Fragment::Trailer(partial.consumed()),
            Self::Complete { consumed, bytes } => Fragment::Complete { consumed, bytes },
        }
    }
}

/// Output selection for the shared framing machine. Whole scans continue at
/// data boundaries; fragment decodes retain each narrowed transition result.
pub(in crate::h1) trait Mode {
    type Output<'input>;

    fn data<'input>(
        consumed: NonZero<usize>,
        bytes: &'input [u8],
        boundary: Boundary,
    ) -> Option<Self::Output<'input>>;

    fn pending(input: &[u8], consumed: usize) -> Self::Output<'_>;

    fn trailer<'input>(
        input: &'input [u8],
        consumed: NonZero<usize>,
        bytes: &'input [u8],
        next: trailer::Trailer,
    ) -> Self::Output<'input>;

    fn complete<'input>(
        input: &'input [u8],
        consumed: NonZero<usize>,
        bytes: &'input [u8],
    ) -> Self::Output<'input>;

    fn finished(input: &[u8]) -> Self::Output<'_>;
}

pub(in crate::h1) struct Whole;
pub(in crate::h1) struct Fragments;

impl Mode for Whole {
    type Output<'input> = Run<'input>;

    fn data<'input>(_: NonZero<usize>, _: &'input [u8], _: Boundary) -> Option<Run<'input>> {
        None
    }

    fn pending(input: &[u8], consumed: usize) -> Run<'_> {
        Run::pending(input, consumed)
    }

    fn trailer<'input>(
        input: &'input [u8],
        consumed: NonZero<usize>,
        _: &'input [u8],
        _: trailer::Trailer,
    ) -> Run<'input> {
        Run::pending(input, consumed.get())
    }

    fn complete<'input>(
        input: &'input [u8],
        consumed: NonZero<usize>,
        _: &'input [u8],
    ) -> Run<'input> {
        Run::complete(input, consumed.get())
    }

    fn finished(input: &[u8]) -> Run<'_> {
        Run::complete(input, 0)
    }
}

impl Mode for Fragments {
    type Output<'input> = Decoded<'input>;

    fn data<'input>(
        consumed: NonZero<usize>,
        bytes: &'input [u8],
        boundary: Boundary,
    ) -> Option<Decoded<'input>> {
        Some(Decoded::Data {
            consumed,
            bytes,
            boundary,
        })
    }

    fn pending(_: &[u8], consumed: usize) -> Decoded<'_> {
        Decoded::More(consumed)
    }

    fn trailer<'input>(
        _: &'input [u8],
        consumed: NonZero<usize>,
        bytes: &'input [u8],
        next: trailer::Trailer,
    ) -> Decoded<'input> {
        Decoded::Trailer(trailer::Partial::new(bytes, consumed, next))
    }

    fn complete<'input>(
        _: &'input [u8],
        consumed: NonZero<usize>,
        bytes: &'input [u8],
    ) -> Decoded<'input> {
        Decoded::Complete {
            consumed,
            bytes: bytes
                .split_last_chunk::<2>()
                .map_or(&bytes[..0], |(fields, _)| fields),
        }
    }

    fn finished(_: &[u8]) -> Decoded<'_> {
        Decoded::More(0)
    }
}
