use core::{num::NonZero, ops::ControlFlow};

use crate::h1::chunk::{Error, Framer, Limit, Run, scan::state};

/// The exact trailer phase, with no body or size-line alternatives.
#[derive(Clone, Copy)]
pub(in crate::h1) struct Trailer {
    pub(in crate::h1) cursor: Cursor,
    limits: Limit,
}

/// Mutable scan progress; limits remain with the original trailer owner.
#[derive(Clone, Copy)]
pub(in crate::h1) struct Cursor {
    pub(super) state: state::State,
    pub(super) wire: usize,
}

pub(in crate::h1) enum Progress {
    More {
        next: Cursor,
        consumed: Option<NonZero<usize>>,
    },
    Complete {
        wire: usize,
        consumed: NonZero<usize>,
        trailers: NonZero<u32>,
    },
}

/// The current input's exact admitted trailer prefix and its continuation.
pub(in crate::h1) struct Partial<'input> {
    bytes: &'input [u8],
    consumed: NonZero<usize>,
    next: Trailer,
}

impl Trailer {
    pub(super) const fn new(state: state::State, wire: usize, limits: Limit) -> Self {
        Self {
            cursor: Cursor { state, wire },
            limits,
        }
    }

    pub(in crate::h1) const fn capacity(&self) -> usize {
        self.limits.trailers()
    }

    pub(in crate::h1) fn scan(mut self, input: &[u8]) -> Result<(Progress, &[u8]), Error> {
        let mut consumed = None;
        for (offset, &byte) in input.iter().enumerate() {
            let wire = Framer::wire(self.cursor.wire, 1, &self.limits)?;
            let progress = self
                .cursor
                .state
                .scan(byte, self.cursor.wire, &self.limits)?;
            self.cursor.wire = wire;
            let next = NonZero::<usize>::MIN.saturating_add(offset);
            match progress {
                ControlFlow::Continue(state) => self.cursor.state = state,
                ControlFlow::Break(trailers) => {
                    return Ok((
                        Progress::Complete {
                            wire,
                            consumed: next,
                            trailers,
                        },
                        Run::complete(input, next.get()).into_slice(),
                    ));
                }
            }
            consumed = Some(next);
        }
        Ok((
            Progress::More {
                next: self.cursor,
                consumed,
            },
            input,
        ))
    }
}

impl<'input> Partial<'input> {
    pub(super) fn new(bytes: &'input [u8], consumed: NonZero<usize>, next: Trailer) -> Self {
        Self {
            bytes,
            consumed,
            next,
        }
    }

    pub(in crate::h1) const fn consumed(&self) -> NonZero<usize> {
        self.consumed
    }

    pub(in crate::h1) fn into_parts(self) -> (&'input [u8], NonZero<usize>, Trailer) {
        (self.bytes, self.consumed, self.next)
    }
}
