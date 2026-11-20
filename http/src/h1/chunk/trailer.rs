use core::{num::NonZero, ops::ControlFlow};

use crate::h1::chunk::{Error, Framer, Framing, Run, scan::state};

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
pub struct Pending<'input> {
    pub(in crate::h1) bytes: &'input [u8],
    pub(super) consumed: NonZero<usize>,
    pub(in crate::h1) cursor: Cursor,
}

impl Cursor {
    pub(in crate::h1) fn scan<'a>(
        &self,
        input: &'a [u8],
        limits: &Framing,
    ) -> Result<(Progress, &'a [u8]), Error> {
        let mut cursor = *self;
        for (offset, &byte) in input.iter().enumerate() {
            let wire = Framer::wire(cursor.wire, 1, limits)?;
            let progress = cursor.state.scan(byte, cursor.wire, limits)?;
            cursor.wire = wire;
            match progress {
                ControlFlow::Continue(state) => cursor.state = state,
                ControlFlow::Break(trailers) => {
                    let next = NonZero::<usize>::MIN.saturating_add(offset);
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
        }
        Ok((
            Progress::More {
                next: cursor,
                consumed: NonZero::new(input.len()),
            },
            input,
        ))
    }
}

impl Pending<'_> {
    pub const fn consumed(&self) -> NonZero<usize> {
        self.consumed
    }

    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}
