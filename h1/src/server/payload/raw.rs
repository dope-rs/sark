use std::num;

use dope::net::link::egress::data::{self, exec};
use o3::buffer::{self, hold, segments::scatter};

use crate::server::payload::{Cursor, Frame, Inner, State, Stream};

fn cursor<'a, 'd>(frame: &'a Frame<'d>, offset: data::Offset) -> Option<Cursor<'a, 'd>> {
    if offset.get() >= frame.len() {
        return None;
    }
    // SAFETY: this immutable frame retains every span and the checked offset
    // lies strictly below their exact total length.
    Some(unsafe { resume(frame, offset) })
}

/// # Safety
/// The offset must lie strictly below the frame's logical length.
unsafe fn resume<'a, 'd>(frame: &'a Frame<'d>, offset: data::Offset) -> Cursor<'a, 'd> {
    let state = match &frame.inner {
        Inner::Chunk(parts) => {
            let prefix = parts.prefix.as_ref();
            let body = parts.body.owner().as_ref();
            match offset.split_at(prefix.len()) {
                data::Split::Before(offset) => State::Prefix {
                    parts,
                    bytes: unsafe { prefix.get_unchecked(offset.get()..) },
                },
                data::Split::After(offset) => match offset.split_at(body.len()) {
                    data::Split::Before(offset) => State::Body {
                        suffix: &parts.suffix,
                        bytes: unsafe { body.get_unchecked(offset.get()..) },
                    },
                    data::Split::After(offset) => {
                        let suffix = parts.suffix.as_ref();
                        State::Tail(unsafe { suffix.get_unchecked(offset.get()..) })
                    }
                },
            }
        }
        Inner::Terminator(bytes) => {
            let bytes = bytes.as_ref();
            State::Tail(unsafe { bytes.get_unchecked(offset.get()..) })
        }
    };
    Cursor(state)
}

unsafe impl<'a, 'd> buffer::raw::Scatter<'a> for Cursor<'a, 'd> {
    fn remaining(&self) -> usize {
        match self.0 {
            State::Prefix { parts, bytes } => {
                bytes.len() + parts.body.owner().len() + parts.suffix.len()
            }
            State::Body { suffix, bytes } => bytes.len() + suffix.len(),
            State::Tail(bytes) => bytes.len(),
        }
    }

    unsafe fn advance<const MAX: usize>(
        &mut self,
        cap: num::NonZero<usize>,
    ) -> buffer::raw::Chunk<'a, MAX> {
        let (bytes, state) = match self.0 {
            State::Prefix { parts, bytes } => {
                let (bytes, rest) = bytes.split_at(bytes.len().min(cap.get()));
                let state = if rest.is_empty() {
                    State::Body {
                        suffix: &parts.suffix,
                        bytes: parts.body.owner().as_ref(),
                    }
                } else {
                    State::Prefix { parts, bytes: rest }
                };
                (bytes, state)
            }
            State::Body { suffix, bytes } => {
                let (bytes, rest) = bytes.split_at(bytes.len().min(cap.get()));
                let state = if rest.is_empty() {
                    State::Tail(suffix.as_ref())
                } else {
                    State::Body {
                        suffix,
                        bytes: rest,
                    }
                };
                (bytes, state)
            }
            State::Tail(bytes) => {
                let (bytes, rest) = bytes.split_at(bytes.len().min(cap.get()));
                (bytes, State::Tail(rest))
            }
        };
        self.0 = state;
        // SAFETY: the closed state excludes empty spans, caps the emitted
        // prefix, and carries the exact logical suffix length.
        unsafe { buffer::raw::Chunk::new(bytes) }
    }
}

// SAFETY: The closed frame variants retain fixed spans; the cursor traverses exactly those spans in wire order.
unsafe impl<'d, K> data::raw::Payload<'d> for Stream<'d, K> {
    type Mode = exec::Fallible;
    type Cursor<'a>
        = Cursor<'a, 'd>
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.frame.len()
    }

    fn charge(&self) -> hold::Charge {
        self.frame.charge
    }

    fn start(&self) -> Self::Cursor<'_> {
        data::raw::Payload::start(&self.frame)
    }

    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>> {
        cursor(&self.frame, offset)
    }

    unsafe fn resume(
        &self,
        _: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_> {
        // SAFETY: resume requires this offset to lie strictly below the frame length.
        unsafe { resume(&self.frame, offset) }
    }
}

// SAFETY: The closed frame variants retain fixed spans; the cursor traverses exactly those spans in wire order.
unsafe impl<'d> data::raw::Payload<'d> for Frame<'d> {
    type Mode = exec::Fallible;
    type Cursor<'a>
        = Cursor<'a, 'd>
    where
        Self: 'a;

    fn len(&self) -> usize {
        self.charge.logical_bytes
    }

    fn charge(&self) -> hold::Charge {
        self.charge
    }

    fn start(&self) -> Self::Cursor<'_> {
        Cursor(match &self.inner {
            Inner::Chunk(parts) => State::Prefix {
                parts,
                bytes: parts.prefix.as_ref(),
            },
            Inner::Terminator(bytes) => State::Tail(bytes.as_ref()),
        })
    }

    fn cursor(&self, offset: data::Offset) -> Option<Self::Cursor<'_>> {
        cursor(self, offset)
    }

    unsafe fn resume(
        &self,
        _: &mut scatter::raw::Position,
        offset: data::Offset,
    ) -> Self::Cursor<'_> {
        // SAFETY: resume requires this offset to lie strictly below the frame length.
        unsafe { resume(self, offset) }
    }
}
