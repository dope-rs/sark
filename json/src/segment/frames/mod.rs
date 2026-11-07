use std::{
    marker,
    num::NonZero,
    ops::{self, ControlFlow},
    task::Poll,
};

use o3::{
    alloc,
    buffer::{resident, storage},
};
use sark_http::request;

use crate::{
    Error, Nesting, Plain, Raw, Text,
    input::Taken,
    segment::{self, Atom, Cursor, End, Outcome, cursor::Replay, input::Input, is_delimiter},
    source::string::Scan,
};

mod context;
mod sealed;
pub(super) use context::Context;
pub(super) use sealed::Sealed;

/// Representation of each element in a retained JSON array.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Mode {
    Frame,
    Plain,
    Raw,
}

/// Bytes admitted by this array's selected grammar, before owner binding.
#[repr(transparent)]
pub struct Captured<F> {
    bytes: storage::Shared,
    marker: marker::PhantomData<F>,
}

/// The admitted byte domain selected for each array element.
pub trait Capture<C: Context>: Sealed + Sized {
    type Value;
    const MODE: Mode;
    fn bind(context: C, bytes: Captured<Self>) -> Self::Value;
}

impl Capture<()> for Text<storage::Shared> {
    type Value = Self;
    const MODE: Mode = Mode::Frame;
    fn bind(_: (), bytes: Captured<Self>) -> Self::Value {
        Self::admitted(bytes.bytes)
    }
}

impl<'d> Capture<request::Id<'d>> for Text<storage::Shared> {
    type Value = Text<request::Bytes<'d>>;
    const MODE: Mode = Mode::Frame;
    fn bind(context: request::Id<'d>, bytes: Captured<Self>) -> Self::Value {
        Self::admitted(bytes.bytes).into_request(context)
    }
}

impl Capture<()> for Plain<storage::Shared> {
    type Value = Self;
    const MODE: Mode = Mode::Plain;
    fn bind(_: (), bytes: Captured<Self>) -> Self::Value {
        Self::admitted(bytes.bytes)
    }
}

impl<'d> Capture<request::Id<'d>> for Plain<storage::Shared> {
    type Value = Plain<request::Bytes<'d>>;
    const MODE: Mode = Mode::Plain;
    fn bind(context: request::Id<'d>, bytes: Captured<Self>) -> Self::Value {
        Self::admitted(bytes.bytes).into_request(context)
    }
}

impl Capture<()> for Raw<storage::Shared> {
    type Value = Self;
    const MODE: Mode = Mode::Raw;
    fn bind(_: (), bytes: Captured<Self>) -> Self::Value {
        Self::admitted(bytes.bytes)
    }
}

impl<'d> Capture<request::Id<'d>> for Raw<storage::Shared> {
    type Value = Raw<request::Bytes<'d>>;
    const MODE: Mode = Mode::Raw;
    fn bind(context: request::Id<'d>, bytes: Captured<Self>) -> Self::Value {
        Self::admitted(bytes.bytes).into_request(context)
    }
}

/// Cooperative materialization of a retained JSON array.
/// Copy states replay only a completed string's content in this same immutable
/// input. Their cursor and end stay with that input across budget yields.
pub struct Frames<'d, C: Context = (), F: Capture<C> = Text<storage::Shared>> {
    input: Input<'d>,
    cursor: Cursor,
    state: State,
    values: Vec<F::Value>,
    context: C,
    marker: marker::PhantomData<F>,
}

enum State {
    Leading,
    Element { end: End },
    String { start: usize, string: Scan },
    Atom { start: usize, atom: Atom },
    Compound { start: usize, nesting: Nesting },
    Copy(Replay),
    Delimiter,
    Trailing,
    Release,
    Failed(Option<crate::Error>),
}

impl<'d> Frames<'d> {
    pub fn from_scoped(input: Vec<resident::Shared<'d>>) -> crate::Result<Self> {
        Ok(Self::with_input(Input::new(input)?, ()))
    }
}

impl<'d, C: Context, F: Capture<C>> Frames<'d, C, F> {
    pub fn admitted(input: segment::Admitted<'d>, context: C) -> Self {
        Self::with_input(input.input, context)
    }

    fn with_input(input: Input<'d>, context: C) -> Self {
        Self {
            input,
            cursor: Cursor::new(),
            state: State::Leading,
            values: Vec::new(),
            context,
            marker: marker::PhantomData,
        }
    }

    fn capture(&mut self, range: ops::Range<usize>, removed: usize) -> crate::Result<()> {
        alloc::VecExt::try_reserve(&mut self.values, 1).map_err(Error::Allocation)?;
        let owned = if removed == 0 {
            self.input.try_own(range.clone())?
        } else {
            None
        };
        if let Some(shared) = owned {
            self.store(shared);
        } else {
            let output =
                alloc::VecExt::try_new(range.len() - removed).map_err(Error::Allocation)?;
            let cursor = Cursor::at(&self.input, range.start).ok_or(Error::Invalid)?;
            self.state = State::Copy(Replay::new(cursor, range.end, output, removed != 0));
        }
        Ok(())
    }

    fn store(&mut self, bytes: storage::Shared) {
        // SAFETY: capture reserved this slot; copying preserves the exclusive values table.
        (unsafe { alloc::raw::VecExt::vacancy(&mut self.values) })(F::bind(
            self.context,
            Captured {
                bytes,
                marker: marker::PhantomData,
            },
        ));
        self.state = State::Delimiter;
    }

    pub fn release(&mut self, budget: &mut usize) -> Poll<()> {
        if !matches!(self.state, State::Failed(_)) {
            self.state = State::Failed(None);
        }
        while !self.values.is_empty() {
            if *budget == 0 {
                return Poll::Pending;
            }
            drop(self.values.pop());
            *budget -= 1;
        }
        self.input.release(budget).0
    }

    pub fn step(mut self, budget: NonZero<usize>) -> Outcome<'d, Self, Vec<F::Value>> {
        if matches!(self.state, State::Failed(_)) {
            let mut left = budget.get();
            if self.release(&mut left).is_pending() {
                return Outcome::Pending(self);
            }
            if let State::Failed(Some(error)) = self.state {
                return Outcome::Failed(error.into());
            }
            return Outcome::Pending(self);
        }
        match self.try_step(budget) {
            Ok(ControlFlow::Continue(())) => Outcome::Pending(self),
            Ok(ControlFlow::Break(values)) => Outcome::Ready(values),
            Err(error) => {
                self.state = State::Failed(Some(error));
                Outcome::Pending(self)
            }
        }
    }

    fn try_step(&mut self, budget: NonZero<usize>) -> crate::Result<ControlFlow<Vec<F::Value>>> {
        let mut left = budget.get();
        while let Some(budget) = NonZero::new(left) {
            let input = &self.input;
            match &mut self.state {
                State::Failed(_) => break,
                State::Leading => {
                    if self.cursor.take_ws(input) == Taken::Yes {
                        left -= 1;
                    } else {
                        if self.cursor.take(input, b'[') == Taken::No {
                            return Err(Error::Invalid);
                        }
                        left -= 1;
                        self.state = State::Element { end: End::Allowed };
                    }
                }
                State::Element { end } => {
                    if self.cursor.take_ws(input) == Taken::Yes {
                        left -= 1;
                    } else if *end == End::Allowed && self.cursor.take(input, b']') == Taken::Yes {
                        left -= 1;
                        self.state = State::Trailing;
                    } else {
                        let (byte, advance) = self.cursor.read(input).ok_or(Error::Invalid)?;
                        if byte == b'"' {
                            self.cursor.advance(advance);
                            left -= 1;
                            self.state = State::String {
                                start: self.cursor.absolute,
                                string: Scan::new(input.is_admitted()),
                            };
                        } else if F::MODE != Mode::Raw {
                            return Err(Error::Invalid);
                        } else if matches!(byte, b'{' | b'[') {
                            let nesting = if input.is_admitted() {
                                Nesting::admitted()
                            } else {
                                Nesting::start(byte)?
                            };
                            let start = self.cursor.absolute;
                            self.cursor.advance(advance);
                            left -= 1;
                            self.state = State::Compound { start, nesting };
                        } else {
                            let atom = Atom::new(byte, false)?;
                            self.state = State::Atom {
                                start: self.cursor.absolute,
                                atom,
                            };
                        }
                    }
                }
                State::String { start, string } => {
                    let (byte, advance) = self.cursor.read(input).ok_or(Error::Invalid)?;
                    let decoded = string.step(byte)?;
                    if decoded.is_none() {
                        let (range, removed) = match F::MODE {
                            Mode::Raw => (*start - 1..self.cursor.absolute + 1, 0),
                            Mode::Plain if string.removed() != 0 => {
                                return Err(Error::Invalid);
                            }
                            Mode::Plain | Mode::Frame => {
                                (*start..self.cursor.absolute, string.removed())
                            }
                        };
                        self.cursor.advance(advance);
                        left -= 1;
                        self.capture(range, removed)?;
                    } else {
                        self.cursor.advance(advance);
                        left -= 1;
                    }
                }
                State::Atom { start, atom } => {
                    let (byte, advance) = self.cursor.read(input).ok_or(Error::Invalid)?;
                    if is_delimiter(byte) {
                        atom.finish()?;
                        let range = *start..self.cursor.absolute;
                        self.capture(range, 0)?;
                    } else {
                        *atom = atom.step(byte)?;
                        self.cursor.advance(advance);
                        left -= 1;
                    }
                }
                State::Compound { start, nesting } => {
                    let (byte, advance) = self.cursor.read(input).ok_or(Error::Invalid)?;
                    let done = nesting.step(byte)?;
                    self.cursor.advance(advance);
                    left -= 1;
                    if done {
                        let range = *start..self.cursor.absolute;
                        self.capture(range, 0)?;
                    }
                }
                State::Copy(replay) => {
                    if replay.is_done() {
                        let shared = replay.try_finish()?;
                        self.store(shared);
                    } else {
                        let copied = replay.try_copy(input, budget)?;
                        left -= copied.get();
                    }
                }
                State::Delimiter => {
                    if self.cursor.take_ws(input) == Taken::Yes {
                        left -= 1;
                    } else if self.cursor.take(input, b',') == Taken::Yes {
                        left -= 1;
                        self.state = State::Element {
                            end: End::Forbidden,
                        };
                    } else if self.cursor.take(input, b']') == Taken::Yes {
                        left -= 1;
                        self.state = State::Trailing;
                    } else {
                        return Err(Error::Invalid);
                    }
                }
                State::Trailing => {
                    if self.cursor.take_ws(input) == Taken::Yes {
                        left -= 1;
                    } else if self.cursor.absolute == input.len() {
                        self.state = State::Release;
                    } else {
                        return Err(Error::Invalid);
                    }
                }
                State::Release => {
                    if self.input.release(&mut left).0.is_ready() {
                        return Ok(ControlFlow::Break(std::mem::take(&mut self.values)));
                    }
                }
            }
        }
        Ok(ControlFlow::Continue(()))
    }
}
