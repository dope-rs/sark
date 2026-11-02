use std::{iter, num::NonZero, ops::ControlFlow, task::Poll};

use o3::{
    alloc,
    buffer::{resident, storage},
};

use crate::{
    Error, Nesting,
    input::Taken,
    segment::{
        Admitted, Cursor, Decode, Decoder, End, Outcome, Progress,
        input::{Input, Split},
    },
};

/// Cooperative materialization of a JSON array whose elements are generated
/// JSON objects.
pub struct Nested<'d, T: Decode<C>, C: Copy = ()> {
    input: Input<'d>,
    cursor: Cursor,
    state: State<'d, T, C>,
    context: C,
    values: Vec<T>,
    error: Option<Result<(), crate::parsing::Error<'d>>>,
}

enum State<'d, T: Decode<C>, C: Copy> {
    Leading,
    Element { end: End },
    Group { start: usize, nesting: Nesting },
    Projecting(Split<'d>),
    Starting(Admitted<'d>),
    Decoding(Box<Decoder<'d, T, C>>),
    Delimiter,
    Trailing,
    Release,
}

impl<'d, T: Decode> Nested<'d, T> {
    pub fn one(input: storage::Shared) -> crate::Result<Self> {
        Self::new(iter::once(input))
    }

    pub fn new<I, O>(input: I) -> crate::Result<Self>
    where
        I: IntoIterator<Item = O>,
        I::IntoIter: ExactSizeIterator,
        resident::Shared<'d>: From<O>,
    {
        Ok(Self::with_input(Input::new(input)?, ()))
    }
}

impl<'d, T: Decode<C>, C: Copy> Nested<'d, T, C> {
    pub fn admitted(input: Admitted<'d>, context: C) -> Self {
        Self::with_input(input.input, context)
    }

    fn with_input(input: Input<'d>, context: C) -> Self {
        Self {
            input,
            cursor: Cursor::new(),
            state: State::Leading,
            values: Vec::new(),
            context,
            error: None,
        }
    }

    pub fn release(&mut self, budget: &mut usize) -> Poll<()> {
        self.error.get_or_insert(Ok(()));
        let state = match &mut self.state {
            State::Projecting(parts) => parts.release(budget),
            State::Starting(input) => input.release(budget),
            State::Decoding(worker) => worker.release(budget),
            State::Leading
            | State::Element { .. }
            | State::Group { .. }
            | State::Delimiter
            | State::Trailing
            | State::Release => Poll::Ready(()),
        };
        if state.is_pending() {
            return Poll::Pending;
        }
        while let Some(value) = self.values.last_mut() {
            if *budget == 0 || <T as Decode<C>>::release(value, budget).is_pending() {
                return Poll::Pending;
            }
            if *budget == 0 {
                return Poll::Pending;
            }
            drop(self.values.pop());
            *budget -= 1;
        }
        self.input.release(budget).0
    }

    pub fn step(mut self, budget: NonZero<usize>) -> Outcome<'d, Self, Vec<T>> {
        if self.error.is_some() {
            let mut left = budget.get();
            if self.release(&mut left).is_pending() {
                return Outcome::Pending(self);
            }
            if let Some(Err(error)) = self.error {
                return Outcome::Failed(error);
            }
            return Outcome::Pending(self);
        }
        match self.try_step(budget) {
            Ok(ControlFlow::Continue(())) => Outcome::Pending(self),
            Ok(ControlFlow::Break(values)) => Outcome::Ready(values),
            Err(error) => {
                self.error = Some(Err(error));
                Outcome::Pending(self)
            }
        }
    }

    fn try_step(
        &mut self,
        budget: NonZero<usize>,
    ) -> Result<ControlFlow<Vec<T>>, crate::parsing::Error<'d>> {
        let mut left = budget.get();
        while let Some(work) = NonZero::new(left) {
            let input = &self.input;
            match &mut self.state {
                State::Leading => {
                    if self.cursor.take_ws(input) == Taken::Yes {
                        left -= 1;
                    } else {
                        if self.cursor.take(input, b'[') == Taken::No {
                            return Err(Error::Invalid.into());
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
                        if !matches!(byte, b'{' | b'[') {
                            return Err(Error::Invalid.into());
                        }
                        let nesting = if input.is_admitted() {
                            Nesting::admitted()
                        } else {
                            Nesting::start(byte)?
                        };
                        let start = self.cursor.absolute;
                        self.cursor.advance(advance);
                        left -= 1;
                        self.state = State::Group { start, nesting };
                    }
                }
                State::Group { start, nesting } => {
                    let (byte, advance) = self.cursor.read(input).ok_or(Error::Invalid)?;
                    let done = nesting.step(byte)?;
                    self.cursor.advance(advance);
                    left -= 1;
                    if done {
                        let parts = Split::new(input, *start..self.cursor.absolute)?;
                        alloc::VecExt::try_reserve(&mut self.values, 1)
                            .map_err(Error::Allocation)?;
                        self.state = State::Projecting(parts);
                    }
                }
                State::Projecting(parts) => {
                    let ready = parts.step(input, &mut left)?;
                    if let Some(input) = ready {
                        self.state = State::Starting(Admitted { input });
                    }
                }
                State::Starting(input) => {
                    let worker = Decoder::try_admitted(input, self.context)?;
                    self.state = State::Decoding(worker);
                }
                State::Decoding(worker) => match worker.poll(work) {
                    Progress::NeedInput => return Err(Error::Invalid.into()),
                    Progress::Pending => return Ok(ControlFlow::Continue(())),
                    Progress::Ready(value) => {
                        // SAFETY: Group reserved this slot; projection and decoding preserve it.
                        unsafe { alloc::raw::VecExt::vacancy(&mut self.values) }.insert(value);
                        self.state = State::Delimiter;
                        return Ok(ControlFlow::Continue(()));
                    }
                    Progress::Failed(error) => return Err(error),
                },
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
                        return Err(Error::Invalid.into());
                    }
                }
                State::Trailing => {
                    if self.cursor.take_ws(input) == Taken::Yes {
                        left -= 1;
                    } else if self.cursor.absolute == input.len() {
                        self.state = State::Release;
                    } else {
                        return Err(Error::Invalid.into());
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
