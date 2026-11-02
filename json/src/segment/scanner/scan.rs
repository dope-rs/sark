use std::{convert::identity, mem};

use o3::{
    alloc::{self, array},
    buffer::resident,
};
use sark_http::payload::owned;

use crate::{
    Error, Nesting,
    input::Taken,
    segment::{
        Admitted, Atom, Capture, Create, Cursor, Field, Order, State, Transfer, ValueKind,
        input::{self, Input},
        is_delimiter, state,
        state::Name,
    },
    source::string,
};

pub(super) struct Scan<'d> {
    pub(super) input: Input<'d>,
    pub(super) cursor: Cursor,
    pub(super) state: State,
    pub(super) selection: Selection<'d>,
}

pub(in crate::segment) struct Selection<'d> {
    pub(super) fields: &'d [Field<'d>],
    order: Order,
    next: usize,
    pub(super) values: Vec<Option<state::Value>>,
    pub(super) comma: bool,
}

impl<'d> Selection<'d> {
    pub(in crate::segment) fn try_new(
        fields: &'d [Field<'d>],
        order: Order,
    ) -> Result<Self, alloc::Error> {
        let values = array::Block::try_new(fields.len(), ())?
            .init(|_| None)
            .into_vec();
        Ok(Self {
            fields,
            order,
            next: 0,
            values,
            comma: false,
        })
    }

    fn store(&mut self, capture: Capture, end: usize, kind: ValueKind) {
        if let Some((field, value)) = capture.finish(end, kind) {
            // SAFETY: Name admits this field in selection.fields. The values table has the
            // same fixed length; Capture retains a vacant slot until this value completes.
            *unsafe { self.values.get_unchecked_mut(field) } = Some(value);
        }
    }
}

impl<'d> Scan<'d> {
    pub(super) fn admitted(admitted: Admitted<'d>, selection: Selection<'d>) -> Self {
        Self {
            input: admitted.input,
            cursor: Cursor::new(),
            state: State::Leading,
            selection,
        }
    }

    pub(super) fn try_stream(fields: &'d [Field<'d>], order: Order) -> Result<Self, Create> {
        Ok(Self {
            input: Input::empty(),
            cursor: Cursor::new(),
            state: State::Leading,
            selection: Selection::try_new(fields, order).map_err(Create::from)?,
        })
    }

    pub(super) fn new<I, O>(owners: I, fields: &'d [Field<'d>], order: Order) -> crate::Result<Self>
    where
        I: IntoIterator<Item = O>,
        I::IntoIter: ExactSizeIterator,
        resident::Shared<'d>: From<O>,
    {
        Ok(Self {
            input: Input::new(owners)?,
            cursor: Cursor::new(),
            state: State::Leading,
            selection: Selection::try_new(fields, order).map_err(Error::Allocation)?,
        })
    }

    pub(super) fn try_new(
        owners: owned::Segments<'d>,
        fields: &'d [Field<'d>],
        order: Order,
    ) -> Result<Self, Create> {
        let input = Input::try_new(owners).map_err(Create::from)?;
        let set = Selection::try_new(fields, order).map_err(Create::from)?;
        Ok(Self {
            input,
            cursor: Cursor::new(),
            state: State::Leading,
            selection: set,
        })
    }

    pub(super) fn push(self, owner: resident::Shared<'d>) -> Transfer<Self, input::Failure<'d>> {
        let Self {
            input,
            cursor,
            state,
            selection: select,
        } = self;
        input.push(owner).map(
            |input| Self {
                input,
                cursor,
                state,
                selection: select,
            },
            identity,
        )
    }

    pub(super) fn skip(&mut self, budget: usize) -> usize {
        let Some(chunk) = self.cursor.chunk(&self.input) else {
            return 0;
        };
        let full = chunk.bytes();
        let skipped = self.state.skip(&full[..full.len().min(budget)]);
        let consumed = skipped.len();
        if consumed != 0 {
            self.cursor.advance(chunk.advance(skipped));
        }
        consumed
    }

    pub(super) fn transition(&mut self, budget: &mut usize) -> crate::Result<()> {
        let before = self.cursor.absolute;
        let state = mem::replace(&mut self.state, State::Done);
        if let State::Select(name) = state {
            self.state = name.step(
                &self.input,
                self.selection.fields,
                self.selection.order,
                &mut self.selection.next,
                budget,
            )?;
            return Ok(());
        }
        self.state = match state {
            State::Leading => {
                if self.cursor.take_ws(&self.input) == Taken::Yes {
                    State::Leading
                } else {
                    if self.cursor.take(&self.input, b'{') == Taken::No {
                        return Err(Error::Invalid);
                    }
                    State::NameOrEnd
                }
            }
            State::NameOrEnd => {
                if self.cursor.take_ws(&self.input) == Taken::Yes {
                    State::NameOrEnd
                } else if self.cursor.take(&self.input, b'}') == Taken::Yes {
                    if matches!(self.selection.order, Order::Ordered)
                        && self.selection.next != self.selection.fields.len()
                    {
                        return Err(Error::Invalid);
                    }
                    State::Trailing
                } else if self.selection.comma {
                    if self.cursor.take(&self.input, b',') == Taken::No {
                        return Err(Error::Invalid);
                    }
                    State::NameStart
                } else {
                    if self.cursor.take(&self.input, b'"') == Taken::No {
                        return Err(Error::Invalid);
                    }
                    State::Name {
                        start: self.cursor.absolute,
                        string: string::Scan::new(self.input.is_admitted()),
                    }
                }
            }
            State::NameStart => {
                if self.cursor.take_ws(&self.input) == Taken::Yes {
                    State::NameStart
                } else {
                    if self.cursor.take(&self.input, b'"') == Taken::No {
                        return Err(Error::Invalid);
                    }
                    State::Name {
                        start: self.cursor.absolute,
                        string: string::Scan::new(self.input.is_admitted()),
                    }
                }
            }
            State::Name { start, mut string } => {
                let (byte, advance) = self.cursor.read(&self.input).ok_or(Error::Invalid)?;
                self.cursor.advance(advance);
                if string.step(byte)?.is_none() {
                    State::Colon {
                        name: start..self.cursor.absolute - 1,
                        escaped: string.removed() != 0,
                    }
                } else {
                    State::Name { start, string }
                }
            }
            State::Colon { name, escaped } => {
                if self.cursor.take_ws(&self.input) == Taken::Yes {
                    State::Colon { name, escaped }
                } else {
                    if self.cursor.take(&self.input, b':') == Taken::No {
                        return Err(Error::Invalid);
                    }
                    let field = match self.selection.order {
                        Order::Unordered => 0,
                        Order::Ordered => self.selection.next,
                    };
                    let cursor = Cursor::at(&self.input, name.start).ok_or(Error::Invalid)?;
                    State::Select(Name::new(cursor, name.end, escaped, field))
                }
            }
            State::ValueStart {
                field,
                projection: mode,
            } => {
                if self.cursor.take_ws(&self.input) == Taken::Yes {
                    State::ValueStart {
                        field,
                        projection: mode,
                    }
                } else {
                    let (byte, advance) = self.cursor.read(&self.input).ok_or(Error::Invalid)?;
                    let field = field.filter(|&index| {
                        !self
                            .selection
                            .values
                            .get(index)
                            .is_some_and(Option::is_some)
                    });
                    let capture = Capture::new(field, mode, &self.cursor);
                    match byte {
                        b'"' => {
                            self.cursor.advance(advance);
                            State::String {
                                capture,
                                string: string::Scan::new(self.input.is_admitted()),
                            }
                        }
                        b'{' | b'[' => {
                            self.cursor.advance(advance);
                            let nesting = if self.input.is_admitted() {
                                Nesting::admitted()
                            } else {
                                Nesting::start(byte)?
                            };
                            State::Compound { capture, nesting }
                        }
                        _ => State::Atom {
                            capture,
                            atom: Atom::new(byte, field.is_some())?,
                        },
                    }
                }
            }
            State::String {
                capture,
                mut string,
            } => {
                let (byte, advance) = self.cursor.read(&self.input).ok_or(Error::Invalid)?;
                self.cursor.advance(advance);
                if string.step(byte)?.is_none() {
                    self.selection.store(
                        capture,
                        self.cursor.absolute,
                        ValueKind::String {
                            escapes: string.removed(),
                        },
                    );
                    self.selection.comma = true;
                    State::NameOrEnd
                } else {
                    State::String { capture, string }
                }
            }
            State::Compound {
                capture,
                mut nesting,
            } => {
                let (byte, advance) = self.cursor.read(&self.input).ok_or(Error::Invalid)?;
                self.cursor.advance(advance);
                if nesting.step(byte)? {
                    self.selection
                        .store(capture, self.cursor.absolute, ValueKind::Compound);
                    self.selection.comma = true;
                    State::NameOrEnd
                } else {
                    State::Compound { capture, nesting }
                }
            }
            State::Atom { capture, atom } => match self.cursor.read(&self.input) {
                Some((byte, advance)) if !is_delimiter(byte) => {
                    self.cursor.advance(advance);
                    State::Atom {
                        capture,
                        atom: atom.step(byte)?,
                    }
                }
                None | Some(_) => {
                    let kind = atom.finish()?;
                    self.selection.store(capture, self.cursor.absolute, kind);
                    self.selection.comma = true;
                    State::NameOrEnd
                }
            },
            State::Trailing => {
                if self.cursor.take_ws(&self.input) == Taken::Yes {
                    State::Trailing
                } else if self.cursor.absolute == self.input.len() {
                    State::Done
                } else {
                    return Err(Error::Invalid);
                }
            }
            State::Select(name) => State::Select(name),
            State::Done => State::Done,
        };
        *budget -= (self.cursor.absolute - before).max(1);
        Ok(())
    }
}
