use std::mem;

use o3::alloc::{self, array};

use crate::{
    Error, Nesting,
    input::Taken,
    segment::{
        Atom, Capture, Cursor, Field, Order, Projection, State, ValueKind, input::Input,
        is_delimiter, state, state::Name,
    },
    source::string,
};

pub(in crate::segment) struct Scanning<'d> {
    pub(super) input: Input<'d>,
    pub(super) cursor: Cursor,
    pub(super) state: State,
    pub(super) selection: Selection<'d>,
    pub(in crate::segment) stream: super::Stream,
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

impl<'d> Scanning<'d> {
    pub(super) fn new(input: Input<'d>, selection: Selection<'d>) -> Self {
        Self {
            input,
            cursor: Cursor::new(),
            state: State::Leading,
            selection,
            stream: super::Stream::new(),
        }
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
                if self.cursor.take(&self.input, b'{') == Taken::No {
                    return Err(Error::Invalid);
                }
                State::NameOrEnd
            }
            State::NameOrEnd => {
                if self.cursor.take(&self.input, b'}') == Taken::Yes {
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
                if self.cursor.take(&self.input, b'"') == Taken::No {
                    return Err(Error::Invalid);
                }
                State::Name {
                    start: self.cursor.absolute,
                    string: string::Scan::new(self.input.is_admitted()),
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
            State::ValueStart {
                field,
                projection: mode,
            } => {
                let (byte, advance) = self.cursor.read(&self.input).ok_or(Error::Invalid)?;
                let field = field.filter(|&index| {
                    !self
                        .selection
                        .values
                        .get(index)
                        .is_some_and(Option::is_some)
                });
                let mode = if mode == Projection::Frame && byte != b'"' {
                    Projection::Scalar
                } else {
                    mode
                };
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
                if self.cursor.absolute == self.input.len() {
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
