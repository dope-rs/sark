use std::ops;

use crate::{
    Error,
    segment::{Cursor, Field, Order, Projection, State, input::Input},
    source::string::{Admitted, String},
};

/// Replays an admitted name while its enclosing scanner pins the original range.
pub(in crate::segment) struct Name {
    range: ops::Range<usize>,
    cursor: Cursor,
    field: usize,
    matched: usize,
    string: Option<String<Admitted>>,
}

impl Name {
    pub(in crate::segment) fn new(cursor: Cursor, end: usize, escaped: bool, field: usize) -> Self {
        Self {
            range: cursor.absolute..end,
            cursor,
            field,
            matched: 0,
            string: escaped.then(String::new),
        }
    }

    pub(in crate::segment) fn range(&self) -> ops::Range<usize> {
        self.range.clone()
    }

    pub(in crate::segment) fn step(
        mut self,
        input: &Input<'_>,
        fields: &[Field<'_>],
        order: Order,
        next: &mut usize,
        budget: &mut usize,
    ) -> crate::Result<State> {
        while *budget != 0 {
            let Some(field) = fields.get(self.field) else {
                *budget -= 1;
                return match order {
                    Order::Unordered => Ok(State::ValueStart {
                        field: None,
                        projection: Projection::None,
                    }),
                    Order::Ordered => Err(Error::Invalid),
                };
            };
            let expected = field.name;
            let equal = if self.string.is_none() && self.range.len() != expected.len() {
                *budget -= 1;
                false
            } else if self.cursor.absolute == self.range.end {
                *budget -= 1;
                if self.matched == expected.len() {
                    if matches!(order, Order::Ordered) {
                        *next = self.field + 1;
                    }
                    return Ok(State::ValueStart {
                        field: Some(self.field),
                        projection: field.projection,
                    });
                }
                false
            } else if let Some(string) = &mut self.string {
                let (byte, advance) = self.cursor.read(input).ok_or(Error::Invalid)?;
                self.cursor.advance(advance);
                *budget -= 1;
                let output = string.decode(byte);
                let bytes = output.as_slice();
                let end = self.matched + bytes.len();
                let equal = expected.get(self.matched..end) == Some(bytes);
                self.matched = end;
                equal
            } else {
                let chunk = self.cursor.chunk(input).ok_or(Error::Invalid)?;
                let len = chunk
                    .bytes()
                    .len()
                    .min(self.range.end - self.cursor.absolute)
                    .min(*budget);
                let bytes = &chunk.bytes()[..len];
                let end = self.matched + len;
                let equal = expected.get(self.matched..end) == Some(bytes);
                self.cursor.advance(chunk.advance(bytes));
                self.matched = end;
                *budget -= len;
                equal
            };
            if !equal {
                if matches!(order, Order::Ordered) {
                    return Err(Error::Invalid);
                }
                self.field += 1;
                self.matched = 0;
                self.cursor = Cursor::at(input, self.range.start).ok_or(Error::Invalid)?;
                if let Some(string) = &mut self.string {
                    *string = String::new();
                }
            }
        }
        Ok(State::Select(self))
    }
}
