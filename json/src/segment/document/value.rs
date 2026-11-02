use o3::buffer::storage;

use crate::{
    Error, Token,
    segment::{
        Cursor, Projection, Stored, ValueKind,
        input::{Input, Split},
        state,
    },
};

/// One present field borrowed together with its unchanged backing input.
/// Its validated kind and retained range survive every projection through this borrow.
pub struct Value<'a, 'd> {
    input: &'a Input<'d>,
    value: &'a state::Value,
}

impl<'a, 'd> Value<'a, 'd> {
    pub(super) fn new(input: &'a Input<'d>, value: &'a state::Value) -> Self {
        Self { input, value }
    }

    pub fn is_null(&self) -> bool {
        matches!(self.value.kind, ValueKind::Null)
    }

    pub fn u64(&self) -> crate::Result<u64> {
        match self.value.kind {
            ValueKind::Unsigned(value) => Ok(value),
            ValueKind::String { escapes: 0.. }
            | ValueKind::Number
            | ValueKind::True
            | ValueKind::False
            | ValueKind::Null
            | ValueKind::Compound => Err(Error::Invalid),
        }
    }

    pub fn boolean(&self) -> crate::Result<bool> {
        match self.value.kind {
            ValueKind::True => Ok(true),
            ValueKind::False => Ok(false),
            ValueKind::String { escapes: 0.. }
            | ValueKind::Unsigned(0..)
            | ValueKind::Number
            | ValueKind::Null
            | ValueKind::Compound => Err(Error::Invalid),
        }
    }

    pub fn frame(&self) -> crate::Result<crate::Text<storage::Shared>> {
        let value = self.bytes_value()?;
        let shared = match value.kind {
            ValueKind::String { escapes: 0 } => self.materialize(value, 1, 1),
            ValueKind::String { escapes: 1.. } => {
                let decoded = value.decoded.as_ref().ok_or(Error::Invalid)?;
                let decoded = decoded.try_clone().map_err(Error::Retain)?;
                Ok(decoded)
            }
            ValueKind::Unsigned(0..)
            | ValueKind::Number
            | ValueKind::True
            | ValueKind::False
            | ValueKind::Null
            | ValueKind::Compound => self.materialize(value, 0, 0),
        };
        shared.map(crate::Text::admitted)
    }

    pub fn plain(&self) -> crate::Result<crate::Plain<storage::Shared>> {
        let value = self.bytes_value()?;
        let shared = match value.kind {
            ValueKind::String { escapes: 0 } => self.materialize(value, 1, 1),
            ValueKind::String { escapes: 1.. }
            | ValueKind::Unsigned(0..)
            | ValueKind::Number
            | ValueKind::True
            | ValueKind::False
            | ValueKind::Null
            | ValueKind::Compound => Err(Error::Invalid),
        };
        shared.map(crate::Plain::admitted)
    }

    pub fn raw(&self) -> crate::Result<crate::Raw<storage::Shared>> {
        self.materialize(self.original()?, 0, 0)
            .map(crate::Raw::admitted)
    }

    pub fn inline_plain<const N: usize>(&self) -> crate::Result<crate::Plain<Token<N>>> {
        let value = self.original()?;
        let token = match value.kind {
            ValueKind::String { escapes: 0 } => self.inline(value, 1, 1),
            ValueKind::String { escapes: 1.. }
            | ValueKind::Unsigned(0..)
            | ValueKind::Number
            | ValueKind::True
            | ValueKind::False
            | ValueKind::Null
            | ValueKind::Compound => Err(Error::Invalid),
        };
        token.map(crate::Plain::admitted)
    }

    pub fn inline_raw<const N: usize>(&self) -> crate::Result<crate::Raw<Token<N>>> {
        self.inline(self.original()?, 0, 0)
            .map(crate::Raw::admitted)
    }

    pub(super) fn split(&self) -> crate::Result<Split<'d>> {
        let value = self.original()?;
        if value.projection != Projection::Segments || !matches!(value.kind, ValueKind::Compound) {
            return Err(Error::Invalid);
        }
        let Stored::Original(range) = &value.stored else {
            return Err(Error::Invalid);
        };
        Split::new(self.input, range.clone())
    }

    fn bytes_value(&self) -> crate::Result<&state::Value> {
        match self.value.projection {
            Projection::Bytes | Projection::Frame | Projection::Segments => Ok(self.value),
            Projection::None | Projection::Scalar => Err(Error::Invalid),
        }
    }

    fn original(&self) -> crate::Result<&state::Value> {
        let value = self.bytes_value()?;
        if value.decoded.is_some() {
            return Err(Error::Invalid);
        }
        Ok(value)
    }

    fn materialize(
        &self,
        value: &state::Value,
        head: usize,
        tail: usize,
    ) -> crate::Result<storage::Shared> {
        match &value.stored {
            Stored::Original(range) => self.input.project(range.start + head..range.end - tail),
            Stored::Copied(shared) => shared
                .try_get(head..shared.len() - tail)
                .map_err(Error::Retain)?
                .ok_or(Error::Invalid),
        }
    }

    fn inline<const N: usize>(
        &self,
        value: &state::Value,
        head: usize,
        tail: usize,
    ) -> crate::Result<Token<N>> {
        const { assert!(N <= u8::MAX as usize) }
        let mut token = Token::new();
        match &value.stored {
            Stored::Original(range) => {
                let start = range.start + head;
                let end = range.end - tail;
                let mut cursor = Cursor::at(self.input, start).ok_or(Error::Invalid)?;
                while cursor.absolute < end {
                    let (byte, advance) = cursor.read(self.input).ok_or(Error::Invalid)?;
                    token.push(byte)?;
                    cursor.advance(advance);
                }
            }
            Stored::Copied(shared) => {
                let end = shared.len() - tail;
                for byte in shared.as_slice().get(head..end).ok_or(Error::Invalid)? {
                    token.push(*byte)?;
                }
            }
        }
        Ok(token)
    }
}
