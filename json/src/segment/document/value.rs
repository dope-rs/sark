use o3::buffer::storage;

use crate::{
    Error, Token,
    segment::{
        Cursor, Stored, ValueKind,
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
            _ => Err(Error::Invalid),
        }
    }

    pub fn boolean(&self) -> crate::Result<bool> {
        match self.value.kind {
            ValueKind::True => Ok(true),
            ValueKind::False => Ok(false),
            _ => Err(Error::Invalid),
        }
    }

    pub fn frame(&self) -> crate::Result<crate::Text<storage::Shared>> {
        let shared = match self.value.kind {
            ValueKind::String { escapes: 0 } => self.materialize(1, 1),
            ValueKind::String { escapes: 1.. } => match &self.value.stored {
                Stored::Decoded(decoded) => decoded.try_clone().map_err(Error::Retain),
                _ => Err(Error::Invalid),
            },
            _ => Err(Error::Invalid),
        };
        shared.map(crate::Text::admitted)
    }

    pub fn plain(&self) -> crate::Result<crate::Plain<storage::Shared>> {
        let shared = match self.value.kind {
            ValueKind::String { escapes: 0 } => self.materialize(1, 1),
            _ => Err(Error::Invalid),
        };
        shared.map(crate::Plain::admitted)
    }

    pub fn raw(&self) -> crate::Result<crate::Raw<storage::Shared>> {
        self.materialize(0, 0).map(crate::Raw::admitted)
    }

    pub fn inline_plain<const N: usize>(&self) -> crate::Result<crate::Plain<Token<N>>> {
        let token = match self.value.kind {
            ValueKind::String { escapes: 0 } => self.inline(1, 1),
            _ => Err(Error::Invalid),
        };
        token.map(crate::Plain::admitted)
    }

    pub fn inline_raw<const N: usize>(&self) -> crate::Result<crate::Raw<Token<N>>> {
        self.inline(0, 0).map(crate::Raw::admitted)
    }

    pub(super) fn split(&self) -> crate::Result<Split<'d>> {
        let Stored::Segments(range) = &self.value.stored else {
            return Err(Error::Invalid);
        };
        if !matches!(self.value.kind, ValueKind::Compound) {
            return Err(Error::Invalid);
        }
        Split::new(self.input, range.clone())
    }

    fn materialize(&self, head: usize, tail: usize) -> crate::Result<storage::Shared> {
        match &self.value.stored {
            Stored::Bytes(range) | Stored::Frame(range) | Stored::Segments(range) => {
                self.input.project(range.start + head..range.end - tail)
            }
            Stored::Copied(shared) => shared
                .try_get(head..shared.len() - tail)
                .map_err(Error::Retain)?
                .ok_or(Error::Invalid),
            _ => Err(Error::Invalid),
        }
    }

    fn inline<const N: usize>(&self, head: usize, tail: usize) -> crate::Result<Token<N>> {
        const { assert!(N <= u8::MAX as usize) }
        match &self.value.stored {
            Stored::Bytes(range) | Stored::Frame(range) | Stored::Segments(range) => {
                let mut token = Token::new();
                let start = range.start + head;
                let end = range.end - tail;
                let mut cursor = Cursor::at(self.input, start).ok_or(Error::Invalid)?;
                while cursor.absolute < end {
                    let (byte, advance) = cursor.read(self.input).ok_or(Error::Invalid)?;
                    token.push(byte)?;
                    cursor.advance(advance);
                }
                Ok(token)
            }
            Stored::Copied(shared) => {
                let mut token = Token::new();
                let end = shared.len() - tail;
                for byte in shared.as_slice().get(head..end).ok_or(Error::Invalid)? {
                    token.push(*byte)?;
                }
                Ok(token)
            }
            _ => Err(Error::Invalid),
        }
    }
}
