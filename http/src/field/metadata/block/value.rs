use core::convert::Infallible;

use o3::buffer::write;

use crate::field::metadata::{block, map::record};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Empty,
    Text,
    Whitespace,
    Invalid,
}

/// Constant-space validation of a decoded compressed field value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Value(State);

impl Value {
    pub const fn new() -> Self {
        Self(State::Empty)
    }

    pub const fn push(&mut self, byte: u8) {
        if matches!(self.0, State::Invalid) {
            return;
        }
        self.0 = if !crate::field::Error::is_text(byte) {
            State::Invalid
        } else if matches!(byte, b' ' | b'\t') {
            match self.0 {
                State::Empty => State::Invalid,
                State::Text | State::Whitespace => State::Whitespace,
                State::Invalid => State::Invalid,
            }
        } else {
            State::Text
        };
    }

    pub fn extend(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.push(byte);
        }
    }

    pub const fn is_valid(self) -> bool {
        matches!(self.0, State::Empty | State::Text)
    }

    pub const fn finish(self) -> block::Validation {
        if self.is_valid() {
            block::Validation::Valid
        } else {
            block::Validation::Malformed
        }
    }

    pub fn from_span(value: record::Span<'_>) -> Self {
        let mut scan = Self::new();
        let (first, second) = value.parts();
        scan.extend(first);
        scan.extend(second);
        scan
    }
}

impl Default for Value {
    fn default() -> Self {
        Self::new()
    }
}

impl write::Sink for Value {
    type Error = Infallible;

    fn write_byte(&mut self, byte: u8) -> Result<(), Self::Error> {
        self.push(byte);
        Ok(())
    }

    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.extend(bytes);
        Ok(())
    }

    fn write_slices<const N: usize>(&mut self, slices: [&[u8]; N]) -> Result<(), Self::Error> {
        for bytes in slices {
            self.extend(bytes);
        }
        Ok(())
    }
}

const _: () = assert!(size_of::<Value>() == 1);
