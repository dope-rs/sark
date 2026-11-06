use core::{num::NonZero, ops::ControlFlow};

use crate::{
    field,
    h1::chunk::{self, ban},
};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
enum Stage {
    #[default]
    LineStart,
    Name,
    Value,
    LineLf,
    FinalLf,
}

/// Name length and field count never exceed the accepted byte count.
/// scan admits the next byte count before either subordinate counter advances.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::h1::chunk) struct State {
    bytes: u32,
    fields: u32,
    name_len: u32,
    candidates: u16,
    stage: Stage,
}

impl State {
    const ALL: u16 = (1 << ban::Field::NAMES.len()) - 1;

    pub(in crate::h1::chunk) const fn new() -> Self {
        Self {
            bytes: 0,
            fields: 0,
            name_len: 0,
            candidates: Self::ALL,
            stage: Stage::LineStart,
        }
    }

    pub(in crate::h1::chunk) fn scan(
        self,
        byte: u8,
        offset: usize,
        limits: &chunk::Limit,
    ) -> Result<ControlFlow<NonZero<u32>, Self>, chunk::Error> {
        let mut next = self;
        let bytes = NonZero::<u32>::MIN
            .checked_add(next.bytes)
            .ok_or(chunk::Error::Overflow {
                axis: chunk::Axis::Trailer,
                value: next.bytes as usize,
                added: 1,
            })?;
        let actual = bytes.get() as usize;
        if actual > limits.trailer_bytes {
            return Err(chunk::Error::Limit {
                axis: chunk::Axis::Trailer,
                actual,
                maximum: limits.trailer_bytes,
            });
        }
        next.bytes = bytes.get();
        let next = match next.stage {
            Stage::LineStart => next.start(byte, offset)?,
            Stage::Name => next.name(byte, offset)?,
            Stage::Value => next.value(byte, offset)?,
            Stage::LineLf => next.line_lf(byte, offset, limits)?,
            Stage::FinalLf => {
                if byte != b'\n' {
                    return Err(chunk::Error::Syntax {
                        offset,
                        syntax: chunk::Syntax::TrailerEnd,
                        actual: byte,
                    });
                }
                return Ok(ControlFlow::Break(bytes));
            }
        };
        Ok(ControlFlow::Continue(next))
    }

    fn start(mut self, byte: u8, offset: usize) -> Result<Self, chunk::Error> {
        if byte == b'\r' {
            self.stage = Stage::FinalLf;
        } else if field::Error::is_name_byte(byte) {
            self.name_len = 1;
            self.candidates = Self::matching(Self::ALL, 0, byte);
            self.stage = Stage::Name;
        } else {
            return Err(chunk::Error::Syntax {
                offset,
                syntax: chunk::Syntax::TrailerStart,
                actual: byte,
            });
        }
        Ok(self)
    }

    fn name(mut self, byte: u8, offset: usize) -> Result<Self, chunk::Error> {
        if byte == b':' {
            if let Some(field) = ban::Field::get(self.candidates, self.name_len as usize) {
                return Err(chunk::Error::Forbidden { offset, field });
            }
            self.stage = Stage::Value;
        } else if field::Error::is_name_byte(byte) {
            let name_len = self.name_len.wrapping_add(1);
            self.candidates = Self::matching(self.candidates, self.name_len as usize, byte);
            self.name_len = name_len;
        } else {
            return Err(chunk::Error::Syntax {
                offset,
                syntax: chunk::Syntax::TrailerName,
                actual: byte,
            });
        }
        Ok(self)
    }

    fn value(mut self, byte: u8, offset: usize) -> Result<Self, chunk::Error> {
        if byte == b'\r' {
            self.stage = Stage::LineLf;
            Ok(self)
        } else if field::Error::is_text(byte) {
            Ok(self)
        } else {
            Err(chunk::Error::Syntax {
                offset,
                syntax: chunk::Syntax::TrailerValue,
                actual: byte,
            })
        }
    }

    fn line_lf(
        mut self,
        byte: u8,
        offset: usize,
        limits: &chunk::Limit,
    ) -> Result<Self, chunk::Error> {
        if byte != b'\n' {
            return Err(chunk::Error::Syntax {
                offset,
                syntax: chunk::Syntax::TrailerLf,
                actual: byte,
            });
        }
        let fields = self.fields.wrapping_add(1);
        let actual = fields as usize;
        if actual > limits.trailer_fields {
            return Err(chunk::Error::Limit {
                axis: chunk::Axis::Fields,
                actual,
                maximum: limits.trailer_fields,
            });
        }
        self.fields = fields;
        self.name_len = 0;
        self.candidates = Self::ALL;
        self.stage = Stage::LineStart;
        Ok(self)
    }

    fn matching(set: u16, index: usize, byte: u8) -> u16 {
        let mut output = 0u16;
        let mut slot = 0usize;
        while slot < ban::Field::NAMES.len() {
            let bit = 1u16 << slot;
            let expected = ban::Field::NAMES[slot].get(index).copied();
            if set & bit != 0
                && expected.is_some_and(|expected| expected.eq_ignore_ascii_case(&byte))
            {
                output |= bit;
            }
            slot += 1;
        }
        output
    }
}
