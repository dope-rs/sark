use std::num::NonZero;

use crate::{
    field::scan,
    h1::field::{Entry, Limit, Progress, syntax::Error},
    request::span,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Name,
    Value { raw: u32 },
    ValueLf { raw: u32 },
    EndLf,
}

/// Owner-free progress through one growing field block.
#[derive(Clone, Copy)]
pub(in crate::h1) struct Cursor {
    line: u32,
    cursor: u32,
    seen: u32,
    limit: Limit,
    phase: Phase,
}

impl Cursor {
    pub(super) fn seed(start: usize, seen: usize, limit: Limit) -> Result<Self, Error> {
        let start = Self::try_narrow(start)?;
        let seen = Self::try_narrow(seen)?;
        if seen > limit.count {
            return Err(Error::TooMany);
        }
        Ok(Self {
            seen,
            ..Self::new(start, limit)
        })
    }

    pub(in crate::h1) const fn new(start: u32, limit: Limit) -> Self {
        Self {
            line: start,
            cursor: start,
            seen: 0,
            limit,
            phase: Phase::Name,
        }
    }

    #[inline(always)]
    pub(in crate::h1) fn advance<'owner>(
        &mut self,
        bytes: &'owner [u8],
    ) -> Result<Progress<'owner>, Error> {
        match self.phase {
            Phase::Name => {
                let cursor = self.cursor as usize;
                let Some(byte) = bytes.get(cursor) else {
                    return Ok(Progress::More);
                };
                if self.cursor == self.line && *byte == b'\r' {
                    return self.ending(bytes);
                }
                if self.cursor == self.line && matches!(byte, b' ' | b'\t') {
                    return Err(Error::Name {
                        offset: cursor,
                        actual: *byte,
                    });
                }
                match scan::Bytes::new(bytes).name(cursor) {
                    scan::Outcome::Found((colon, b':')) if colon != self.line as usize => {
                        let raw = Self::try_narrow(colon)?
                            .checked_add(1)
                            .ok_or(Error::TooLarge)?;
                        self.value(bytes, raw, raw)
                    }
                    scan::Outcome::Found((offset, actual)) => Err(Error::Name { offset, actual }),
                    scan::Outcome::Invalid { offset, actual } => {
                        Err(Error::Name { offset, actual })
                    }
                    scan::Outcome::Partial => {
                        let next = Self {
                            cursor: Self::try_narrow(bytes.len())?,
                            ..*self
                        };
                        let progress = next.incomplete()?;
                        *self = next;
                        Ok(progress)
                    }
                }
            }
            Phase::Value { raw } => self.value(bytes, raw, self.cursor),
            Phase::ValueLf { raw } => {
                let lf = self.cursor.checked_add(1).ok_or(Error::TooLarge)?;
                let Some(byte) = bytes.get(lf as usize) else {
                    let progress = self.incomplete()?;
                    return Ok(progress);
                };
                if *byte != b'\n' {
                    return Err(Error::Value {
                        offset: lf as usize,
                        actual: *byte,
                    });
                }
                let field = self.admit(bytes, raw, self.cursor)?;
                Ok(Progress::Field(field))
            }
            Phase::EndLf => self.ending(bytes),
        }
    }

    fn value<'owner>(
        &mut self,
        owner: &'owner [u8],
        raw: u32,
        cursor: u32,
    ) -> Result<Progress<'owner>, Error> {
        let bytes = owner;
        match scan::Bytes::new(bytes).value(cursor as usize) {
            scan::Outcome::Found(end) => {
                let field = self.admit(owner, raw, Self::try_narrow(end)?)?;
                Ok(Progress::Field(field))
            }
            scan::Outcome::Invalid { offset, actual } => Err(Error::Value { offset, actual }),
            scan::Outcome::Partial => {
                let next = if bytes.last() == Some(&b'\r') {
                    let end = Self::try_narrow(bytes.len() - 1)?;
                    Self {
                        cursor: end,
                        phase: Phase::ValueLf { raw },
                        ..*self
                    }
                } else {
                    Self {
                        cursor: Self::try_narrow(bytes.len())?,
                        phase: Phase::Value { raw },
                        ..*self
                    }
                };
                let progress = next.incomplete()?;
                *self = next;
                Ok(progress)
            }
        }
    }

    fn ending<'owner>(&mut self, bytes: &'owner [u8]) -> Result<Progress<'owner>, Error> {
        let lf = self.cursor.checked_add(1).ok_or(Error::TooLarge)?;
        let Some(byte) = bytes.get(lf as usize) else {
            let next = Self {
                phase: Phase::EndLf,
                ..*self
            };
            let progress = next.incomplete()?;
            *self = next;
            return Ok(progress);
        };
        if *byte != b'\n' {
            return Err(Error::Ending {
                offset: lf as usize,
                actual: *byte,
            });
        }
        let wire = NonZero::<u32>::MIN.checked_add(lf).ok_or(Error::TooLarge)?;
        Ok(Progress::End(End::new(self.cursor, wire)))
    }

    fn admit<'owner>(
        &mut self,
        owner: &'owner [u8],
        raw: u32,
        end: u32,
    ) -> Result<Entry<'owner>, Error> {
        let next = end.checked_add(2).ok_or(Error::TooLarge)?;
        let width = end - self.line;
        if width > self.limit.line_bytes {
            return Err(Error::TooLong);
        }
        if self.seen == self.limit.count {
            return Err(Error::TooMany);
        }
        let value = Self::retain(owner, raw, end)?;
        let trimmed = value.bytes().trim_ascii_start();
        let start = raw + (value.bytes().len() - trimmed.len()) as u32;
        let end = start + crate::field::Error::trim_ows(trimmed).len() as u32;
        let name_end = raw - 1;
        let field = Entry::new(
            Self::retain(owner, self.line, name_end)?,
            Self::retain(owner, start, end)?,
            span::Span::from_range(self.line as usize..next as usize)
                .map_err(|_| Error::Invalid)?,
        );
        *self = Self {
            line: next,
            cursor: next,
            seen: self.seen + 1,
            limit: self.limit,
            phase: Phase::Name,
        };
        Ok(field)
    }

    /// Retains a range this scan measured inside the block it walks.
    fn retain<'owner>(
        owner: &'owner [u8],
        start: u32,
        end: u32,
    ) -> Result<span::Retained<'owner>, Error> {
        span::Retained::try_new(owner, start..end).map_err(|_| Error::Invalid)
    }

    fn incomplete<'owner>(&self) -> Result<Progress<'owner>, Error> {
        let width = self.cursor - self.line;
        if width > self.limit.line_bytes {
            return Err(Error::TooLong);
        }
        Ok(Progress::More)
    }

    fn try_narrow(value: usize) -> Result<u32, Error> {
        if value > u32::MAX as usize {
            Err(Error::TooLarge)
        } else {
            Ok(value as u32)
        }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Cursor>() == 28);
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::h1) struct End {
    fields: u32,
    wire: NonZero<u32>,
}

impl End {
    pub(super) const fn new(fields: u32, wire: NonZero<u32>) -> Self {
        Self { fields, wire }
    }

    pub(in crate::h1) const fn fields(self) -> u32 {
        self.fields
    }

    pub(in crate::h1) const fn wire_len(self) -> NonZero<u32> {
        self.wire
    }
}

const _: () = {
    assert!(core::mem::size_of::<End>() == 8);
    assert!(core::mem::size_of::<Option<End>>() == 8);
};
