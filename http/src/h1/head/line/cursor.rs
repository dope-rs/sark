use line::error;

use crate::{
    field::scan::Match,
    h1::head::{Boundary, line},
};

/// Owner-free progress to the end of one growing start line.
#[derive(Clone, Copy, Default)]
#[repr(transparent)]
pub(in crate::h1::head) struct Cursor(u32);

impl Cursor {
    pub(in crate::h1::head) fn pending(bytes: &[u8]) -> Result<Self, error::Request> {
        match Match::find(b'\r', bytes) {
            None => Ok(Self::at(bytes.len())),
            Some(end) => {
                if end.suffix().is_empty() {
                    Ok(Self::at(end.start()))
                } else {
                    Err(error::Request::Syntax)
                }
            }
        }
    }

    pub(in crate::h1::head) fn at(position: usize) -> Self {
        if position >= u32::MAX as usize {
            Self(u32::MAX)
        } else {
            Self(position as u32)
        }
    }

    pub(in crate::h1::head) fn scan(
        self,
        owner: &[u8],
        limit: usize,
    ) -> Result<(Self, Option<&[u8]>), Boundary> {
        if self.0 == u32::MAX {
            return Err(Boundary::TooLarge {
                actual: owner.len(),
                limit: u32::MAX as usize - 1,
            });
        }
        let start = self.0 as usize;
        let Some(tail) = owner.get(start..) else {
            return Err(Boundary::Invalid);
        };
        match Match::find(b'\r', tail) {
            None => {
                if owner.len() > limit {
                    return Err(Boundary::TooLarge {
                        actual: owner.len(),
                        limit,
                    });
                }
                Ok((Self(Self::narrow(owner.len())?), None))
            }
            Some(found) => {
                let end = start + found.start();
                let line_end = end + 2;
                if line_end > limit {
                    return Err(Boundary::TooLarge {
                        actual: line_end,
                        limit,
                    });
                }
                match found.suffix().first() {
                    None => Ok((Self(Self::narrow(end)?), None)),
                    Some(lf) => {
                        if *lf != b'\n' {
                            return Err(Boundary::Invalid);
                        }
                        let prefix = owner.get(..line_end).ok_or(Boundary::Invalid)?;
                        Ok((self, Some(prefix)))
                    }
                }
            }
        }
    }

    fn narrow(value: usize) -> Result<u32, Boundary> {
        if value >= u32::MAX as usize {
            return Err(Boundary::TooLarge {
                actual: value,
                limit: u32::MAX as usize - 1,
            });
        }
        Ok(value as u32)
    }
}

const _: () = assert!(core::mem::size_of::<Cursor>() == 4);
