use core::mem;

use crate::h1::chunk::{Axis, Error};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Line(u32);

impl Line {
    pub(super) const fn new() -> Self {
        Self(0)
    }

    pub(super) fn try_add(self, maximum: usize) -> Result<Self, Error> {
        let value = self.0 as usize;
        let next = self.0.checked_add(1).ok_or(Error::Overflow {
            axis: Axis::Line,
            value,
            added: 1,
        })?;
        let actual = next as usize;
        if actual > maximum {
            return Err(Error::Limit {
                axis: Axis::Line,
                actual,
                maximum,
            });
        }
        Ok(Self(next))
    }
}

const _: () = assert!(mem::size_of::<usize>() >= mem::size_of::<u32>());
