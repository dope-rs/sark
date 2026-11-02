//! Request target capture access.

use std::{mem, ops};

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Order {
        start: usize,
        end: usize,
        capacity: usize,
    },
    Bounds {
        start: usize,
        end: usize,
        capacity: usize,
    },
}

pub struct Target<'a> {
    raw: &'a [u8],
}

/// An admitted path slice retaining its offset within the route path.
pub struct Capture<'a> {
    value: &'a [u8],
    start: usize,
}

impl<'a> Capture<'a> {
    pub(in crate::request) const fn new(value: &'a [u8], start: usize) -> Self {
        Self { value, start }
    }

    pub const fn as_bytes(&self) -> &'a [u8] {
        self.value
    }

    pub const fn range(&self) -> ops::Range<usize> {
        self.start..self.start + self.value.len()
    }
}

impl<'a> Target<'a> {
    pub const fn new(raw: &'a [u8]) -> Self {
        Self { raw }
    }

    pub fn get(&self, range: ops::Range<usize>) -> Result<&'a [u8], Error> {
        let start = range.start;
        let end = range.end;
        let capacity = self.raw.len();
        if start > end {
            return Err(Error::Order {
                start,
                end,
                capacity,
            });
        }
        self.raw.get(start..end).ok_or(Error::Bounds {
            start,
            end,
            capacity,
        })
    }

    pub fn try_capture(&self, range: ops::Range<usize>) -> Result<Capture<'a>, Error> {
        let start = range.start;
        Ok(Capture::new(self.get(range)?, start))
    }
}

const _: () = {
    assert!(mem::size_of::<Target<'_>>() == mem::size_of::<&[u8]>());
    assert!(mem::size_of::<Capture<'_>>() == 3 * mem::size_of::<usize>());
    assert!(mem::size_of::<Error>() == mem::size_of::<usize>() * 4);
    assert!(!mem::needs_drop::<Error>());
};
