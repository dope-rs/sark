use core::{num::NonZero, ops};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Order {
        start: usize,
        end: usize,
    },
    Overflow {
        start: usize,
        len: usize,
    },
    Bounds {
        start: usize,
        len: usize,
    },
    Shift {
        start: usize,
        len: usize,
        base: usize,
    },
}

/// Compact byte range into one retained wire owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    start_plus_one: NonZero<u32>,
    len: u32,
}

impl Span {
    pub(crate) const fn from_target(target: super::Target) -> Self {
        Self::pack(target.start(), target.len())
    }

    pub fn checked(start: usize, len: usize) -> Result<Self, Error> {
        let Some(end) = start.checked_add(len) else {
            return Err(Error::Overflow { start, len });
        };
        if start >= u32::MAX as usize || end > u32::MAX as usize {
            return Err(Error::Bounds { start, len });
        }
        Ok(Self::pack(start, len))
    }

    /// Retains a range that begins at the start of its block.
    ///
    /// The request line measures its method before every target it admits, so
    /// the method range is the block prefix and its start needs no proof.
    pub(crate) const fn from_start(len: u32) -> Self {
        Self::pack(0, len as usize)
    }

    pub fn from_range(range: ops::Range<usize>) -> Result<Self, Error> {
        let Some(len) = range.end.checked_sub(range.start) else {
            return Err(Error::Order {
                start: range.start,
                end: range.end,
            });
        };
        if range.start >= u32::MAX as usize || range.end > u32::MAX as usize {
            return Err(Error::Bounds {
                start: range.start,
                len,
            });
        }
        Ok(Self::pack(range.start, len))
    }

    pub const fn start(self) -> usize {
        (self.start_plus_one.get() - 1) as usize
    }

    pub const fn len(self) -> usize {
        self.len as usize
    }

    pub const fn is_empty(self) -> bool {
        self.len == 0
    }

    pub const fn end(self) -> usize {
        self.start() + self.len()
    }

    /// The range moved to a successor start its owner's growth check admitted.
    ///
    /// `start` is the value that check produced, so the move consumes
    /// the bounded start instead of re-deriving the bound that admitted it.
    pub(crate) const fn shifted_to(self, start: NonZero<u32>) -> Self {
        Self {
            start_plus_one: start,
            len: self.len,
        }
    }

    pub fn get(self, input: &[u8]) -> Option<&[u8]> {
        input.get(self.start()..self.end())
    }

    pub fn shifted(self, base: usize) -> Result<Self, Error> {
        let start = self.start();
        let len = self.len();
        let Some(shifted) = base.checked_add(start) else {
            return Err(Error::Shift { start, len, base });
        };
        let Some(end) = shifted.checked_add(len) else {
            return Err(Error::Shift { start, len, base });
        };
        if shifted >= u32::MAX as usize || end > u32::MAX as usize {
            return Err(Error::Shift { start, len, base });
        }
        Ok(Self::pack(shifted, len))
    }

    /// Stores the bounds admitted by the caller without another failure path.
    const fn pack(start: usize, len: usize) -> Self {
        Self {
            start_plus_one: NonZero::<u32>::MIN.saturating_add(start as u32),
            len: len as u32,
        }
    }
}

impl Default for Span {
    fn default() -> Self {
        Self::from_start(0)
    }
}

/// A wire range retained together with the block it was measured in; the parse
/// that measures a range reaches [`Self::try_new`] once and retains both the
/// range and its bytes, so later layers never re-derive either from the owner.
#[derive(Clone, Copy)]
pub(crate) struct Retained<'owner> {
    bytes: &'owner [u8],
    span: Span,
}

impl<'owner> Retained<'owner> {
    pub(crate) fn try_new(owner: &'owner [u8], range: ops::Range<u32>) -> Result<Self, Error> {
        let span = Span::from_range(range.start as usize..range.end as usize)?;
        let start = span.start();
        let bytes = owner.get(start..span.end()).ok_or(Error::Bounds {
            start,
            len: span.len(),
        })?;
        Ok(Self { bytes, span })
    }

    pub(crate) const fn bytes(self) -> &'owner [u8] {
        self.bytes
    }

    pub(crate) const fn span(self) -> Span {
        self.span
    }
}

const _: () = {
    assert!(core::mem::size_of::<Span>() == 8);
    assert!(core::mem::size_of::<Option<Span>>() == 8);
};
