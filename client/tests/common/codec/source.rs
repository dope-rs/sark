use std::ops;

use o3::buffer::{self, hold, resident, storage::range};

/// Consumes a borrowed fixture without cloning its owner or changing retention.
pub struct Source<'a, R> {
    owner: &'a R,
    offset: usize,
}

impl<'a, R> Source<'a, R> {
    pub(super) const fn new(owner: &'a R) -> Self {
        Self { owner, offset: 0 }
    }
}

impl<R: AsRef<[u8]>> AsRef<[u8]> for Source<'_, R> {
    fn as_ref(&self) -> &[u8] {
        &self.owner.as_ref()[self.offset..]
    }
}

impl<R: buffer::Cursor> buffer::Source for Source<'_, R> {
    type View<'a>
        = R::View<'a>
    where
        Self: 'a;

    fn remaining(&self) -> usize {
        self.as_ref().len()
    }
}

impl<R: buffer::Cursor> buffer::Cursor for Source<'_, R> {
    fn try_get<'a>(&'a self, range: ops::Range<usize>) -> Result<Self::View<'a>, range::Error> {
        if range.start > range.end || range.end > self.as_ref().len() {
            return Err(range::Error::new(range, self.as_ref().len()));
        }
        self.owner
            .try_get(self.offset + range.start..self.offset + range.end)
    }
}

// SAFETY: Selection offsets and retention preserve the original proven source view.
unsafe impl<'d, R: buffer::Cursor + hold::raw::Retain<'d>> hold::raw::Retain<'d> for Source<'_, R> {
    fn charge<'a>(plan: &Self::View<'a>) -> hold::Charge
    where
        Self: 'a,
    {
        R::charge(plan)
    }

    fn try_retain<'a>(
        plan: &Self::View<'a>,
        budget: &resident::Budget<'d>,
    ) -> Result<hold::Retained<'d>, hold::Error>
    where
        Self: 'a,
    {
        R::try_retain(plan, budget)
    }
}

// SAFETY: the immutable source preserves its bytes; only the proved prefix advances.
unsafe impl<R: buffer::raw::Source> buffer::raw::Consume for Source<'_, R> {
    fn consume<const MAX: u32>(prefix: buffer::raw::Consumed<'_, '_, Self, MAX>) {
        let (source, end) = prefix.into_parts();
        source.offset += end.count().get() as usize;
    }
}
