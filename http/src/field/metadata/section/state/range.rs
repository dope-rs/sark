use core::{num::NonZero, ops};

#[derive(Clone, Copy)]
pub(super) struct Authority(Option<Range>);

pub(super) enum Source {
    Absent,
    Retained(Range),
    Staged(Range),
}

pub(super) enum Stage {
    Absent,
    Retained,
    Staged(Range),
}

#[derive(Clone, Copy)]
pub(super) struct Range {
    first: u32,
    second: NonZero<u32>,
}

impl Authority {
    pub(super) const fn absent() -> Self {
        Self(None)
    }

    pub(super) fn retained(range: ops::Range<usize>) -> Option<Self> {
        Some(Self(Some(Range::retained(range)?)))
    }

    pub(super) fn staged(range: ops::Range<usize>) -> Option<Self> {
        Some(Self(Some(Range::staged(range)?)))
    }

    pub(super) fn source(self) -> Source {
        match self.0 {
            None => Source::Absent,
            Some(saved) => match saved.is_staged() {
                true => Source::Staged(saved),
                false => Source::Retained(saved),
            },
        }
    }

    pub(super) fn stage(self) -> Stage {
        match self.0 {
            None => Stage::Absent,
            Some(saved) => match saved.is_staged() {
                true => Stage::Staged(saved),
                false => Stage::Retained,
            },
        }
    }
}

impl Range {
    fn retained(range: ops::Range<usize>) -> Option<Self> {
        if range.start >= range.end || range.end > u32::MAX as usize {
            return None;
        }
        Some(Self {
            first: range.start as u32,
            second: NonZero::<u32>::new(range.end as u32)?,
        })
    }

    fn staged(range: ops::Range<usize>) -> Option<Self> {
        let retained = Self::retained(range)?;
        Some(Self {
            first: retained.second.get(),
            second: NonZero::<u32>::MIN.saturating_add(retained.first),
        })
    }

    pub(super) const fn is_staged(self) -> bool {
        self.first >= self.second.get()
    }

    pub(super) fn range(self) -> ops::Range<usize> {
        let second = self.second.get();
        if self.first < second {
            self.first as usize..second as usize
        } else {
            second as usize - 1..self.first as usize
        }
    }
}

const _: () = assert!(core::mem::size_of::<Authority>() == 2 * core::mem::size_of::<u32>());
