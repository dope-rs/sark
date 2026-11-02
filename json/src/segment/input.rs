use std::{mem, num::NonZero, ops, task::Poll};

use o3::{
    alloc::{self, VecExt as _},
    buffer::{resident, segments, storage},
};
use sark_http::payload::{owned, response::Body as _};

use crate::{Error, segment::Transfer};

const _: () = assert!(usize::BITS >= u32::BITS);

pub(super) struct Input<'d> {
    segments: Vec<Part<'d>>,
    len: usize,
    admitted: bool,
}

/// A bounded transfer of an admitted range into its final owner table.
pub(super) struct Split<'d> {
    range: ops::Range<usize>,
    next: usize,
    end: usize,
    output: Input<'d>,
}

impl<'d> Split<'d> {
    pub(super) fn release(&mut self, budget: &mut usize) -> Poll<()> {
        self.output.release(budget).0
    }
    /// The caller retains this input unchanged and has validated the complete range.
    pub(super) fn new(source: &Input<'d>, range: ops::Range<usize>) -> crate::Result<Self> {
        let parts = source.indices(&range)?;
        let output = Input {
            segments: Vec::try_new(parts.len()).map_err(Error::Allocation)?,
            len: range.len(),
            admitted: true,
        };
        Ok(Self {
            range,
            next: parts.start,
            end: parts.end,
            output,
        })
    }

    pub(super) fn step(
        &mut self,
        source: &Input<'d>,
        budget: &mut usize,
    ) -> crate::Result<Option<Input<'d>>> {
        while self.next != self.end && *budget != 0 {
            // SAFETY: new admitted next..end in this same table, and Nested/Project
            // keep it unchanged until the transfer finishes.
            let part = unsafe { source.segments.get_unchecked(self.next) };
            let start = self.range.start.max(part.start);
            let end = self.range.end.min(part.end);
            let owner = part
                .owner
                .get(start - part.start..end - part.start)
                .ok_or(Error::Invalid)?
                .try_retain()
                .map_err(Error::Retain)?;
            // SAFETY: new reserved end - next slots; each successful step consumes one.
            unsafe { alloc::raw::VecExt::vacancy(&mut self.output.segments) }.insert(Part {
                owner,
                start: start - self.range.start,
                end: end - self.range.start,
            });
            self.next += 1;
            *budget -= 1;
        }
        Ok((self.next == self.end).then(|| mem::replace(&mut self.output, Input::empty())))
    }
}

pub(super) struct Part<'d> {
    owner: resident::Shared<'d>,
    start: usize,
    end: usize,
}

impl<'d> Part<'d> {
    pub(super) fn is_empty(&self) -> bool {
        self.owner.is_empty()
    }
    /// Byte range this segment covers; contiguous with its neighbours.
    pub(super) fn range(&self) -> ops::Range<usize> {
        self.start..self.end
    }

    /// Takes this segment's bytes, leaving it empty. A later walk of the same
    /// segment reports nothing, so releasing twice cannot double count.
    pub(super) fn take(&mut self) -> usize {
        mem::take(&mut self.owner).len()
    }
}

pub(super) enum Failure<'d> {
    Length {
        len: usize,
        add: usize,
        owner: resident::Shared<'d>,
    },
    Segments {
        current: segments::Count,
        add: segments::Count,
        owner: resident::Shared<'d>,
    },
    Allocation {
        error: alloc::Error,
        owner: resident::Shared<'d>,
    },
}

impl<'d> Input<'d> {
    pub(super) const fn empty() -> Self {
        Self {
            segments: Vec::new(),
            len: 0,
            admitted: false,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn count(&self) -> usize {
        self.segments.len()
    }

    /// Retires at most one table entry per unit after parsing and projection
    /// have finished. The remaining table is never used as input again.
    pub(super) fn release(&mut self, budget: &mut usize) -> (Poll<()>, super::scanner::Released) {
        let mut released = super::scanner::Released::new();
        self.admitted = false;
        while *budget != 0 {
            let Some(part) = self.segments.pop() else {
                return (Poll::Ready(()), released);
            };
            released.record(part.owner.len());
            self.len = part.start;
            *budget -= 1;
        }
        let done = if self.segments.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        };
        (done, released)
    }

    pub(super) const fn is_admitted(&self) -> bool {
        self.admitted
    }

    pub(super) fn new<I, O>(owners: I) -> crate::Result<Self>
    where
        I: IntoIterator<Item = O>,
        I::IntoIter: ExactSizeIterator,
        resident::Shared<'d>: From<O>,
    {
        let owners = owners.into_iter();
        let mut len = 0usize;
        let mut segments = Vec::try_new(owners.len()).map_err(Error::Allocation)?;
        for owner in owners {
            let owner = resident::Shared::from(owner);
            if owner.is_empty() {
                continue;
            }
            if segments.len() == u32::MAX as usize {
                return Err(Error::InputSegments {
                    current: segments::Count::new(u32::MAX),
                    add: segments::Count::ONE,
                });
            }
            let start = len;
            let add = owner.len();
            len = len
                .checked_add(add)
                .ok_or(Error::InputLength { current: len, add })?;
            let slot = alloc::Vacant::try_new(&mut segments).map_err(Error::Allocation)?;
            slot.insert(Part {
                owner,
                start,
                end: len,
            });
        }
        Ok(Self {
            segments,
            len,
            admitted: false,
        })
    }

    pub(super) fn try_new(owners: owned::Segments<'d>) -> Result<Self, alloc::Error> {
        let capacity = owners.count().into_raw() as usize;
        let mut segments: Vec<Part<'d>> = alloc::VecExt::try_new(capacity)?;
        let len = owners.len();
        let mut owners = owners.into_iter();
        let mut start = 0usize;
        while let Some(owner) = owners.next() {
            let owner = resident::Shared::from(owner);
            let end = len - owners.byte_len();
            // SAFETY: this sealed owner iterator yields exactly the reserved count.
            unsafe { alloc::raw::VecExt::vacancy(&mut segments) }.insert(Part {
                owner,
                start,
                end,
            });
            start = end;
        }
        Ok(Self {
            segments,
            len,
            admitted: false,
        })
    }

    pub(super) fn push(mut self, owner: resident::Shared<'d>) -> Transfer<Self, Failure<'d>> {
        if owner.is_empty() {
            return Transfer::Accepted(self);
        }
        let start = self.len;
        let add = owner.len();
        let Some(end) = self.len.checked_add(add) else {
            return Transfer::Rejected {
                state: self,
                error: Failure::Length {
                    len: start,
                    add,
                    owner,
                },
            };
        };
        if self.segments.len() == u32::MAX as usize {
            return Transfer::Rejected {
                state: self,
                error: Failure::Segments {
                    current: segments::Count::new(u32::MAX),
                    add: segments::Count::ONE,
                    owner,
                },
            };
        }
        match alloc::Vacant::try_new(&mut self.segments) {
            Ok(slot) => {
                self.len = end;
                slot.insert(Part { owner, start, end });
                Transfer::Accepted(self)
            }
            Err(error) => Transfer::Rejected {
                state: self,
                error: Failure::Allocation { error, owner },
            },
        }
    }

    /// Segments at and after `released`. Walking the table supplies each
    /// index, so every position the iterator yields exists in it.
    pub(super) fn tail(
        &mut self,
        released: segments::Count,
    ) -> impl Iterator<Item = &mut Part<'d>> + '_ {
        self.segments.iter_mut().skip(released.into_raw() as usize)
    }

    fn indices(&self, range: &ops::Range<usize>) -> crate::Result<ops::Range<usize>> {
        if range.start > range.end || range.end > self.len {
            return Err(Error::Invalid);
        }
        if range.is_empty() {
            return Ok(0..0);
        }
        let start = self
            .segments
            .partition_point(|part| part.end <= range.start);
        let end = self.segments.partition_point(|part| part.start < range.end);
        Ok(start..end)
    }

    fn parts(&self, range: &ops::Range<usize>) -> crate::Result<&[Part<'d>]> {
        let indices = self.indices(range)?;
        // SAFETY: indices is the ordered in-bounds range from this same table.
        Ok(unsafe { self.segments.get_unchecked(indices) })
    }

    /// Independent single-owner projections can be retained directly. All
    /// other projections must finish their budgeted copy before materializing.
    /// Retaining an independent projection preserves its detachability.
    pub(super) fn needs_copy(&self, range: &ops::Range<usize>) -> crate::Result<bool> {
        Ok(match self.parts(range)? {
            [] => false,
            [part] => matches!(part.owner.scope(), resident::Scope::Bound(_)),
            [_, _, ..] => true,
        })
    }

    pub(super) fn try_own(
        &self,
        range: ops::Range<usize>,
    ) -> crate::Result<Option<storage::Shared>> {
        match self.parts(&range)? {
            [] => Ok(Some(storage::Shared::new())),
            [part] => part
                .owner
                .get(range.start - part.start..range.end - part.start)
                .ok_or(Error::Invalid)?
                .try_own()
                .map_err(Error::Retain),
            [_, _, ..] => Ok(None),
        }
    }

    pub(super) fn locate(&self, index: usize) -> Option<(usize, usize)> {
        let segment = self.segments.partition_point(|part| part.end <= index);
        let part = self.segments.get(segment)?;
        Some((segment, index - part.start))
    }

    pub(super) fn bytes_at(&self, segment: usize, local: usize) -> Option<&[u8]> {
        self.segments.get(segment)?.owner.as_slice().get(local..)
    }

    pub(super) fn project(&self, range: ops::Range<usize>) -> crate::Result<storage::Shared> {
        if range.start > range.end || range.end > self.len {
            return Err(Error::Invalid);
        }
        let Some(len) = NonZero::new(range.len()) else {
            return Ok(storage::Shared::new());
        };
        let (segment, local) = self.locate(range.start).ok_or(Error::Invalid)?;
        if len.get() <= self.segments[segment].owner.len() - local {
            let owner = self.segments[segment]
                .owner
                .get(local..local + len.get())
                .ok_or(Error::Invalid)?
                .try_retain()
                .map_err(Error::Retain)?;
            return match owner.try_detach() {
                Ok(shared) => Ok(shared),
                Err(owner) => storage::Shared::try_copy(owner.as_slice()).map_err(Error::from),
            };
        }
        let mut output = alloc::array::Block::try_new(len.get(), ()).map_err(Error::Allocation)?;
        let mut target = &mut output[..];
        let mut index = range.start;
        let mut parts = self.segments[segment..].iter();
        while !target.is_empty() {
            let part = parts.next().ok_or(Error::Invalid)?;
            let owner = part
                .owner
                .as_slice()
                .get(index - part.start..)
                .ok_or(Error::Invalid)?;
            let take = target.len().min(owner.len());
            let (head, tail) = target.split_at_mut(take);
            head.write_copy_of_slice(&owner[..take]);
            target = tail;
            index += take;
        }
        // SAFETY: consuming every exclusive target suffix initialized the exact allocation.
        let mut output = unsafe { alloc::raw::AssumeInit::assume_init(output) }.into_vec();
        storage::Shared::try_from(&mut output).map_err(Error::Exhausted)
    }
}
