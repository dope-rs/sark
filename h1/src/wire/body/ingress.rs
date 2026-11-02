use core::{mem, num};
use std::collections;

use o3::{
    alloc,
    buffer::{hold::ranges, segments, storage, write::SpareExt as _},
    mem::rc,
};
use ranges::retain;
use retain::Error::{Metadata, Resident, Retain};
use sark_core::body::admit::Retention;
use sark_http::{
    h1::chunk::Boundary,
    payload::{owned, response::Body as _},
};
use segments::assembly;
use storage::range;

use crate::wire::body::shape::Shape;

const PAGE: num::NonZero<usize> = num::NonZero::<usize>::MIN.saturating_add(16 * 1024 - 1);
pub(crate) const PAGE_BYTES: usize = PAGE.get();

#[derive(Debug)]
pub enum Error {
    Range(range::Error),
    Storage(storage::AllocError),
    Assembly(assembly::Error),
    Ranges(ranges::Error<u16>),
    Metadata(alloc::Error),
    Retain {
        index: usize,
        error: rc::Error,
    },
    Resident {
        index: usize,
        current: usize,
        added: usize,
    },
    Reserve(collections::TryReserveError),
}

impl From<retain::Error> for Error {
    fn from(error: retain::Error) -> Self {
        match error {
            Metadata(error) => Self::Metadata(error),
            Retain { index, error } => Self::Retain { index, error },
            Resident {
                index,
                current,
                added,
            } => Self::Resident {
                index,
                current,
                added,
            },
        }
    }
}

enum Ranges {
    Inline {
        end: Option<u16>,
        spare: Vec<(u16, u16)>,
    },
    Heap(Vec<(u16, u16)>),
}

impl Default for Ranges {
    fn default() -> Self {
        Self::Inline {
            end: None,
            spare: Vec::new(),
        }
    }
}

impl Ranges {
    fn len(&self) -> usize {
        match self {
            Self::Inline { end, .. } => usize::from(end.is_some()),
            Self::Heap(ranges) => ranges.len(),
        }
    }

    fn clear(&mut self) {
        let spare = match self {
            Self::Inline { spare, .. } | Self::Heap(spare) => spare,
        };
        spare.clear();
        *self = Self::Inline {
            end: None,
            spare: mem::take(spare),
        };
    }

    /// Records an ordered same-page range from `Ingress::push_segment`.
    /// Its private cursor starts each page at zero and stays within `PAGE_BYTES`.
    fn push(&mut self, start: u16, end: u16) -> Result<(), Error> {
        if start == end {
            return Ok(());
        }
        match self {
            Self::Inline { end: first, spare } => match first {
                Some(first) => {
                    spare.try_reserve_exact(2).map_err(Error::Reserve)?;
                    spare.push((0, *first));
                    spare.push((start, end));
                    *self = Self::Heap(mem::take(spare));
                }
                None => *first = Some(end),
            },
            Self::Heap(ranges) => {
                ranges.try_reserve(1).map_err(Error::Reserve)?;
                ranges.push((start, end));
            }
        }
        Ok(())
    }

    fn flush<'d>(&mut self, page: storage::Shared) -> Result<owned::Segments<'d>, Error> {
        let result = match self {
            Self::Inline { end, .. } => match *end {
                None => Ok(owned::Segments::from(page)),
                Some(end) => match page.project(0..usize::from(end)) {
                    Ok(part) => Ok(owned::Segments::from(part)),
                    Err(rejected) => Err(Error::Range(rejected.into_parts().1)),
                },
            },
            Self::Heap(ranges) => ranges::Set::try_new(&page, ranges, page.len())
                .map_err(Error::Ranges)
                .and_then(|parts| parts.try_retain().map_err(Error::from))
                .map(owned::Segments::from),
        };
        self.clear();
        result
    }
}

/// The single owner plane for every HTTP/1 body that crosses an ingress
/// lifetime boundary. Wire framers feed logical bytes and optional segment
/// boundaries into this value; they never retain payload owners themselves.
pub(crate) struct Ingress<'d> {
    retained: owned::Segments<'d>,
    staging: Option<storage::Owned>,
    ranges: Ranges,
    segment_start: Option<u16>,
    shape: Shape,
    retention: Retention,
}

/// Exclusive access issued only after this ingress has completed its body.
pub(crate) struct Finished<'frame, 'd> {
    source: &'frame mut Ingress<'d>,
}

impl<'frame, 'd> Finished<'frame, 'd> {
    pub(crate) fn len(&self) -> usize {
        self.source.retained.len()
    }

    pub(crate) fn into_body(self) -> &'frame mut owned::Segments<'d> {
        &mut self.source.retained
    }

    pub(crate) fn take(self) -> owned::Segments<'d> {
        self.source.take()
    }
}

impl Default for Ingress<'_> {
    fn default() -> Self {
        Self {
            retained: owned::Segments::new(),
            staging: None,
            ranges: Ranges::default(),
            segment_start: None,
            shape: Shape::Flat,
            retention: Retention::Discarded,
        }
    }
}

impl<'d> Ingress<'d> {
    pub(crate) fn retained_len(&self) -> usize {
        self.retained.len() + self.staging_len()
    }

    pub(crate) fn audit_chunks(&self) -> usize {
        const { assert!(usize::BITS >= u32::BITS) };
        let retained = self.retained.count().into_raw() as usize;
        retained + usize::from(self.staging_len() != 0)
    }

    pub(in crate::wire::body) fn start(&mut self, keep: Retention, shape: Shape) {
        self.retained = owned::Segments::new();
        self.staging = None;
        self.ranges.clear();
        self.segment_start = None;
        self.shape = shape;
        self.retention = keep;
    }

    pub(crate) fn push_flat(
        &mut self,
        part: &[u8],
        body_len: num::NonZero<usize>,
    ) -> Result<(), Error> {
        if let Retention::Discarded = self.retention {
            return Ok(());
        }
        self.push_bytes(part, body_len.min(PAGE))
    }

    pub(crate) fn push_segment(
        &mut self,
        mut part: &[u8],
        boundary: Boundary,
    ) -> Result<(), Error> {
        if let Retention::Discarded = self.retention {
            return Ok(());
        }
        if part.is_empty() {
            return Ok(());
        }
        if self.segment_start.is_none()
            && self.ranges.len() == crate::MAX_CHUNK_PAYLOAD_RANGES
            && let Shape::Segmented = self.shape
        {
            self.shape = Shape::Flat;
            self.ranges.clear();
        }
        if let Shape::Flat = self.shape {
            return self.push_bytes(part, PAGE);
        }
        let mut start = match self.segment_start.take() {
            Some(start) => start,
            None => self.staging_len() as u16,
        };
        while !part.is_empty() {
            let page = match &mut self.staging {
                Some(page) => page,
                slot @ None => {
                    slot.insert(storage::Owned::try_with(PAGE_BYTES).map_err(Error::Storage)?)
                }
            };
            page.spare().drain(&mut part);
            if page.len() == PAGE_BYTES {
                self.ranges.push(start, PAGE_BYTES as u16)?;
                self.flush_page()?;
                start = 0;
            }
        }
        match boundary {
            Boundary::End => {
                if self.staging_len() != 0 {
                    let end = self.staging_len() as u16;
                    self.ranges.push(start, end)?;
                }
            }
            Boundary::More => self.segment_start = Some(start),
        }
        Ok(())
    }

    fn push_bytes(&mut self, mut part: &[u8], capacity: num::NonZero<usize>) -> Result<(), Error> {
        while !part.is_empty() {
            let page = match &mut self.staging {
                Some(page) => page,
                slot @ None => {
                    slot.insert(storage::Owned::try_with(capacity.get()).map_err(Error::Storage)?)
                }
            };
            let limit = page.capacity();
            page.spare().drain(&mut part);
            if page.len() == limit {
                self.flush_page()?;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<Finished<'_, 'd>, Error> {
        if let Retention::Retained = self.retention {
            self.flush_page()?;
        }
        Ok(Finished { source: self })
    }

    fn flush_page(&mut self) -> Result<(), Error> {
        let Some(page) = self.staging.take() else {
            return Ok(());
        };
        if page.is_empty() {
            return Ok(());
        }
        let page = page.freeze();
        let source = self.ranges.flush(page)?;
        let (next, result) = match mem::take(&mut self.retained).try_extend(source) {
            Ok(body) => (body, Ok(())),
            Err(rejected) => {
                let ((body, _), error) = rejected.into_parts();
                (body, Err(Error::Assembly(error)))
            }
        };
        self.retained = next;
        result
    }

    /// Drains the completed segment prefix. Before frame completion this
    /// finalizes only the current batch and leaves a fresh builder for later
    /// wire bytes; after completion it moves the final batch unchanged.
    pub(crate) fn take(&mut self) -> owned::Segments<'d> {
        mem::take(&mut self.retained)
    }

    pub(crate) fn reset(&mut self) {
        self.start(Retention::Discarded, Shape::Flat);
    }

    fn staging_len(&self) -> usize {
        self.staging.as_ref().map_or(0, |page| page.len())
    }
}

const _: () = {
    assert!(mem::size_of::<Shape>() == mem::size_of::<bool>());
    assert!(mem::align_of::<Shape>() == mem::align_of::<bool>());
    assert!(mem::size_of::<Retention>() == mem::size_of::<bool>());
    assert!(PAGE_BYTES <= u16::MAX as usize);
    assert!(mem::size_of::<Ranges>() == mem::size_of::<(Vec<(u16, u16)>, Option<u16>)>());
};
