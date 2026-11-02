use std::{mem, num::NonZero, task::Poll};

use o3::buffer::{resident, segments};
use sark_http::payload::owned;

use crate::segment::{
    Admitted, Document, Field, Order, Push, ScanProgress, Transfer, input, is_overlap,
    state::{Pin, Value},
};
mod plan;
mod prepared;
mod scan;
pub(super) use prepared::Prepared;
use scan::Scan;
pub(super) use scan::Selection;

pub(super) enum Scanner<'d> {
    Open(Scanning<'d>),
    Closed(Scanning<'d>),
    Preparing(Prepared<'d>),
}

pub(super) struct Scanning<'d> {
    scan: Scan<'d>,
    pub(super) stream: Stream,
}
pub(super) struct Released {
    pub(super) bytes: usize,
    pub(super) chunks: segments::Count,
}

impl Released {
    pub(super) const fn new() -> Self {
        Self {
            bytes: 0,
            chunks: segments::Count::EMPTY,
        }
    }

    pub(super) fn record(&mut self, bytes: usize) {
        self.bytes += bytes;
        if bytes != 0 {
            self.chunks = segments::Count::new(self.chunks.into_raw() + 1);
        }
    }

    /// Reports disjoint owners from this decoder's admitted input table.
    pub(super) fn add(&self, bytes: usize, chunks: segments::Count) -> (usize, segments::Count) {
        (
            bytes + self.bytes,
            segments::Count::new(chunks.into_raw() + self.chunks.into_raw()),
        )
    }
}

pub(super) struct Stream {
    release: segments::Count,
    field: usize,
    bytes: usize,
    chunks: segments::Count,
}
impl Stream {
    fn new() -> Self {
        Self {
            release: segments::Count::EMPTY,
            field: 0,
            bytes: 0,
            chunks: segments::Count::EMPTY,
        }
    }
    /// Advances past one reaped segment. A reaping walk covers at most the
    /// input's own table, and the input constructor already proved that table
    /// fits `u32`, so the count stays inside the segment domain.
    fn advance(&mut self) {
        self.release = segments::Count::new(self.release.into_raw() + 1);
        self.field = 0;
    }
    fn restart(&mut self) {
        self.release = segments::Count::EMPTY;
        self.field = 0;
    }
    /// Each field comparison or empty owner advances within the same budget.
    /// No parser or materializer mutates the fields while this walk is pending.
    fn reap(
        &mut self,
        input: &mut input::Input<'_>,
        fields: &[Option<Value>],
        end: usize,
        pin: Pin,
        budget: &mut usize,
    ) -> Poll<()> {
        let count = end.saturating_sub(self.release.into_raw() as usize);
        for part in input.tail(self.release).take(count) {
            if *budget == 0 {
                return Poll::Pending;
            }
            if part.is_empty() {
                *budget -= 1;
                self.advance();
                continue;
            }
            let range = part.range();
            if let Pin::Range(ref pin) = pin
                && is_overlap(pin, &range)
            {
                return Poll::Ready(());
            }
            let mut retained = false;
            while let Some(value) = fields.get(self.field) {
                if *budget == 0 {
                    return Poll::Pending;
                }
                *budget -= 1;
                self.field += 1;
                if value
                    .as_ref()
                    .is_some_and(|value| value.is_retained(&range))
                {
                    retained = true;
                    break;
                }
            }
            if !retained {
                if *budget == 0 {
                    return Poll::Pending;
                }
                *budget -= 1;
                self.record(part.take());
            }
            self.advance();
        }
        Poll::Ready(())
    }
    /// Records the bytes and the one chunk released by a segment. Released
    /// bytes are a subset of the input length, which the input constructor
    /// proved fits `usize`, and chunks are a subset of its table.
    fn record(&mut self, bytes: usize) {
        if bytes != 0 {
            self.chunks = segments::Count::new(self.chunks.into_raw() + 1);
        }
        self.bytes += bytes;
    }
    pub(super) fn drain(&mut self) -> Released {
        Released {
            bytes: mem::take(&mut self.bytes),
            chunks: mem::replace(&mut self.chunks, segments::Count::EMPTY),
        }
    }
}
impl<'d> Scanner<'d> {
    pub(super) fn admitted(input: Admitted<'d>, selection: Selection<'d>) -> Self {
        Self::Closed(Scanning {
            scan: Scan::admitted(input, selection),
            stream: Stream::new(),
        })
    }

    pub(super) fn try_stream(
        fields: &'d [Field<'d>],
        order: Order,
    ) -> Result<Self, crate::segment::Create> {
        Ok(Self::Open(Scanning {
            scan: Scan::try_stream(fields, order)?,
            stream: Stream::new(),
        }))
    }
    pub(super) fn new<I, O>(owners: I, fields: &'d [Field<'d>], order: Order) -> crate::Result<Self>
    where
        I: IntoIterator<Item = O>,
        I::IntoIter: ExactSizeIterator,
        resident::Shared<'d>: From<O>,
    {
        Ok(Self::Closed(Scanning {
            scan: Scan::new(owners, fields, order)?,
            stream: Stream::new(),
        }))
    }
    pub(super) fn try_new(
        owners: owned::Segments<'d>,
        fields: &'d [Field<'d>],
        order: Order,
    ) -> Result<Self, crate::segment::Create> {
        Ok(Self::Closed(Scanning {
            scan: Scan::try_new(owners, fields, order)?,
            stream: Stream::new(),
        }))
    }
    pub(super) fn push(self, segment: resident::Shared<'d>) -> Transfer<Self, Push<'d>> {
        let Self::Open(scanner) = self else {
            return Transfer::Rejected {
                state: self,
                error: Push::Complete(segment),
            };
        };
        let Scanning { scan, stream } = scanner;
        scan.push(segment).map(
            |scan| Self::Open(Scanning { scan, stream }),
            |error| match error {
                input::Failure::Length { len, add, owner } => Push::Length { len, add, owner },
                input::Failure::Segments {
                    current,
                    add,
                    owner,
                } => Push::Segments {
                    current,
                    add,
                    owner,
                },
                input::Failure::Allocation { error, owner } => Push::Allocation { error, owner },
            },
        )
    }
    pub(super) fn close(self) -> Self {
        match self {
            Self::Open(scan) => Self::Closed(scan),
            closed => closed,
        }
    }
    pub(super) fn release(self) -> crate::segment::Release<'d> {
        match self {
            Self::Open(scan) | Self::Closed(scan) => scan.release(),
            Self::Preparing(prep) => prep.release(),
        }
    }
}
impl<'d> Scanning<'d> {
    pub(super) fn release(self) -> crate::segment::Release<'d> {
        Document::new(self.scan.input, self.scan.selection.values).release()
    }

    pub(super) fn step(
        &mut self,
        budget: NonZero<usize>,
        closed: bool,
    ) -> crate::Result<(ScanProgress, usize)> {
        let mut left = budget.get();
        while left != 0 {
            let pin = self.scan.state.pin(self.scan.cursor.absolute);
            let end = self.scan.cursor.segment;
            if self
                .stream
                .reap(
                    &mut self.scan.input,
                    &self.scan.selection.values,
                    end,
                    pin,
                    &mut left,
                )
                .is_pending()
            {
                break;
            }
            if self.scan.state.is_done() {
                return Ok((ScanProgress::Ready, left));
            }
            if !closed
                && self.scan.cursor.absolute == self.scan.input.len()
                && self.scan.state.can_wait()
            {
                return Ok((ScanProgress::NeedInput, left));
            }
            if left == 0 {
                break;
            }
            let skipped = self.scan.skip(left);
            if skipped != 0 {
                left -= skipped;
            } else {
                self.scan.transition(&mut left)?;
            }
        }
        Ok((ScanProgress::Pending, left))
    }
}
