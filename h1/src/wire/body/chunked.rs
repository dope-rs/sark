use std::{collections, mem};

use sark_core::body::admit::Retention;
use sark_http::h1::chunk::{self, Boundary};

use crate::wire::body::{Payload, fixed, ingress, shape::Shape};

pub(crate) struct Complete<'frame, 'd> {
    /// Wire bytes consumed from the current transport input.
    pub(crate) consumed: usize,
    pub(crate) body: Payload<'frame, 'd>,
}

/// The exact completed decoder, frozen until its source-bound frame is consumed.
pub(crate) struct Borrowed<'frame> {
    source: &'frame Chunked,
}

impl<'frame> Borrowed<'frame> {
    pub(crate) fn len(&self) -> usize {
        self.source.decoder.body_bytes()
    }

    pub(in crate::wire) fn into_ranges(self) -> &'frame [(u32, u32)] {
        &self.source.ranges
    }
}

#[derive(Debug)]
pub enum Decode {
    Wire(chunk::Error),
    Range {
        start: usize,
        end: usize,
        maximum: usize,
    },
    Framing,
    Ingress(ingress::Error),
    Reserve(collections::TryReserveError),
}

impl From<ingress::Error> for Decode {
    fn from(error: ingress::Error) -> Self {
        Self::Ingress(error)
    }
}

/// Per-connection chunk framing with borrowed one-shot ranges and owned
/// cross-transport payload pages.
pub(crate) struct Chunked {
    decoder: chunk::Framer,
    limits: chunk::Limit,
    ranges: Vec<(u32, u32)>,
    progress: Progress,
    boundary: Boundary,
}

#[derive(Clone, Copy)]
enum Progress {
    Idle,
    Borrowed,
    Streamed(Retention),
    Complete,
}

#[derive(Clone, Copy)]
enum Flow {
    Borrowed,
    Streamed,
}

impl Chunked {
    pub(crate) fn new(ranges: Vec<(u32, u32)>) -> Self {
        Self {
            decoder: chunk::Framer::new(),
            limits: chunk::Limit::with_body(0),
            ranges,
            progress: Progress::Idle,
            boundary: Boundary::End,
        }
    }
    pub(crate) fn decode<'frame, 'd>(
        &'frame mut self,
        input: crate::Input<'_>,
        head: usize,
        max: usize,
        keep: Retention,
        store: &'frame mut ingress::Ingress<'d>,
    ) -> Result<Option<Complete<'frame, 'd>>, Decode> {
        if !matches!(self.progress, Progress::Idle) {
            return Err(Decode::Framing);
        }
        let Some(suffix) = input.as_slice().get(head..) else {
            return Err(Decode::Framing);
        };
        self.limits = chunk::Limit::with_body(max);
        self.ranges.clear();
        let cap = max.min(crate::MAX_CHUNK_PAYLOAD_RANGES);
        if let Retention::Retained = keep
            && self.ranges.capacity() < cap
        {
            self.ranges
                .try_reserve_exact(cap)
                .map_err(Decode::Reserve)?;
        }
        store.start(keep, Shape::Segmented);
        self.progress = Progress::Borrowed;
        self.drive(input, suffix, keep, Flow::Borrowed, store)
    }

    fn drive<'frame, 'd>(
        &'frame mut self,
        input: crate::Input<'_>,
        mut suffix: &[u8],
        keep: Retention,
        mut flow: Flow,
        store: &'frame mut ingress::Ingress<'d>,
    ) -> Result<Option<Complete<'frame, 'd>>, Decode> {
        let req = input.as_slice();

        let consumed = if let Retention::Discarded = keep {
            let before = self.decoder.wire_bytes();
            let scanned = self
                .decoder
                .scan(suffix, &self.limits)
                .map_err(Decode::Wire)?;
            if !scanned.is_complete() {
                self.progress = Progress::Streamed(keep);
                return Ok(None);
            }
            req.len() - suffix.len() + (self.decoder.wire_bytes() - before)
        } else {
            loop {
                let (next, event) = self
                    .decoder
                    .decode(suffix, &self.limits, usize::MAX)
                    .map_err(Decode::Wire)?;
                self.decoder = next;
                let used = match &event {
                    chunk::Fragment::More(consumed) => *consumed,
                    chunk::Fragment::Trailer(consumed)
                    | chunk::Fragment::Data { consumed, .. }
                    | chunk::Fragment::Complete { consumed, .. } => consumed.get(),
                };
                let Some(rest) = suffix.get(used..) else {
                    return Err(Decode::Framing);
                };
                suffix = rest;
                let end = req.len() - suffix.len();

                match event {
                    chunk::Fragment::More(_) | chunk::Fragment::Trailer(_) => {
                        if matches!(flow, Flow::Borrowed) {
                            self.promote(input, keep, store)?;
                        }
                        return Ok(None);
                    }
                    chunk::Fragment::Data {
                        bytes: chunk,
                        boundary,
                        ..
                    } => {
                        if !chunk.is_empty() {
                            if matches!(flow, Flow::Streamed) {
                                store.push_segment(chunk, boundary)?;
                            } else if self.ranges.len() == crate::MAX_CHUNK_PAYLOAD_RANGES {
                                self.promote(input, keep, store)?;
                                flow = Flow::Streamed;
                                store.push_segment(chunk, boundary)?;
                            } else {
                                let start = end - chunk.len();
                                self.push_range(start, end, boundary)?;
                            }
                        }
                    }
                    chunk::Fragment::Complete { .. } => break end,
                }
            }
        };
        let body_len = self.decoder.body_bytes();
        let body = match (flow, keep) {
            (Flow::Borrowed, Retention::Retained) => {
                self.progress = Progress::Complete;
                Payload::Chunked(Borrowed { source: self }, ())
            }
            (Flow::Borrowed, Retention::Discarded) => {
                self.progress = Progress::Complete;
                Payload::Discarded(body_len, ())
            }
            (Flow::Streamed, Retention::Retained) => {
                let body = store.finish()?;
                self.progress = Progress::Complete;
                Payload::Streamed(body, ())
            }
            (Flow::Streamed, Retention::Discarded) => {
                store.finish()?;
                self.progress = Progress::Complete;
                Payload::Discarded(body_len, ())
            }
        };
        Ok(Some(Complete { consumed, body }))
    }

    pub(crate) fn resume(
        &mut self,
        wire: &[u8],
        store: &mut ingress::Ingress<'_>,
    ) -> Result<Option<fixed::Complete>, Decode> {
        let Progress::Streamed(keep) = self.progress else {
            return Err(Decode::Framing);
        };
        let consumed = match self.drive(
            crate::Input::Borrowed(wire),
            wire,
            keep,
            Flow::Streamed,
            store,
        )? {
            Some(done) => done.consumed,
            None => return Ok(None),
        };
        Ok(Some(fixed::Complete {
            consumed,
            body_len: self.decoder.body_bytes(),
        }))
    }

    fn promote(
        &mut self,
        input: crate::Input<'_>,
        keep: Retention,
        store: &mut ingress::Ingress<'_>,
    ) -> Result<(), Decode> {
        if let Retention::Retained = keep {
            let req = input.as_slice();
            let count = self.ranges.len();
            for (index, &(start, end)) in self.ranges.iter().enumerate() {
                // SAFETY: decode clears ranges before Borrowed drive records
                // Framer fragments from this same immutable input. Only that
                // drive calls promote before returning; Streamed resume cannot.
                let part = unsafe { req.get_unchecked(start as usize..end as usize) };
                let boundary = if index + 1 == count {
                    self.boundary
                } else {
                    Boundary::End
                };
                store.push_segment(part, boundary)?;
            }
            self.ranges.clear();
        }
        self.progress = Progress::Streamed(keep);
        Ok(())
    }

    fn push_range(&mut self, start: usize, end: usize, boundary: Boundary) -> Result<(), Decode> {
        let max = u32::MAX as usize;
        if start > max || end > max {
            return Err(Decode::Range {
                start,
                end,
                maximum: max,
            });
        }
        let start = start as u32;
        let end = end as u32;
        if let Some(last) = self.ranges.last_mut()
            && last.1 == start
        {
            last.1 = end;
            self.boundary = boundary;
            return Ok(());
        }
        self.ranges.push((start, end));
        self.boundary = boundary;
        Ok(())
    }

    pub(crate) fn into_ranges(mut self) -> Vec<(u32, u32)> {
        self.ranges.clear();
        self.ranges
    }
}

const _: () = {
    assert!(mem::size_of::<Flow>() == 1);
    assert!(mem::size_of::<Complete<'static, 'static>>() == 3 * mem::size_of::<usize>());
    assert!(mem::size_of::<Progress>() == 1);
    assert!(mem::size_of::<Borrowed<'static>>() == mem::size_of::<usize>());
};
