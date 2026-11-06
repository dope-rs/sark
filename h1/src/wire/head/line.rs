use header::Fields as _;
use sark_core::{
    request::field::{self, Prefix, SliceValue},
    route::{self, field::header},
};
use sark_http::{
    field::{
        head::{self, headers},
        scan::{Bytes, Outcome},
    },
    h1::{
        field::Entry,
        head::{
            line::Version,
            request::{Coding, Fields},
        },
    },
    request::{self, span},
};

use super::Admission;

pub enum Error<R: route::Schema> {
    Name {
        offset: usize,
        actual: u8,
    },
    Value {
        offset: usize,
        actual: u8,
    },
    Point {
        base: usize,
        relative: usize,
        available: usize,
    },
    Ending {
        start: usize,
        available: usize,
    },
    Limit {
        observed: usize,
        limit: usize,
    },
    Field {
        start: usize,
        available: usize,
        cause: head::Error,
    },
    Header(R::Error),
    Admission(crate::wire::Failure),
    Frame {
        slot: R::Header,
        cause: field::Error,
    },
    Span(request::Error),
    Offset {
        base: usize,
        relative: usize,
        suffix: usize,
        available: usize,
    },
}

pub(super) enum Step<R: route::Schema> {
    Complete(usize),
    NeedMore,
    Bad(Error<R>),
}

/// Applies one field whose syntax and absolute spans the native cursor admitted.
pub(super) fn apply<R: route::Schema, A: Admission>(
    headers: &mut R::RawHeaders,
    admission: &mut A,
    fields: &mut Fields,
    flags: &mut head::Flags,
    coding: Coding,
    entry: Entry<'_>,
    version: Version,
) -> Result<(), Error<R>> {
    let slot = R::Header::slice(entry.name());
    let known = headers::Known::from_name(entry.name());
    if let Some(known) = known {
        let skip = matches!(known, headers::Known::AcceptEncoding)
            && slot.is_none()
            && !R::FULL
            && matches!(coding, Coding::Plain);
        if !skip {
            known
                .apply(fields, flags, entry.value(), version)
                .map_err(|cause| Error::Field {
                    start: entry.line_span().start(),
                    available: entry.line_span().len(),
                    cause,
                })?;
        }
        if matches!(known, headers::Known::Host) {
            fields.routing = fields
                .routing
                .try_host(entry.value_span())
                .map_err(|cause| Error::Field {
                    start: entry.line_span().start(),
                    available: entry.line_span().len(),
                    cause: head::Error::Routing(cause),
                })?;
        }
    }
    if let Some(slot) = slot {
        R::Header::set(headers, slot, &SliceValue::from(entry)).map_err(Error::Header)?;
    }
    if A::FIELDS {
        admission
            .field(entry.name(), entry.value(), entry.value_span())
            .map_err(Error::Admission)?;
    }
    Ok(())
}

struct Bounds {
    tail: usize,
    start: usize,
    end: usize,
}

impl Bounds {
    fn trimmed(mut self, input: &[u8]) -> Self {
        while self.start < self.end && matches!(input.get(self.start), Some(b' ' | b'\t')) {
            self.start += 1;
        }
        while self.end > self.start && matches!(input.get(self.end - 1), Some(b' ' | b'\t')) {
            self.end -= 1;
        }
        self
    }
}

pub(super) struct Scan<'a, R: route::Schema, A = ()> {
    pub(super) headers: &'a mut R::RawHeaders,
    pub(super) admission: &'a mut A,
    pub(super) input: &'a [u8],
    pub(super) rest: &'a [u8],
    pub(super) start: usize,
    pub(super) fields: &'a mut Fields,
    pub(super) flags: &'a mut head::Flags,
    pub(super) coding: Coding,
    pub(super) version: Version,
}

impl<'a, R: route::Schema, A: Admission> Scan<'a, R, A> {
    fn observe(&mut self, colon: usize, bounds: Bounds) -> Result<(), Step<R>> {
        if A::FIELDS {
            let bounds = bounds.trimmed(self.rest);
            let name = self.try_prefix(colon)?;
            let span = self
                .try_span(&bounds)
                .map_err(|cause| Step::Bad(Error::Span(request::Error::Span(cause))))?;
            let value = span.get(self.input).ok_or_else(|| {
                Step::Bad(Error::Point {
                    base: span.start(),
                    relative: span.len(),
                    available: self.input.len(),
                })
            })?;
            self.admission
                .field(name, value, span)
                .map_err(|error| Step::Bad(Error::Admission(error)))?;
        }
        Ok(())
    }

    fn value(&self, colon: usize) -> Result<Bounds, Step<R>> {
        let limit = head::MAX_HEADER_LINE_BYTES;
        let capped = self.capped();
        let start = self.try_index(colon, 1, capped.len())?;
        let tail = match Bytes::new(capped).value(start) {
            Outcome::Found(tail) => tail,
            Outcome::Invalid { offset, actual } => {
                let offset = self.point(capped.len(), offset)?;
                return Err(Step::Bad(Error::Value { offset, actual }));
            }
            Outcome::Partial => return Err(self.incomplete()),
        };
        if tail > limit {
            return Err(Step::Bad(Error::Limit {
                observed: tail,
                limit,
            }));
        }
        Ok(Bounds {
            tail,
            start,
            end: tail,
        })
    }

    fn set(&mut self, slot: R::Header, bounds: &Bounds) -> Result<(), Step<R>> {
        let value = self
            .try_value(bounds)
            .map_err(|cause| Step::Bad(Error::Frame { slot, cause }))?;
        R::Header::set(self.headers, slot, &value).map_err(|cause| Step::Bad(Error::Header(cause)))
    }

    fn try_value(&self, bounds: &Bounds) -> Result<SliceValue<'a>, field::Error> {
        let span = self.try_span(bounds).map_err(field::Error::Span)?;
        SliceValue::try_at(self.input, span)
    }

    fn try_span(&self, bounds: &Bounds) -> Result<span::Span, span::Error> {
        span::Span::from_range(bounds.start..bounds.end).and_then(|span| span.shifted(self.start))
    }

    fn scan_known(&mut self, known: headers::Known, colon: usize) -> Result<Bounds, Step<R>> {
        let capped = self.capped();
        let start = self.try_index(colon, 1, capped.len())?;
        let line = &capped[start..];
        let value = match known.scan_line(self.fields, self.flags, line, self.version) {
            Ok(Some(value)) => value,
            Ok(None) => return Err(self.incomplete()),
            Err(cause) => {
                return Err(Step::Bad(Error::Field {
                    start: self.start,
                    available: capped.len(),
                    cause,
                }));
            }
        };
        let bounds = Bounds {
            tail: start + value.tail(),
            start: start + value.start(),
            end: start + value.end(),
        };
        if matches!(known, headers::Known::Host) {
            let span = self
                .try_span(&bounds)
                .map_err(|cause| Step::Bad(Error::Span(request::Error::Span(cause))))?;
            let route = self.fields.routing.try_host(span).map_err(|cause| {
                Step::Bad(Error::Field {
                    start: self.start,
                    available: capped.len(),
                    cause: head::Error::Routing(cause),
                })
            })?;
            self.fields.routing = route;
        }
        Ok(bounds)
    }

    fn name(&self, start: usize) -> Result<(usize, &'a [u8]), Step<R>> {
        let capped = self.capped();
        let (name_end, term) = match Bytes::new(capped).name(start) {
            Outcome::Found(found) => found,
            Outcome::Invalid { offset, actual } => {
                let offset = self.point(capped.len(), offset)?;
                return Err(Step::Bad(Error::Name { offset, actual }));
            }
            Outcome::Partial => return Err(self.incomplete()),
        };
        if term == b'\r' {
            return Err(if name_end == 0 && capped.get(1) == Some(&b'\n') {
                Step::Complete(0)
            } else if name_end == 0 && capped.len() == 1 {
                Step::NeedMore
            } else {
                Step::Bad(Error::Ending {
                    start: self.start,
                    available: capped.len(),
                })
            });
        }
        if name_end == 0 {
            return Err(Step::Bad(Error::Name {
                offset: self.start,
                actual: term,
            }));
        }
        let name = self.try_prefix(name_end)?;
        Ok((name_end, name))
    }

    fn miss(mut self, name_end: usize, known: Option<headers::Known>) -> Step<R> {
        let bounds = match known {
            Some(headers::Known::AcceptEncoding)
                if !R::FULL && matches!(self.coding, Coding::Plain) =>
            {
                self.value(name_end)
            }
            Some(known) => self.scan_known(known, name_end),
            None => self.value(name_end),
        };
        match bounds {
            Ok(bounds) => {
                let tail = bounds.tail;
                match self.observe(name_end, bounds) {
                    Ok(()) => Step::Complete(tail),
                    Err(step) => step,
                }
            }
            Err(step) => step,
        }
    }

    fn user_bounds(&mut self, colon: usize) -> Result<Bounds, Step<R>> {
        match headers::Known::from_name(self.try_prefix(colon)?) {
            Some(known) => self.scan_known(known, colon),
            None => Ok(self.value(colon)?.trimmed(self.rest)),
        }
    }

    fn incomplete(&self) -> Step<R> {
        let limit = head::MAX_HEADER_LINE_BYTES;
        let observed = self.rest.len();
        let ending = observed == limit + 1 && self.rest.last() == Some(&b'\r');
        if observed > limit && !ending {
            Step::Bad(Error::Limit { observed, limit })
        } else {
            Step::NeedMore
        }
    }

    fn capped(&self) -> &'a [u8] {
        match self.rest.get(..head::MAX_HEADER_LINE_BYTES + 2) {
            Some(capped) => capped,
            None => self.rest,
        }
    }

    fn try_prefix(&self, end: usize) -> Result<&'a [u8], Step<R>> {
        self.rest.get(..end).ok_or_else(|| {
            Step::Bad(Error::Point {
                base: self.start,
                relative: end,
                available: self.rest.len(),
            })
        })
    }

    fn try_index(&self, relative: usize, suffix: usize, cap: usize) -> Result<usize, Step<R>> {
        let error = || {
            Step::Bad(Error::Offset {
                base: self.start,
                relative,
                suffix,
                available: cap,
            })
        };
        let index = relative.checked_add(suffix).ok_or_else(error)?;
        if index > cap {
            return Err(error());
        }
        Ok(index)
    }

    fn point(&self, len: usize, relative: usize) -> Result<usize, Step<R>> {
        let failure = || {
            Step::Bad(Error::Point {
                base: self.start,
                relative,
                available: len,
            })
        };
        self.start.checked_add(relative).ok_or_else(failure)
    }
}

impl<R, A: Admission> Prefix<R::Header> for Scan<'_, R, A>
where
    R: route::Schema,
{
    type Output = Step<R>;

    fn rest(&self) -> &[u8] {
        self.rest
    }

    fn user(mut self, slot: R::Header, colon: usize) -> Self::Output {
        let bounds = match self.user_bounds(colon) {
            Ok(bounds) => bounds,
            Err(step) => return step,
        };
        let tail = bounds.tail;
        match self
            .set(slot, &bounds)
            .and_then(|()| self.observe(colon, bounds))
        {
            Ok(()) => Step::Complete(tail),
            Err(step) => step,
        }
    }

    /// A later name contains any preceding colon, so it cannot be a known name.
    fn miss(self, start: usize) -> Self::Output {
        if let Some((known, colon)) = headers::Known::from_prefix(self.capped())
            && start <= colon
        {
            return self.miss(colon, Some(known));
        }
        let (name_end, _) = match self.name(start) {
            Ok(name) => name,
            Err(step) => return step,
        };
        self.miss(name_end, None)
    }

    fn short(self) -> Self::Output {
        let (name_end, name) = match self.name(0) {
            Ok(name) => name,
            Err(step) => return step,
        };
        match R::Header::slice(name) {
            Some(slot) => self.user(slot, name_end),
            None => self.miss(name_end, headers::Known::from_name(name)),
        }
    }
}
