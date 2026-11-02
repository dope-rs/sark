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
pub(super) fn apply<R: route::Schema>(
    headers: &mut R::RawHeaders,
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
    Ok(())
}

struct Bounds {
    tail: usize,
    start: usize,
    end: usize,
}

pub(super) struct Scan<'a, R: route::Schema> {
    pub(super) headers: &'a mut R::RawHeaders,
    pub(super) input: &'a [u8],
    pub(super) rest: &'a [u8],
    pub(super) start: usize,
    pub(super) fields: &'a mut Fields,
    pub(super) flags: &'a mut head::Flags,
    pub(super) coding: Coding,
    pub(super) version: Version,
}

impl<'a, R: route::Schema> Scan<'a, R> {
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
            Ok(bounds) => Step::Complete(bounds.tail),
            Err(step) => step,
        }
    }

    fn user_bounds(&mut self, colon: usize) -> Result<Bounds, Step<R>> {
        match headers::Known::from_name(self.try_prefix(colon)?) {
            Some(known) => self.scan_known(known, colon),
            None => {
                let Bounds {
                    tail,
                    mut start,
                    mut end,
                } = self.value(colon)?;
                while start < tail && matches!(self.rest.get(start), Some(b' ' | b'\t')) {
                    start += 1;
                }
                while end > start && matches!(self.rest.get(end - 1), Some(b' ' | b'\t')) {
                    end -= 1;
                }
                Ok(Bounds { tail, start, end })
            }
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

impl<R> Prefix<R::Header> for Scan<'_, R>
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
        match self.set(slot, &bounds) {
            Ok(()) => Step::Complete(bounds.tail),
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
