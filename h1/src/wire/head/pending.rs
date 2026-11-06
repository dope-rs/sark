use std::num::NonZero;

use o3::buffer::write::Sink as _;
use sark_core::route;
use sark_http::{
    field::head,
    h1::{
        field::scan,
        head::{
            line::{Owned, Request},
            request::{Coding, Fields},
        },
    },
};

use crate::wire::{
    head::{Admission, Block, Error, Gzip, Parsed, Scanned, line},
    recv,
};

type Source = Owned<{ recv::BODY_CAPACITY }>;

pub(crate) struct Partial<'input, H, A = ()> {
    pub(super) input: &'input [u8],
    pub(super) headers: H,
    pub(super) admission: A,
    pub(super) fields: Fields,
    pub(super) flags: head::Flags,
    pub(super) count: usize,
    pub(super) pos: usize,
}

/// Typed completed fields and append-only ownership of the uncompleted head.
pub(crate) struct Pending<H, A = ()> {
    source: scan::State<Source>,
    headers: H,
    admission: A,
    fields: Fields,
    flags: head::Flags,
    coding: Coding,
}

pub(crate) enum Progress<H, A: Admission = ()> {
    Pending(Pending<H, A>),
    Complete(Source, Gzip<H, A>),
}

impl<H, A: Admission> Partial<'_, H, A> {
    pub(crate) fn retain<R>(
        self,
        request: Request<'_>,
        coding: Coding,
        limit: usize,
    ) -> Result<Pending<H, A>, Error<R>>
    where
        R: route::Schema<RawHeaders = H>,
    {
        let input = self.input;
        let capacity = NonZero::new(input.len()).unwrap_or(NonZero::<usize>::MIN);
        let source = request.try_snapshot(capacity).map_err(Error::Storage)?;
        Pending::try_new(
            source,
            self.headers,
            self.admission,
            self.fields,
            self.flags,
            coding,
            (self.pos, self.count),
            limit,
        )
    }
}

impl<H, A: Admission> Pending<H, A> {
    fn try_new<R: route::Schema<RawHeaders = H>>(
        source: Source,
        headers: H,
        admission: A,
        fields: Fields,
        flags: head::Flags,
        coding: Coding,
        (pos, count): (usize, usize),
        limit: usize,
    ) -> Result<Self, Error<R>> {
        let source = scan::State::try_new(source, pos, count, limit, head::MAX_HEADER_LINE_BYTES)
            .map_err(|(_, error)| Error::Syntax(error))?;
        Ok(Self {
            source,
            headers,
            admission,
            fields,
            flags,
            coding,
        })
    }

    pub(crate) fn try_start<R>(
        source: Source,
        coding: Coding,
        limit: usize,
    ) -> Result<Progress<H, A>, Error<R>>
    where
        R: route::Schema<RawHeaders = H>,
        H: Default,
    {
        let parsed = Block::<H, A>::scan::<R>(
            source.as_ref(),
            source.request().end(),
            limit,
            coding,
            source.request().version(),
        )?;
        match parsed {
            Scanned::Complete(block) => {
                let coding = block.coding();
                let head = Parsed::from_block::<R>(block, &source.request())?;
                Ok(Progress::Complete(source, Gzip { head, coding }))
            }
            Scanned::Pending(partial) => {
                let Partial {
                    input: _,
                    headers,
                    admission,
                    fields,
                    flags,
                    count,
                    pos,
                } = partial;
                Self::try_new(
                    source,
                    headers,
                    admission,
                    fields,
                    flags,
                    coding,
                    (pos, count),
                    limit,
                )
                .map(Progress::Pending)
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.source.as_ref().len()
    }

    pub(crate) fn advance<R>(mut self, incoming: &[u8]) -> Result<Progress<H, A>, Error<R>>
    where
        R: route::Schema<RawHeaders = H>,
    {
        let prior = self.len();
        let maximum = A::MAX_BYTES.min(recv::HEAD_CAPACITY);
        let (prefix, tail) = incoming.split_at(incoming.len().min(maximum.saturating_sub(prior)));
        self.source.try_extend(prefix).map_err(Error::Storage)?;
        let Self {
            source,
            mut headers,
            mut admission,
            mut fields,
            mut flags,
            coding,
        } = self;
        let version = source.source().request().version();
        let progress = source
            .advance(|entry| {
                line::apply::<R, A>(
                    &mut headers,
                    &mut admission,
                    &mut fields,
                    &mut flags,
                    coding,
                    entry,
                    version,
                )
            })
            .map_err(|(_, error)| match error {
                scan::Error::Syntax(error) => Error::Syntax(error),
                scan::Error::Sink(error) => Error::Line(error),
            })?;
        match progress {
            scan::Progress::More(source) => {
                if !tail.is_empty() {
                    return Err(Error::Length {
                        len: prior.saturating_add(incoming.len()),
                    });
                }
                Ok(Progress::Pending(Self {
                    source,
                    headers,
                    admission,
                    fields,
                    flags,
                    coding,
                }))
            }
            scan::Progress::Complete(complete) => {
                let (mut source, _, len) = complete.into_parts();
                let block = Block {
                    headers,
                    admission,
                    fields,
                    len,
                };
                let coding = block.coding();
                let head = Parsed::from_block::<R>(block, &source.request())?;
                source.write_slice(tail).map_err(Error::Storage)?;
                Ok(Progress::Complete(source, Gzip { head, coding }))
            }
        }
    }
}
