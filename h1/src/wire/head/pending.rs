use std::num::NonZero;

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
    head::{Block, Error, Scanned, line},
    recv,
};

type Source = Owned<{ recv::BODY_CAPACITY }>;

pub(crate) struct Partial<'input, H> {
    pub(super) input: &'input [u8],
    pub(super) headers: H,
    pub(super) fields: Fields,
    pub(super) flags: head::Flags,
    pub(super) count: usize,
    pub(super) pos: usize,
}

/// Typed completed fields and append-only ownership of the uncompleted head.
pub(crate) struct Pending<H> {
    source: scan::State<Source>,
    headers: H,
    fields: Fields,
    flags: head::Flags,
    coding: Coding,
}

pub(crate) enum Progress<H> {
    Pending(Pending<H>),
    Complete(Source, Block<H>),
}

impl<H> Partial<'_, H> {
    pub(crate) fn retain<R>(
        self,
        request: Request<'_>,
        coding: Coding,
        limit: usize,
    ) -> Result<Pending<H>, Error<R>>
    where
        R: route::Schema<RawHeaders = H>,
    {
        let input = self.input;
        if input.len() > recv::HEAD_CAPACITY {
            return Err(Error::Length { len: input.len() });
        }
        let capacity = NonZero::new(input.len()).unwrap_or(NonZero::<usize>::MIN);
        let source = request.try_snapshot(capacity).map_err(Error::Storage)?;
        Pending::try_new(
            source,
            self.headers,
            self.fields,
            self.flags,
            coding,
            (self.pos, self.count),
            limit,
        )
    }
}

impl<H> Pending<H> {
    fn try_new<R: route::Schema<RawHeaders = H>>(
        source: Source,
        headers: H,
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
            fields,
            flags,
            coding,
        })
    }

    pub(crate) fn try_start<R>(
        source: Source,
        coding: Coding,
        limit: usize,
    ) -> Result<Progress<H>, Error<R>>
    where
        R: route::Schema<RawHeaders = H>,
        H: Default,
    {
        let parsed = Block::scan::<R>(
            source.as_ref(),
            source.request().end(),
            limit,
            coding,
            source.request().version(),
        )?;
        match parsed {
            Scanned::Complete(block) => Ok(Progress::Complete(source, block)),
            Scanned::Pending(partial) => {
                if partial.input.len() > recv::HEAD_CAPACITY {
                    return Err(Error::Length {
                        len: partial.input.len(),
                    });
                }
                let Partial {
                    input: _,
                    headers,
                    fields,
                    flags,
                    count,
                    pos,
                } = partial;
                Self::try_new(source, headers, fields, flags, coding, (pos, count), limit)
                    .map(Progress::Pending)
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.source.as_ref().len()
    }

    pub(crate) fn advance<R>(mut self, incoming: &[u8]) -> Result<Progress<H>, Error<R>>
    where
        R: route::Schema<RawHeaders = H>,
    {
        self.source.try_extend(incoming).map_err(Error::Storage)?;
        let Self {
            source,
            mut headers,
            mut fields,
            mut flags,
            coding,
        } = self;
        let version = source.source().request().version();
        let progress = source
            .advance(|entry| {
                line::apply::<R>(
                    &mut headers,
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
                if source.as_ref().len() > recv::HEAD_CAPACITY {
                    return Err(Error::Length {
                        len: source.as_ref().len(),
                    });
                }
                Ok(Progress::Pending(Self {
                    source,
                    headers,
                    fields,
                    flags,
                    coding,
                }))
            }
            scan::Progress::Complete(complete) => {
                let (source, _, len) = complete.into_parts();
                if len.get() as usize > recv::HEAD_CAPACITY {
                    return Err(Error::Length {
                        len: len.get() as usize,
                    });
                }
                Ok(Progress::Complete(
                    source,
                    Block {
                        headers,
                        fields,
                        flags,
                        len,
                    },
                ))
            }
        }
    }
}
