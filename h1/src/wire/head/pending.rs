use std::{num::NonZero, ops::ControlFlow};

use o3::buffer::write::Sink as _;
use sark_core::route;
use sark_http::{
    field::head,
    h1::{
        field::scan,
        head::{line::Owned, request::Fields},
    },
};

use crate::wire::{
    head::{Admission, Block, Error, Parsed, line},
    parsing, recv,
};

type Source = Owned<{ recv::BODY_CAPACITY }>;

pub(crate) struct Scan<C, H, A, const NEGOTIATE: bool> {
    pub(super) cursor: C,
    pub(super) headers: H,
    pub(super) admission: A,
    pub(super) fields: Fields,
    pub(super) flags: head::Flags,
}

pub(super) type Partial<H, A, const NEGOTIATE: bool> = Scan<(usize, usize), H, A, NEGOTIATE>;
/// Typed completed fields and append-only ownership of the uncompleted head.
pub(crate) type Pending<H, A, const NEGOTIATE: bool> = Scan<scan::State<Source>, H, A, NEGOTIATE>;
pub(crate) type Progress<H, A, const NEGOTIATE: bool> =
    ControlFlow<Pending<H, A, NEGOTIATE>, (Source, Parsed<H, A>)>;

impl<H, A: Admission, const NEGOTIATE: bool> Partial<H, A, NEGOTIATE> {
    pub(crate) fn retain<R: route::Schema<RawHeaders = H>>(
        self,
        head: &parsing::Head<'_>,
        limit: usize,
    ) -> Result<Pending<H, A, NEGOTIATE>, Error<R>> {
        let capacity = NonZero::new(head.input().as_slice().len()).unwrap_or(NonZero::<usize>::MIN);
        let source = head
            .request()
            .try_snapshot(capacity)
            .map_err(Error::Storage)?;
        self.bind(source, limit)
    }

    fn bind<R: route::Schema<RawHeaders = H>>(
        self,
        source: Source,
        limit: usize,
    ) -> Result<Pending<H, A, NEGOTIATE>, Error<R>> {
        let (pos, count) = self.cursor;
        let cursor = scan::State::try_new(source, pos, count, limit, head::MAX_HEADER_LINE_BYTES)
            .map_err(|(_, error)| Error::Syntax(error))?;
        Ok(Pending {
            cursor,
            headers: self.headers,
            admission: self.admission,
            fields: self.fields,
            flags: self.flags,
        })
    }
}

impl<H, A: Admission, const NEGOTIATE: bool> Pending<H, A, NEGOTIATE> {
    pub(crate) fn try_start<R>(
        source: Source,
        limit: usize,
    ) -> Result<Progress<H, A, NEGOTIATE>, Error<R>>
    where
        R: route::Schema<RawHeaders = H>,
        H: Default,
    {
        let parsed = Block::<H, A>::scan::<R, NEGOTIATE>(
            source.as_ref(),
            source.request().end(),
            limit,
            source.request().version(),
        )?;
        match parsed {
            ControlFlow::Continue(block) => {
                let head = Parsed::from_block::<R>(block, &source.request())?;
                Ok(ControlFlow::Continue((source, head)))
            }
            ControlFlow::Break(partial) => partial.bind(source, limit).map(ControlFlow::Break),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.cursor.as_ref().len()
    }

    pub(crate) fn advance<R>(
        mut self,
        incoming: &[u8],
    ) -> Result<Progress<H, A, NEGOTIATE>, Error<R>>
    where
        R: route::Schema<RawHeaders = H>,
    {
        let prior = self.len();
        let maximum = A::MAX_BYTES.min(recv::HEAD_CAPACITY);
        let (prefix, tail) = incoming.split_at(incoming.len().min(maximum.saturating_sub(prior)));
        self.cursor.try_extend(prefix).map_err(Error::Storage)?;
        let version = self.cursor.source().request().version();
        let progress = self
            .cursor
            .advance(|entry| {
                line::apply::<R, A, NEGOTIATE>(
                    &mut self.headers,
                    &mut self.admission,
                    &mut self.fields,
                    &mut self.flags,
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
                Ok(ControlFlow::Break(Self {
                    cursor: source,
                    ..self
                }))
            }
            scan::Progress::Complete(complete) => {
                let (mut source, _, len) = complete.into_parts();
                let block = Block {
                    headers: self.headers,
                    admission: self.admission,
                    fields: self.fields,
                    len,
                };
                let head = Parsed::from_block::<R>(block, &source.request())?;
                source.write_slice(tail).map_err(Error::Storage)?;
                Ok(ControlFlow::Continue((source, head)))
            }
        }
    }
}
