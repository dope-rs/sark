use o3::buffer::{
    storage,
    write::{Slice, Split},
};
use sark_http::{
    field,
    payload::response::{bounded, status::Code},
};

use crate::wire::{
    output::sink::{Error, Headers},
    response::{
        self, consts,
        framing::{self, Framing as _},
    },
};

pub(super) fn emit_fields(
    out: Slice<'_, &mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &impl field::Source,
) -> Result<usize, response::Error> {
    let mut out = response::Writer::exact(out);
    out.put_status(date.response.version(), status)?;
    out.fields(fields)
}

pub(super) fn emit_tail(
    out: Slice<'_, &mut usize>,
    date: Headers<'_>,
    len: usize,
) -> Result<(), response::Error> {
    let mut out = response::Writer::exact(out);
    framing::Length(len).write(&mut out)?;
    out.end_head(date.response, date.value, date.server, date.dated)?;
    Ok(())
}

pub(super) struct Layout<'date, 'field, H> {
    date: Headers<'date>,
    fields: &'field H,
    status: Code,
    head: usize,
    capacity: usize,
    encodings: usize,
}

pub(super) struct Pending<'writer, 'out, 'date, 'field, H> {
    split: Split<'writer, 'out, &'out mut usize>,
    date: Headers<'date>,
    fields: &'field H,
    status: Code,
    encodings: usize,
}

pub(super) fn emit_head<H: field::Source, C: response::Count>(
    out: Slice<'_, &mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    len: usize,
) -> Result<(usize, C), response::Error> {
    emit_framed_head(out, date, status, fields, framing::Length(len))
}

pub(super) fn emit_framed_head<H: field::Source, F: framing::Framing, C: response::Count>(
    out: Slice<'_, &mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    frame: F,
) -> Result<(usize, C), response::Error> {
    let head = response::Head {
        response: date.response,
        status,
        headers: fields,
        framing: frame,
    };
    let mut writer = response::Writer::exact(out);
    let (_, count) = head.emit(&mut writer, date.value, date.server, date.dated)?;
    Ok((writer.finish(), count))
}

pub(super) fn render_framed_head<H, F>(
    status: Code,
    fields: &H,
    frame: F,
    tail: usize,
    date: Headers<'_>,
) -> Result<(storage::Owned, Option<usize>, usize), response::Error>
where
    H: field::Source,
    F: framing::Framing,
{
    let head = response::Head {
        response: date.response,
        status,
        headers: fields,
        framing: frame,
    };
    let plan = head.plan(date.server, date.dated)?;
    let expected = plan.len();
    let total = expected
        .checked_add(tail)
        .ok_or(response::Error::PlanOverflow {
            current: expected,
            added: tail,
        })?;
    let mut bytes = storage::Owned::try_filled(total, 0).map_err(response::Error::Storage)?;
    let mut out = Slice::new(&mut bytes[..expected]);
    let mut writer = response::Writer::exact(out.reborrow());
    let (off, ()) = head.emit(
        &mut writer,
        consts::PLACEHOLDER_DATE,
        date.server,
        date.dated,
    )?;
    let len = writer.finish();
    if len != expected {
        return Err(response::Error::HeadLength {
            expected,
            actual: len,
        });
    }
    Ok((bytes, off, len))
}

impl<'date, 'field, H> Layout<'date, 'field, H>
where
    H: field::Source,
{
    pub(super) fn new(
        date: Headers<'date>,
        status: Code,
        fields: &'field H,
        max: usize,
    ) -> Result<Self, response::Error> {
        let head = response::Head {
            response: date.response,
            status,
            headers: fields,
            framing: framing::Padded(0),
        };
        let plan = head.plan(date.server, date.dated)?;
        let len = plan.len();
        let required = len.checked_add(max).ok_or(response::Error::PlanOverflow {
            current: len,
            added: max,
        })?;
        Ok(Self {
            date,
            fields,
            status,
            head: len,
            capacity: required,
            encodings: plan.encodings(),
        })
    }

    pub(super) const fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<'writer, 'out, 'date, 'field, H> Pending<'writer, 'out, 'date, 'field, H> {
    /// Places disjoint slices using `Layout::new`'s checked `capacity = head + max`.
    /// Layout's private fields preserve this proof through placement.
    pub(super) fn place(
        layout: &Layout<'date, 'field, H>,
        out: &'writer mut Slice<'out, &'out mut usize>,
    ) -> Option<Self> {
        let split = out
            .try_split(layout.head, layout.capacity - layout.head)
            .ok()?;
        Some(Self {
            split,
            date: layout.date,
            fields: layout.fields,
            status: layout.status,
            encodings: layout.encodings,
        })
    }

    pub(super) fn encode<B: bounded::Body>(
        &mut self,
        plan: bounded::Plan<B>,
    ) -> Result<usize, Error> {
        plan.encode(&mut self.split.body()).map_err(Into::into)
    }
}

impl<H: field::Source> Pending<'_, '_, '_, '_, H> {
    pub(super) const fn encodings(&self) -> usize {
        self.encodings
    }

    pub(super) fn body(&mut self) -> &[u8] {
        self.split.body().into_mut()
    }

    pub(super) fn finish(mut self) -> Result<Option<usize>, response::Error> {
        let body = self.split.body().len();
        let head = response::Head {
            response: self.date.response,
            status: self.status,
            headers: self.fields,
            framing: framing::Padded(body),
        };
        let mut writer = response::Writer::exact(self.split.head());
        let (off, ()) = head.emit(
            &mut writer,
            self.date.value,
            self.date.server,
            self.date.dated,
        )?;
        self.split.try_commit().map_err(|error| {
            let (expected, actual) = error.into_parts();
            response::Error::HeadLength {
                expected: expected.get(),
                actual,
            }
        })?;
        Ok(off)
    }
}
