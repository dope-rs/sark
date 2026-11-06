use o3::buffer::storage;
use sark_http::{
    field,
    payload::response::{bounded, status::Code},
};

use crate::wire::{
    output::sink::{Error, Headers, gzip::FIELDS},
    response::{
        self, consts,
        framing::{self, Framing as _},
    },
};

/// Retains the emitted fields and their exact output while framing may change.
pub(super) struct Emitted<'out> {
    out: response::Writer<'out>,
    len: usize,
    encodings: usize,
}

impl<'out> Emitted<'out> {
    pub(super) fn new(
        out: &'out mut [u8],
        date: Headers<'_>,
        status: Code,
        fields: &impl field::Source,
        len: usize,
    ) -> Result<Self, response::Error> {
        let mut out = response::Writer::exact(out);
        out.put_status(date.response.version(), status)?;
        let count = out.fields(fields)?;
        let mut tail = out.reborrow();
        framing::Length(len).write(&mut tail)?;
        tail.end_head(date.response, date.value, date.server, date.dated)?;
        let len = tail.finish();
        Ok(Self {
            out,
            len,
            encodings: count,
        })
    }

    pub(super) const fn len(&self) -> usize {
        self.len
    }

    pub(super) const fn encodings(&self) -> usize {
        self.encodings
    }

    pub(super) fn body(&mut self, end: usize) -> Option<&mut [u8]> {
        self.out.get_mut(self.len..end)
    }

    pub(super) fn into_inner(self) -> &'out mut [u8] {
        self.out.into_inner()
    }

    pub(super) fn gzip(mut self, date: Headers<'_>, len: usize) -> Result<usize, response::Error> {
        self.out.group(&FIELDS, ())?;
        framing::Length(len).write(&mut self.out)?;
        self.out
            .end_head(date.response, date.value, date.server, date.dated)?;
        Ok(self.out.finish())
    }
}

pub(super) struct Layout<'date, 'field, H> {
    date: Headers<'date>,
    fields: &'field H,
    status: Code,
    head: usize,
    capacity: usize,
    encodings: usize,
}

pub(super) struct Pending<'out, 'date, 'field, H> {
    head: &'out mut [u8],
    body: &'out mut [u8],
    date: Headers<'date>,
    fields: &'field H,
    status: Code,
    encodings: usize,
}

pub(super) struct Encoded<'out, 'date, 'field, H>(Pending<'out, 'date, 'field, H>);

pub(super) fn emit_head<H: field::Source, C: response::Count>(
    out: &mut [u8],
    date: Headers<'_>,
    status: Code,
    fields: &H,
    len: usize,
) -> Result<(usize, C), response::Error> {
    emit_framed_head(out, date, status, fields, framing::Length(len))
}

pub(super) fn emit_framed_head<H: field::Source, F: framing::Framing, C: response::Count>(
    out: &mut [u8],
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
    let mut writer = response::Writer::new(&mut bytes, expected)?;
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

impl<'out, 'date, 'field, H> Pending<'out, 'date, 'field, H> {
    /// Places disjoint slices using `Layout::new`'s checked `capacity = head + max`.
    /// Layout's private fields preserve this proof through placement.
    pub(super) fn place(layout: &Layout<'date, 'field, H>, out: &'out mut [u8]) -> Option<Self> {
        let (head, body) = out.split_at_mut_checked(layout.head)?;
        let body = body.get_mut(..layout.capacity - layout.head)?;
        Some(Self {
            head,
            body,
            date: layout.date,
            fields: layout.fields,
            status: layout.status,
            encodings: layout.encodings,
        })
    }

    pub(super) fn encode<B: bounded::Body>(
        mut self,
        plan: bounded::Plan<B>,
    ) -> Result<Encoded<'out, 'date, 'field, H>, Error> {
        self.body = plan.encode(self.body)?;
        Ok(Encoded(self))
    }
}

impl<H: field::Source> Encoded<'_, '_, '_, H> {
    pub(super) const fn encodings(&self) -> usize {
        self.0.encodings
    }

    pub(super) fn body(&self) -> &[u8] {
        self.0.body
    }

    pub(super) fn finish(self) -> Result<(usize, Option<usize>), response::Error> {
        let pending = self.0;
        let expected = pending.head.len();
        let head = response::Head {
            response: pending.date.response,
            status: pending.status,
            headers: pending.fields,
            framing: framing::Padded(pending.body.len()),
        };
        let mut writer = response::Writer::exact(pending.head);
        let (off, ()) = head.emit(
            &mut writer,
            pending.date.value,
            pending.date.server,
            pending.date.dated,
        )?;
        let len = writer.finish();
        if len != expected {
            return Err(response::Error::HeadLength {
                expected,
                actual: len,
            });
        }
        // Placement split one output; encoding only shortened its body prefix.
        Ok((len + pending.body.len(), off))
    }
}
