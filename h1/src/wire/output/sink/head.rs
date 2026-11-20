use o3::buffer::{
    storage,
    write::{Slice, Split},
};
use sark_http::{
    field,
    payload::response::{Encode, status::Code},
};

use crate::wire::{
    output::sink::{Error, Headers},
    response::{
        self,
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
    out.end_head(date.response, date.value, date.server, date.dated)
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
    let count = head.emit(&mut writer, date.value, date.server, date.dated)?;
    Ok((writer.finish(), count))
}

impl<'writer, 'out, 'date, 'field, H: field::Source> Pending<'writer, 'out, 'date, 'field, H> {
    pub(super) fn try_place(
        out: &'writer mut Slice<'out, &'out mut usize>,
        date: Headers<'date>,
        status: Code,
        fields: &'field H,
        max: usize,
    ) -> Result<Option<Self>, Error> {
        let (head, encodings) = response::Head {
            response: date.response,
            status,
            headers: fields,
            framing: framing::Padded(0),
        }
        .plan(date.server, date.dated)?;
        let Ok(split) = out.try_split(head, max) else {
            head.checked_add(max).ok_or(response::Error::PlanOverflow {
                current: head,
                added: max,
            })?;
            return Ok(None);
        };
        Ok(Some(Self {
            split,
            date,
            fields,
            status,
            encodings,
        }))
    }

    pub(super) fn encode(&mut self, body: impl Encode) -> Result<usize, Error> {
        let mut out = self.split.body();
        body.encode(&mut out).map_err(storage::BuildError::Build)?;
        Ok(out.len())
    }
    pub(super) const fn encodings(&self) -> usize {
        self.encodings
    }

    pub(super) fn body(&mut self) -> &[u8] {
        self.split.body().into_mut()
    }

    pub(super) fn finish<C: response::Count>(mut self) -> Result<C, response::Error> {
        let body = self.split.body().len();
        let head = response::Head {
            response: self.date.response,
            status: self.status,
            headers: self.fields,
            framing: framing::Padded(body),
        };
        let mut writer = response::Writer::exact(self.split.head());
        let encoded = head.emit::<C>(
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
        Ok(encoded)
    }
}
