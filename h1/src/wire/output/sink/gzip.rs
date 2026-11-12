use std::ops::ControlFlow;

use o3::buffer::{
    bytes::scope,
    write::{Slice, Split},
};
use sark_http::{
    field,
    owner::handoff::{self, stage},
    payload::{
        compress::{self, Compression},
        response::{self, bounded, exact, status::Code},
    },
};

use crate::wire::output::sink::{
    self, Headers,
    error::Error,
    head::{Layout, Pending, emit_fields, emit_head, emit_tail},
    place_exact,
    written::Written,
};

pub(super) struct Gzip<'a, H>(pub(super) &'a H);

const ENTRIES: [field::Entry<'_>; 2] = [
    field::Entry::new(
        field::literal!(field::Lower::try_bytes(b"content-encoding")),
        field::literal!(field::Value::try_bytes(b"gzip")),
    ),
    field::Entry::new(
        field::literal!(field::Lower::try_bytes(b"vary")),
        field::literal!(field::Value::try_bytes(b"Accept-Encoding")),
    ),
];
pub(super) const FIELDS: field::Group<'_> = field::literal!(field::Group::try_new(
    &ENTRIES,
    b"content-encoding: gzip\r\nvary: Accept-Encoding\r\n",
));

impl<H: field::Source> field::Source for Gzip<'_, H> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        self.0.emit(sink);
        sink.group(&FIELDS);
    }
}

pub(super) fn compress_exact<
    'out,
    'req,
    'd,
    H: field::Source,
    B: exact::TryBody,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
>(
    mut out: Slice<'out, &'out mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: exact::Plan<B>,
    gzip: &mut impl Compression,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
    let len = plan.len();
    let encodings = emit_fields(out.reborrow(), date, status, fields)?;
    let base = out.len();
    let mut tail = staged_tail(&mut out, date, len)?;
    let head = base + tail.body().len();
    if encodings != 0 {
        tail.try_commit().unwrap_or_else(|_| unreachable!("head"));
        return place_exact(out, head, plan, slot);
    }
    head.checked_add(len).ok_or(Error::Overflow {
        base: head,
        added: len,
    })?;
    let zipped = if len <= tail.body().remaining() {
        plan.encode(&mut tail.body())?;
        match gzip.encode(&tail.body().into_mut()[head - base..]) {
            compress::Output::Plain => {
                tail.try_commit().unwrap_or_else(|_| unreachable!("head"));
                return Ok(Written::inline(len));
            }
            compress::Output::Compressed(zipped) => zipped,
        }
    } else {
        let body = plan.into_shared()?;
        match gzip.encode(body.as_ref()) {
            compress::Output::Plain => {
                tail.try_commit().unwrap_or_else(|_| unreachable!("head"));
                return sink::stage(slot, body);
            }
            compress::Output::Compressed(zipped) => zipped,
        }
    };
    drop(tail);
    gzip_tail(out, date, zipped.len())?;
    Ok(Written::Compressed {
        body: zipped,
        read: len,
    })
}

pub(super) fn compress_body<'out, 'req, 'd, H, B, Q, O>(
    mut out: Slice<'out, &'out mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    body: B,
    gzip: &mut impl Compression,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error>
where
    H: field::Source,
    B: response::Body<'d>,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
{
    let encodings = emit_fields(out.reborrow(), date, status, fields)?;
    let mut tail = staged_tail(&mut out, date, body.len())?;
    if encodings != 0 {
        tail.try_commit().unwrap_or_else(|_| unreachable!("head"));
        return sink::stage(slot, body);
    }
    let Some(raw) = body.as_slice() else {
        tail.try_commit().unwrap_or_else(|_| unreachable!("head"));
        return sink::stage(slot, body);
    };
    let read = raw.len();
    let zipped = match gzip.encode(raw) {
        compress::Output::Plain => {
            let written = match body.try_resolve_into(&slot, &mut tail.body())? {
                ControlFlow::Break(written) => Written::inline(written),
                ControlFlow::Continue(plan) => sink::staged(plan.stage()),
            };
            tail.try_commit().unwrap_or_else(|_| unreachable!("head"));
            return Ok(written);
        }
        compress::Output::Compressed(zipped) => zipped,
    };
    drop(tail);
    gzip_tail(out, date, zipped.len())?;
    Ok(Written::Compressed { body: zipped, read })
}

fn staged_tail<'writer, 'out>(
    out: &'writer mut Slice<'out, &'out mut usize>,
    date: Headers<'_>,
    len: usize,
) -> Result<Split<'writer, 'out, &'out mut usize>, Error> {
    let base = out.len();
    let mut tail = out
        .try_split(0, out.remaining())
        .unwrap_or_else(|_| unreachable!("head"));
    emit_tail(tail.body(), date, len).map_err(|error| match error {
        crate::wire::response::Error::WriteCapacity { required, actual } => {
            crate::wire::response::Error::WriteCapacity {
                required: base + required,
                actual: base + actual,
            }
        }
        crate::wire::response::Error::WriteOverflow { current, added } => {
            crate::wire::response::Error::WriteOverflow {
                current: base + current,
                added,
            }
        }
        error => error,
    })?;
    Ok(tail)
}

fn gzip_tail(mut out: Slice<'_, &mut usize>, date: Headers<'_>, len: usize) -> Result<(), Error> {
    crate::wire::response::Writer::exact(out.reborrow()).group(&FIELDS, ())?;
    emit_tail(out, date, len)?;
    Ok(())
}

pub(super) fn compress_bounded<
    'out,
    'req,
    'd,
    H: field::Source,
    B: bounded::Body,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
>(
    mut out: Slice<'out, &'out mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: bounded::Plan<B>,
    gzip: &mut impl Compression,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
    let max = plan.max_len();
    let layout = Layout::new(date, status, fields, max)?;
    if let Some(mut pending) = Pending::place(&layout, &mut out) {
        let len = pending.encode(plan)?;
        if pending.encodings() == 0
            && let compress::Output::Compressed(zipped) = gzip.encode(pending.body())
        {
            drop(pending);
            gzip_head(out, date, status, fields, zipped.len())?;
            return Ok(Written::Compressed {
                body: zipped,
                read: len,
            });
        }
        pending.finish()?;
        return Ok(Written::inline(len));
    }
    let body = plan.into_shared()?;
    let len = body.len();
    let encodings = emit_fields(out.reborrow(), date, status, fields)?;
    let tail = staged_tail(&mut out, date, len)?;
    if encodings != 0 {
        tail.try_commit().unwrap_or_else(|_| unreachable!("head"));
        return sink::stage(slot, body);
    }
    let zipped = match gzip.encode(body.as_ref()) {
        compress::Output::Plain => {
            tail.try_commit().unwrap_or_else(|_| unreachable!("head"));
            return sink::stage(slot, body);
        }
        compress::Output::Compressed(zipped) => zipped,
    };
    drop(tail);
    gzip_tail(out, date, zipped.len())?;
    Ok(Written::Compressed {
        body: zipped,
        read: len,
    })
}

fn gzip_head<H: field::Source>(
    out: Slice<'_, &mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    len: usize,
) -> Result<(), Error> {
    let (_, encodings) = emit_head(out, date, status, &Gzip(fields), len)?;
    if encodings != 1 {
        return Err(Error::Encoding { actual: encodings });
    }
    Ok(())
}
