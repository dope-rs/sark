use std::ops::ControlFlow;

use o3::buffer::{
    bytes::scope,
    write::{Sink as _, Slice, Stage},
};
use sark_http::{
    field,
    owner::{
        evidence,
        handoff::{self, stage},
    },
    payload::{
        compress::{self, Compression},
        response::{self, Encode, encoded, headers, status::Code},
    },
};

use crate::wire::output::sink::{
    self, Complete, Headers,
    error::Error,
    head::{Pending, emit_fields, emit_head, emit_tail},
    place_exact,
};

pub(super) struct Gzip<'a, H>(pub(super) &'a H);

const ENTRIES: [headers::Entry<'_>; 2] = [
    headers::Entry::new(
        field::literal!(headers::Name::try_bytes(b"content-encoding")),
        field::literal!(field::Value::try_bytes(b"gzip")),
    ),
    headers::Entry::new(
        field::literal!(headers::Name::try_bytes(b"vary")),
        field::literal!(field::Value::try_bytes(b"Accept-Encoding")),
    ),
];
pub(super) const FIELDS: headers::Group<'_> = field::literal!(headers::Group::try_new(
    &ENTRIES,
    b"content-encoding: gzip\r\nvary: Accept-Encoding\r\n",
));

impl<H: field::Source<headers::Application>> field::Source<headers::Application> for Gzip<'_, H> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field, headers::Application>) {
        self.0.emit(sink);
        sink.group(&FIELDS);
    }
}

pub(super) fn compress_exact<
    'out,
    'd,
    H: field::Source<headers::Application>,
    B: Encode,
    const EXACT: bool,
>(
    mut out: Slice<'out, &'out mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: encoded::Plan<B, EXACT>,
    gzip: &mut impl Compression,
    required: bool,
) -> Result<Complete<'d>, Error> {
    let len = plan.limit();
    let encodings = emit_fields(out.reborrow(), date, status, fields)?;
    let base = out.len();
    let mut tail = staged_tail(&mut out, date, len)?;
    let head = base + tail.body().len();
    if encodings != 0 {
        if required {
            return Err(Error::Encoding { actual: encodings });
        }
        tail.commit();
        return place_exact(out, head, plan);
    }
    head.checked_add(len).ok_or(Error::Overflow {
        base: head,
        added: len,
    })?;
    let zipped = if len <= tail.body().remaining() {
        plan.write(&mut tail.body())?;
        match encode(gzip, &tail.body().into_mut()[head - base..], required)? {
            compress::Output::Plain => {
                tail.commit();
                return Ok(Complete::copied(len));
            }
            compress::Output::Compressed(zipped) => zipped,
        }
    } else {
        let body = plan.into_shared()?;
        match encode(gzip, body.as_ref(), required)? {
            compress::Output::Plain => {
                tail.commit();
                return Ok(Complete::shared(body));
            }
            compress::Output::Compressed(zipped) => zipped,
        }
    };
    drop(tail);
    gzip_tail(out.reborrow(), date, zipped.len())?;
    Ok(finish(out, zipped, len, required))
}

pub(super) fn compress_body<'out, 'input, 'req, 'd, H, B, Q, O>(
    mut out: Slice<'out, &'out mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    body: B,
    gzip: &mut impl Compression,
    required: bool,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<ControlFlow<O::Deferred, Complete<'d>>, crate::wire::Failure>
where
    H: field::Source<headers::Application>,
    B: response::Body<'d>,
    Q: scope::Source,
    Q::Selection: Into<crate::Input<'input>>,
    O: stage::Body<'req, 'd>,
{
    let encodings = emit_fields(out.reborrow(), date, status, fields).map_err(Error::Wire)?;
    let mut tail = staged_tail(&mut out, date, body.len())?;
    if encodings != 0 {
        if required {
            return Err(Error::Encoding { actual: encodings }.into());
        }
        tail.commit();
        return sink::finish(body.try_resolve(&slot).map_err(Error::Owner)?);
    }
    let Some(raw) = body.as_slice() else {
        if required {
            return Err(Error::Compression(compress::Error::Unavailable).into());
        }
        tail.commit();
        return sink::finish(body.try_resolve(&slot).map_err(Error::Owner)?);
    };
    let read = raw.len();
    let zipped = match encode(gzip, raw, required)? {
        compress::Output::Plain => {
            let result = body
                .try_resolve_into(&slot, &mut tail.body())
                .map_err(Error::Owner)?;
            tail.commit();
            return match result {
                ControlFlow::Break(written) => Ok(ControlFlow::Continue(Complete::copied(written))),
                ControlFlow::Continue(plan) => sink::finish(plan),
            };
        }
        compress::Output::Compressed(zipped) => zipped,
    };
    drop(tail);
    gzip_tail(out.reborrow(), date, zipped.len())?;
    Ok(ControlFlow::Continue(finish(out, zipped, read, required)))
}

fn encode(
    gzip: &mut impl Compression,
    raw: &[u8],
    required: bool,
) -> Result<compress::Output, Error> {
    if required {
        gzip.try_encode(raw)
            .map(compress::Output::Compressed)
            .map_err(Error::Compression)
    } else {
        Ok(gzip.encode(raw))
    }
}

fn finish<'d>(
    mut out: Slice<'_, &mut usize>,
    body: compress::Frozen,
    read: usize,
    required: bool,
) -> Complete<'d> {
    let written = body.len();
    if required && out.write_slice(body.as_ref()).is_ok() {
        Complete::Copy {
            copy: evidence::Copy {
                read,
                written,
                cause: evidence::Cause::Compressed,
            },
        }
    } else {
        Complete::Compressed { body, read }
    }
}

fn staged_tail<'writer, 'out>(
    out: &'writer mut Slice<'out, &'out mut usize>,
    date: Headers<'_>,
    len: usize,
) -> Result<Stage<'writer, 'out, &'out mut usize>, Error> {
    let base = out.len();
    let mut tail = out.stage();
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
    'd,
    H: field::Source<headers::Application>,
    B: Encode,
    const EXACT: bool,
>(
    mut out: Slice<'out, &'out mut usize>,
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: encoded::Plan<B, EXACT>,
    gzip: &mut impl Compression,
    required: bool,
) -> Result<Complete<'d>, Error> {
    let max = plan.limit();
    if let Some(mut pending) = Pending::try_place(&mut out, date, status, fields, max)? {
        let len = pending.encode(plan.into_body())?;
        if (required || pending.encodings() == 0)
            && let compress::Output::Compressed(zipped) = encode(gzip, pending.body(), required)?
        {
            drop(pending);
            gzip_head(out.reborrow(), date, status, fields, zipped.len())?;
            return Ok(finish(out, zipped, len, required));
        }
        pending.finish::<()>()?;
        return Ok(Complete::copied(len));
    }
    let body = plan.into_shared()?;
    let len = body.len();
    let encodings = emit_fields(out.reborrow(), date, status, fields)?;
    let tail = staged_tail(&mut out, date, len)?;
    if encodings != 0 {
        if required {
            return Err(Error::Encoding { actual: encodings });
        }
        tail.commit();
        return Ok(Complete::shared(body));
    }
    let zipped = match encode(gzip, body.as_ref(), required)? {
        compress::Output::Plain => {
            tail.commit();
            return Ok(Complete::shared(body));
        }
        compress::Output::Compressed(zipped) => zipped,
    };
    drop(tail);
    gzip_tail(out.reborrow(), date, zipped.len())?;
    Ok(finish(out, zipped, len, required))
}

fn gzip_head<H: field::Source<headers::Application>>(
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
