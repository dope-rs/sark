use o3::buffer::bytes::scope;
use sark_http::{
    field,
    owner::{
        evidence,
        handoff::{self, stage},
    },
    payload::{
        compress::{self, Compression},
        response::{bounded, exact, status::Code},
    },
};

use crate::wire::output::sink::{
    self, Headers,
    error::Error,
    head::{Emitted, Layout, Pending, emit_head},
    place_exact,
    written::Written,
};

pub(super) struct Gzip<'a, H>(pub(super) &'a H);

impl<H: field::Source> field::Source for Gzip<'_, H> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        self.0.emit(sink);
        sink.field(
            field::literal!(field::Lower::try_bytes(b"content-encoding")),
            field::literal!(field::Value::try_bytes(b"gzip")),
        );
        sink.field(
            field::literal!(field::Lower::try_bytes(b"vary")),
            field::literal!(field::Value::try_bytes(b"Accept-Encoding")),
        );
    }
}

pub(super) fn compress_exact<
    'req,
    'd,
    H: field::Source,
    B: exact::TryBody,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
>(
    out: &mut [u8],
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: exact::Plan<B>,
    gzip: &mut impl Compression,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
    let len = plan.len();
    let mut head = Emitted::new(out, date, status, fields, len)?;
    if head.encodings() != 0 {
        let len = head.len();
        return place_exact(head.into_inner(), len, plan, slot);
    }
    let end = head.len().checked_add(len).ok_or(Error::Overflow {
        base: head.len(),
        added: len,
    })?;
    if let Some(body) = head.body(end) {
        plan.encode(body)?;
        let zipped = match gzip.encode(body) {
            compress::Output::Plain => {
                return Ok(Written::Inline {
                    written: end,
                    body_bytes: len,
                    cause: evidence::Cause::Inline,
                });
            }
            compress::Output::Compressed(zipped) => zipped,
        };
        let head = head.gzip(date, zipped.len())?;
        return Ok(Written::Compressed {
            head,
            body: zipped,
            read: len,
        });
    }
    let body = plan.into_shared()?;
    let zipped = match gzip.encode(body.as_ref()) {
        compress::Output::Plain => return sink::stage(slot, head.len(), body),
        compress::Output::Compressed(zipped) => zipped,
    };
    let head = head.gzip(date, zipped.len())?;
    Ok(Written::Compressed {
        head,
        body: zipped,
        read: len,
    })
}

pub(super) fn compress_bounded<
    'req,
    'd,
    H: field::Source,
    B: bounded::Body,
    Q: scope::Source,
    O: stage::Body<'req, 'd>,
>(
    out: &mut [u8],
    date: Headers<'_>,
    status: Code,
    fields: &H,
    plan: bounded::Plan<B>,
    gzip: &mut impl Compression,
    slot: handoff::Slot<'req, 'd, Q, O>,
) -> Result<Written<'d, O::Deferred, Q::Selection>, Error> {
    let max = plan.max_len();
    let layout = Layout::new(date, status, fields, max)?;
    if let Some(pending) = Pending::place(&layout, out) {
        let encoded = pending.encode(plan)?;
        let len = encoded.body().len();
        if encoded.encodings() == 0
            && let compress::Output::Compressed(zipped) = gzip.encode(encoded.body())
        {
            let head = gzip_head(out, date, status, fields, zipped.len())?;
            return Ok(Written::Compressed {
                head,
                body: zipped,
                read: len,
            });
        }
        let (written, _) = encoded.finish()?;
        return Ok(Written::Inline {
            written,
            body_bytes: len,
            cause: evidence::Cause::Inline,
        });
    }
    let body = plan.into_shared()?;
    let len = body.len();
    let plain = Emitted::new(out, date, status, fields, len)?;
    if plain.encodings() != 0 {
        return sink::stage(slot, plain.len(), body);
    }
    let zipped = match gzip.encode(body.as_ref()) {
        compress::Output::Plain => return sink::stage(slot, plain.len(), body),
        compress::Output::Compressed(zipped) => zipped,
    };
    let head = plain.gzip(date, zipped.len())?;
    Ok(Written::Compressed {
        head,
        body: zipped,
        read: len,
    })
}

pub(super) fn gzip_head<H: field::Source>(
    out: &mut [u8],
    date: Headers<'_>,
    status: Code,
    fields: &H,
    len: usize,
) -> Result<usize, Error> {
    let (head, encodings) = emit_head(out, date, status, &Gzip(fields), len)?;
    if encodings != 1 {
        return Err(Error::Encoding { actual: encodings });
    }
    Ok(head)
}
