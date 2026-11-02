use o3::buffer::storage;
use sark_http::{
    field,
    payload::response::{self, bounded, exact, status::Code},
};

use crate::wire::{
    cache,
    output::sink::{
        self, Headers,
        head::{Layout, Pending, render_framed_head},
    },
    response::{Error, framing},
};

pub(super) fn body<'d, H, B>(
    status: Code,
    fields: &H,
    body: B,
    date: Headers<'_>,
) -> Result<cache::Entry, sink::Error>
where
    H: field::Source,
    B: response::Body<'d>,
{
    let Some(raw) = body.as_slice() else {
        return Err(sink::Error::Cache);
    };
    render(status, fields, raw.len(), date, |out| {
        out.copy_from_slice(raw);
        Ok(())
    })
}

fn render<H: field::Source>(
    status: Code,
    fields: &H,
    len: usize,
    date: Headers<'_>,
    encode: impl FnOnce(&mut [u8]) -> Result<(), sink::Error>,
) -> Result<cache::Entry, sink::Error> {
    let (mut bytes, off, head) =
        render_framed_head(status, fields, framing::Length(len), len, date)?;
    let end = bytes.len();
    let out = bytes.get_mut(head..end).ok_or(Error::WriteCapacity {
        required: end,
        actual: end,
    })?;
    encode(out)?;
    Ok(cache::Entry::try_new(
        bytes.freeze(),
        off,
        len,
        date.response,
    )?)
}

pub(super) fn exact<H: field::Source, B: exact::TryBody>(
    status: Code,
    fields: &H,
    plan: exact::Plan<B>,
    date: Headers<'_>,
) -> Result<cache::Entry, sink::Error> {
    render(status, fields, plan.len(), date, |out| {
        plan.encode(out).map_err(sink::Error::from)
    })
}

pub(super) fn bounded<H: field::Source, B: bounded::Body>(
    status: Code,
    fields: &H,
    plan: bounded::Plan<B>,
    date: Headers<'_>,
) -> Result<cache::Entry, sink::Error> {
    let max = plan.max_len();
    let layout = Layout::new(date, status, fields, max)?;
    let cap = layout.capacity();
    let mut bytes = storage::Owned::try_filled(cap, 0).map_err(Error::Storage)?;
    let actual = bytes.len();
    let Some(pending) = Pending::place(&layout, &mut bytes) else {
        return Err(Error::HeadCapacity {
            required: cap,
            actual,
        }
        .into());
    };
    let encoded = pending.encode(plan)?;
    let len = encoded.body().len();
    let (used, offset) = encoded.finish()?;
    bytes.truncate(used);
    Ok(cache::Entry::try_new(
        bytes.freeze(),
        offset,
        len,
        date.response,
    )?)
}

pub(super) fn empty<H: field::Source>(
    status: Code,
    fields: &H,
    frame: framing::Empty,
    date: Headers<'_>,
) -> Result<cache::Entry, sink::Error> {
    let (bytes, offset, _) = render_framed_head(status, fields, frame, 0, date)?;
    Ok(cache::Entry::try_new(
        bytes.freeze(),
        offset,
        0,
        date.response,
    )?)
}
