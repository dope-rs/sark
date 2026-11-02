use std::convert::Infallible;

use o3::buffer::{pool, pool::owners};
use sark_http::{
    owner::{evidence, handoff},
    payload::compress::Gzip,
};

use crate::wire::{cache, output::sink::error::Error};

pub(in crate::wire::output) enum Written<'d, P = Infallible, H = Infallible> {
    Inline {
        written: usize,
        body_bytes: usize,
        cause: evidence::Cause,
    },
    Compressed {
        head: usize,
        body: pool::Span<
            owners::Plain<pool::Fixed<{ Gzip::SLOTS.get() }, { Gzip::CAPACITY.get() }>>,
            pool::Owned,
            pool::Frozen,
        >,
        read: usize,
    },
    Body {
        head: usize,
        body: handoff::Ready<'d>,
    },
    Head {
        written: usize,
        input: H,
    },
    RequestBody {
        head: usize,
        body: P,
    },
}

const _: () = {
    assert!(core::mem::size_of::<Written<'static>>() <= 8 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Written<'static, usize>>() <= 8 * core::mem::size_of::<usize>());
};

impl<'d, P, H> Written<'d, P, H> {
    pub(super) fn cached(
        out: &mut [u8],
        date: &[u8; 29],
        entry: cache::Entry,
        cache: cache::Cache<'_>,
    ) -> Result<Self, Error> {
        let written = entry.try_write(out, date);
        cache.insert(entry);
        Ok(match written?.ok_or(Error::Cache)? {
            cache::Cached::Fixed { len, body } => Self::Inline {
                written: len,
                body_bytes: body,
                cause: evidence::Cause::Cached,
            },
            cache::Cached::Shared { head, body } => Self::Body {
                head,
                body: handoff::Ready::Retained(body),
            },
        })
    }
}
