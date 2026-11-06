use std::convert::Infallible;

use sark_http::{
    owner::{evidence, handoff},
    payload::compress,
};

use crate::wire::{cache, output::sink::error::Error};

pub(in crate::wire::output) enum Written<'d, P = Infallible, H = Infallible> {
    Inline {
        written: usize,
        copy: evidence::Copy,
    },
    Compressed {
        head: usize,
        body: compress::Frozen,
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
    pub(super) const fn inline(written: usize, body_bytes: usize) -> Self {
        Self::Inline {
            written,
            copy: evidence::Copy {
                read: body_bytes,
                written: body_bytes,
                cause: evidence::Cause::Inline,
            },
        }
    }

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
                copy: evidence::Copy {
                    read: body,
                    written: body,
                    cause: evidence::Cause::Cached,
                },
            },
            cache::Cached::Shared { head, body } => Self::Body {
                head,
                body: handoff::Ready::Retained(body),
            },
        })
    }
}
