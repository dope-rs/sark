use o3::buffer::write::Slice;
use std::convert::Infallible;

use sark_http::{
    owner::{evidence, handoff},
    payload::compress,
};

use crate::wire::{cache, output::sink::error::Error};

pub(in crate::wire::output) enum Written<'d, P = Infallible, H = Infallible> {
    Inline { copy: evidence::Copy },
    Compressed { body: compress::Frozen, read: usize },
    Body { body: handoff::Ready<'d> },
    Head { input: H },
    RequestBody { body: P },
}

const _: () = {
    assert!(core::mem::size_of::<Written<'static>>() <= 8 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Written<'static, usize>>() <= 8 * core::mem::size_of::<usize>());
};

impl<'d, P, H> Written<'d, P, H> {
    pub(super) const fn inline(body_bytes: usize) -> Self {
        Self::Inline {
            copy: evidence::Copy {
                read: body_bytes,
                written: body_bytes,
                cause: evidence::Cause::Inline,
            },
        }
    }

    pub(super) fn cached(
        out: Slice<'_, &mut usize>,
        date: &[u8; 29],
        entry: cache::Entry,
        cache: cache::Cache<'_>,
    ) -> Result<Self, Error> {
        let written = entry.try_write(out, date);
        cache.insert(entry);
        Ok(match written?.ok_or(Error::Cache)? {
            cache::Cached::Fixed { body } => Self::Inline {
                copy: evidence::Copy {
                    read: body,
                    written: body,
                    cause: evidence::Cause::Cached,
                },
            },
            cache::Cached::Shared { body } => Self::Body {
                body: handoff::Ready::Retained(body),
            },
        })
    }
}
