use std::convert::Infallible;

use sark_http::{
    owner::{evidence, handoff},
    payload::compress,
};

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
}
