use core::mem;

use o3::buffer::{pool, pool::owners, storage};
use sark_http::payload::{compress::Gzip, fixed, owned};

use crate::wire::{
    head,
    output::{Copy, Outcome},
};

/// Transport plan after lifecycle evidence has been split from ownership.
pub enum Plan<'d> {
    Inline {
        written: usize,
        connection: head::Connection,
    },
    Static {
        hdr_written: usize,
        body: fixed::Body<'d>,
        connection: head::Connection,
    },
    Shared {
        hdr_written: usize,
        body: storage::Shared,
        connection: head::Connection,
    },
    Frozen {
        hdr_written: usize,
        body: pool::Span<
            owners::Plain<pool::Fixed<{ Gzip::SLOTS.get() }, { Gzip::CAPACITY.get() }>>,
            pool::Owned,
            pool::Frozen,
        >,
        connection: head::Connection,
    },
    Segments {
        hdr_written: usize,
        body: owned::Segments<'d>,
        connection: head::Connection,
    },
    Close(fixed::Body<'d>),
}

impl<'d> Plan<'d> {
    /// Splits transport ownership from lifecycle-only copy evidence exactly once.
    /// The task-origin caller links request owners into owned responses;
    /// deferred bodies require a retained origin and cannot reach this split.
    pub fn split(outcome: Outcome<'d>) -> (Self, Copy) {
        let copy = outcome.copy();
        let plan = match outcome {
            Outcome::Send {
                written,
                connection: conn,
            }
            | Outcome::SendCopied {
                written,
                connection: conn,
                ..
            } => Self::Inline {
                written,
                connection: conn,
            },
            Outcome::SendStatic {
                hdr_written: head,
                body,
                connection: conn,
            } => Self::Static {
                hdr_written: head,
                body,
                connection: conn,
            },
            Outcome::SendShared {
                hdr_written: head,
                body,
                connection: conn,
            }
            | Outcome::SendOwnedCopy {
                hdr_written: head,
                body,
                connection: conn,
                ..
            } => Self::Shared {
                hdr_written: head,
                body,
                connection: conn,
            },
            Outcome::SendCompressed {
                hdr_written: head,
                body,
                connection: conn,
                ..
            } => Self::Frozen {
                hdr_written: head,
                body,
                connection: conn,
            },
            Outcome::SendSegmented {
                hdr_written: head,
                body,
                connection: conn,
            }
            | Outcome::SendLinked {
                hdr_written: head,
                body,
                connection: conn,
            } => Self::Segments {
                hdr_written: head,
                body,
                connection: conn,
            },
            Outcome::SendRequestBody(pending) => match pending.body {},
            Outcome::Close(body) => Self::Close(body),
            Outcome::Failed(error) => Self::Close(error.body()),
        };
        (plan, copy)
    }
}

const _: () = assert!(mem::size_of::<Plan<'static>>() <= 6 * mem::size_of::<usize>());
