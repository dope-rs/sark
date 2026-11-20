use core::mem;

use o3::buffer::storage;
use sark_http::{
    owner::evidence,
    payload::{
        compress::{self, Compression, Gzip},
        fixed, owned,
        response::Body as _,
    },
};

use crate::{session, wire::head};

mod admit;
mod egress;
mod owner;
mod sink;

pub(crate) use admit::Admit;
pub use egress::Egress;
pub use owner::Owner;
pub use sink::error::Error;

/// Selects connection compression storage from the route policies.
pub struct Select<const GZIP: bool>;

pub trait Selected {
    type Compression: Compression;
}

impl Selected for Select<false> {
    type Compression = ();
}

impl Selected for Select<true> {
    type Compression = Gzip;
}

/// Exact request origin of one completed response owner.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Response(Owner),
    Linked(Owner),
}

/// Encoded response body and its ownership evidence.
pub enum Complete<'d> {
    Inline,
    Copy {
        copy: evidence::Copy,
    },
    Static {
        body: fixed::Body<'d>,
    },
    Shared {
        body: storage::Shared,
    },
    Compressed {
        body: compress::Frozen,
        read: usize,
    },
    Segments {
        body: owned::Segments<'d>,
    },
    Linked {
        body: owned::Segments<'d>,
    },
    Owned {
        body: storage::Shared,
        cause: evidence::Cause,
    },
}

impl<'d> Complete<'d> {
    /// Projects owner without cloning or moving the response body.
    pub fn retention(&self) -> Origin {
        match self {
            Self::Inline => Origin::Response(Owner::default()),
            Self::Copy { copy, .. } => Origin::Response(Owner::inline(copy.written)),
            Self::Static { body, .. } => Origin::Response(Owner::inline(body.len())),
            Self::Shared { body, .. } => Origin::Response(Owner::shared(body)),
            Self::Compressed { body, .. } => Origin::Response(Owner::shared(body)),
            Self::Segments { body, .. } => Origin::Response(Owner::segmented(body)),
            Self::Linked { body, .. } => Origin::Linked(Owner::segmented(body)),
            Self::Owned { body, .. } => Origin::Response(Owner::shared(body)),
        }
    }

    pub fn copy(&self) -> Option<evidence::Copy> {
        match self {
            Self::Copy { copy, .. } => Some(*copy),
            Self::Compressed { body, read, .. } => Some(evidence::Copy {
                read: *read,
                written: body.len(),
                cause: evidence::Cause::Compressed,
            }),
            Self::Owned { body, cause, .. } => Some(evidence::Copy {
                read: body.len(),
                written: body.len(),
                cause: *cause,
            }),
            Self::Inline
            | Self::Static { .. }
            | Self::Shared { .. }
            | Self::Segments { .. }
            | Self::Linked { .. } => None,
        }
    }

    pub fn into_consume<D>(
        self,
        permit: session::Permit,
        consumption: session::Consumption,
        connection: head::Connection,
    ) -> session::Outcome<'d, D> {
        session::Outcome::Complete {
            permit,
            consumption,
            response: self,
            connection,
        }
    }
}

/// Complete switching-response wire bytes with no HTTP message body.
pub enum Switch<'d> {
    Inline,
    Static(fixed::Body<'d>),
}

const _: () = assert!(mem::size_of::<Complete<'static>>() <= 5 * mem::size_of::<usize>());
