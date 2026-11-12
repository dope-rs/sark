use core::{convert::Infallible, mem};

use o3::buffer::storage;
use sark_http::{
    owner::evidence,
    payload::{
        compress::{self, Compression, Gzip},
        fixed, owned,
        response::Body as _,
    },
};

use crate::{
    session,
    wire::{Failure, head},
};

mod admit;
mod egress;
mod owner;
mod pending;
mod sink;

pub(crate) use admit::Admit;
pub use egress::Egress;
pub use owner::Owner;
pub use pending::Pending;
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

/// Exact ownership state produced by one output decision.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Retention {
    Response(Owner),
    Linked(Owner),
    Rejected,
}

/// Exact request origin of one completed response owner.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Response(Owner),
    Linked(Owner),
}

/// Exact response-body copy observation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Copy {
    Uncopied,
    Copied(evidence::Copy),
}

/// A transport-independent HTTP/1 output decision.
pub enum Outcome<'d, P = Infallible> {
    Send {
        connection: head::Connection,
    },
    SendCopied {
        copy: evidence::Copy,
        connection: head::Connection,
    },
    SendStatic {
        body: fixed::Body<'d>,
        connection: head::Connection,
    },
    SendShared {
        body: storage::Shared,
        connection: head::Connection,
    },
    SendCompressed {
        body: compress::Frozen,
        read: usize,
        connection: head::Connection,
    },
    SendSegmented {
        body: owned::Segments<'d>,
        connection: head::Connection,
    },
    SendLinked {
        body: owned::Segments<'d>,
        connection: head::Connection,
    },
    SendOwnedCopy {
        body: storage::Shared,
        cause: evidence::Cause,
        connection: head::Connection,
    },
    SendRequestBody(Pending<P>),
    Close(fixed::Body<'d>),
    Failed(Failure),
}

/// A response completed synchronously while consuming a request frame.
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

impl Complete<'_> {
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

    pub fn copy(&self) -> Copy {
        match self {
            Self::Copy { copy, .. } => Copy::Copied(*copy),
            Self::Compressed { body, read, .. } => Copy::Copied(evidence::Copy {
                read: *read,
                written: body.len(),
                cause: evidence::Cause::Compressed,
            }),
            Self::Owned { body, cause, .. } => Copy::Copied(evidence::Copy {
                read: body.len(),
                written: body.len(),
                cause: *cause,
            }),
            Self::Inline
            | Self::Static { .. }
            | Self::Shared { .. }
            | Self::Segments { .. }
            | Self::Linked { .. } => Copy::Uncopied,
        }
    }
}

/// Complete switching-response wire bytes with no HTTP message body.
pub enum Switch<'d> {
    Inline,
    Static(fixed::Body<'d>),
}

impl<'d, P> Outcome<'d, P> {
    /// Returns staged response owner when a response was committed.
    pub fn retention(&self) -> Retention {
        match self {
            Self::Send { .. } => Retention::Response(Owner::default()),
            Self::SendCopied { copy, .. } => Retention::Response(Owner::inline(copy.written)),
            Self::SendStatic { body, .. } => Retention::Response(Owner::inline(body.len())),
            Self::SendShared { body, .. } => Retention::Response(Owner::shared(body)),
            Self::SendCompressed { body, .. } => Retention::Response(Owner::shared(body)),
            Self::SendSegmented { body, .. } => Retention::Response(Owner::segmented(body)),
            Self::SendLinked { body, .. } => Retention::Linked(Owner::segmented(body)),
            Self::SendOwnedCopy { body, .. } => Retention::Response(Owner::shared(body)),
            Self::SendRequestBody(_) | Self::Close(_) | Self::Failed(_) => Retention::Rejected,
        }
    }

    pub fn copy(&self) -> Copy {
        match self {
            Self::SendCopied { copy, .. } => Copy::Copied(*copy),
            Self::SendCompressed { body, read, .. } => Copy::Copied(evidence::Copy {
                read: *read,
                written: body.len(),
                cause: evidence::Cause::Compressed,
            }),
            Self::SendOwnedCopy { body, cause, .. } => Copy::Copied(evidence::Copy {
                read: body.len(),
                written: body.len(),
                cause: *cause,
            }),
            Self::Send { .. }
            | Self::SendStatic { .. }
            | Self::SendShared { .. }
            | Self::SendSegmented { .. }
            | Self::SendLinked { .. }
            | Self::SendRequestBody(_)
            | Self::Close(_)
            | Self::Failed(_) => Copy::Uncopied,
        }
    }
}

impl<'d> Outcome<'d, usize> {
    pub(crate) fn link(self, owner: owned::Segments<'d>) -> Outcome<'d> {
        match self {
            Self::Send { connection: conn } => Outcome::Send { connection: conn },
            Self::SendCopied {
                copy,
                connection: conn,
            } => Outcome::SendCopied {
                copy,
                connection: conn,
            },
            Self::SendStatic {
                body,
                connection: conn,
            } => Outcome::SendStatic {
                body,
                connection: conn,
            },
            Self::SendShared {
                body,
                connection: conn,
            } => Outcome::SendShared {
                body,
                connection: conn,
            },
            Self::SendCompressed {
                body,
                read,
                connection: conn,
            } => Outcome::SendCompressed {
                body,
                read,
                connection: conn,
            },
            Self::SendSegmented {
                body,
                connection: conn,
            } => Outcome::SendSegmented {
                body,
                connection: conn,
            },
            Self::SendLinked {
                body,
                connection: conn,
            } => Outcome::SendLinked {
                body,
                connection: conn,
            },
            Self::SendOwnedCopy {
                body,
                cause,
                connection: conn,
            } => Outcome::SendOwnedCopy {
                body,
                cause,
                connection: conn,
            },
            Self::SendRequestBody(pending) => pending.link(owner),
            Self::Close(body) => Outcome::Close(body),
            Self::Failed(error) => Outcome::Failed(error),
        }
    }
}

impl<'d> Outcome<'d> {
    /// Converts one completed write into the session's consume outcome: the
    /// pending route links a request body before any completion, so a deferred
    /// body cannot reach this mapping.
    pub fn into_consume<D>(
        self,
        permit: session::Permit,
        flow: session::Consumption,
        close: head::Connection,
    ) -> session::Outcome<'d, D> {
        let (reply, response) = match self {
            Self::Send { connection: conn } => (Complete::Inline, conn),
            Self::SendCopied {
                copy,
                connection: conn,
            } => (Complete::Copy { copy }, conn),
            Self::SendStatic {
                body,
                connection: conn,
            } => (Complete::Static { body }, conn),
            Self::SendShared {
                body,
                connection: conn,
            } => (Complete::Shared { body }, conn),
            Self::SendCompressed {
                body,
                read,
                connection: conn,
            } => (Complete::Compressed { body, read }, conn),
            Self::SendSegmented {
                body,
                connection: conn,
            } => (Complete::Segments { body }, conn),
            Self::SendLinked {
                body,
                connection: conn,
            } => (Complete::Linked { body }, conn),
            Self::SendOwnedCopy {
                body,
                cause,
                connection: conn,
            } => (Complete::Owned { body, cause }, conn),
            Self::SendRequestBody(pending) => match pending.body {},
            Self::Close(reason) => return session::Outcome::Close(reason),
            Self::Failed(error) => return session::Outcome::Failed(error),
        };
        session::Outcome::Complete {
            permit,
            consumption: flow,
            response: reply,
            connection: close.join(response),
        }
    }
}

const _: () = {
    assert!(mem::size_of::<Outcome<'static, usize>>() == mem::size_of::<Outcome<'static>>());
    assert!(mem::size_of::<Complete<'static>>() <= mem::size_of::<Outcome<'static>>());
    assert!(mem::size_of::<Outcome<'static>>() <= 8 * mem::size_of::<usize>());
};
