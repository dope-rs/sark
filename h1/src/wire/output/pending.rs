use sark_core::body;
use sark_http::payload::{owned, response::Body as _};

use crate::wire::{Failure, head, output::Outcome};

/// Response head awaiting its affine request-body owner.
pub struct Pending<P = usize> {
    head: usize,
    pub(crate) body: P,
    connection: head::Connection,
}

impl<P> Pending<P> {
    pub(crate) const fn new(head: usize, body: P, conn: head::Connection) -> Self {
        Self {
            head,
            body,
            connection: conn,
        }
    }
}

impl Pending {
    /// Binds the affine body this response declared.
    ///
    /// The declared length is already on the wire, so a body of another length
    /// is classified here rather than truncated into the response.
    pub(crate) fn link<'d>(self, body: owned::Segments<'d>) -> Outcome<'d> {
        let Self {
            head,
            body: len,
            connection: conn,
        } = self;
        let actual = body.len();
        if actual != len {
            return Outcome::Failed(Failure::Admission(body::Error::LengthMismatch {
                actual,
                declared: len,
            }));
        }
        Outcome::SendLinked {
            hdr_written: head,
            body,
            connection: conn,
        }
    }
}
