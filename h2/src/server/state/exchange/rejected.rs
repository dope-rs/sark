use sark_core::body::credit;

use crate::server::payload::{self, Buffer};

/// Body storage returned when head admission rejects a selected route.
pub(crate) enum Rejected<'d, O: credit::Origin = credit::raw::Stored> {
    Body(Buffer<'d, O>),
    /// A duplex selection whose retained storage was released at rejection and
    /// which never held request-body credit.
    Duplex,
}

impl<O: credit::Origin> Rejected<'_, O> {
    pub(crate) fn discard<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        evidence: E,
    ) -> Result<(), payload::Error> {
        match self {
            Self::Body(body) => body.discard(ledger, evidence),
            Self::Duplex => Ok(()),
        }
    }
}
