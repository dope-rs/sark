//! Concrete gRPC application preparation for one carrier lifetime.
use crate::server::Bank;

/// A handler, limits, and execution mode awaiting carrier binding.
pub struct Prepared<'a, H, M> {
    app: H,
    bank: &'a Bank,
    mode: M,
}

impl<'a, H, M> Prepared<'a, H, M> {
    pub(in crate::server) const fn new(app: H, bank: &'a Bank, mode: M) -> Self {
        Self { app, bank, mode }
    }

    pub(in crate::server) fn into_parts(self) -> (H, &'a Bank, M) {
        (self.app, self.bank, self.mode)
    }
}
