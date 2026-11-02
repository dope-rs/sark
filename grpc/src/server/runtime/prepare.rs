//! Concrete gRPC application preparation for one carrier lifetime.
use o3::buffer::{resident, storage};
use resident::bound;
use sark_core::manifold::server::{Connection, app};
use sark_h2::stream::Key;

use crate::{
    protocol::section,
    server::{Bank, mode, runtime::run},
};

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

impl<'a, H, M> app::Spec for Prepared<'a, H, M> {
    type Failure = run::Failure;
    type Fields = section::Plan;
    type Egress<'d> = bound::Unbounded;

    fn layout(&self) -> app::Layout {
        self.bank.limits.layout()
    }
}

impl<'a, 'd, H, M, C, O> app::Bind<'d, C, O> for Prepared<'a, H, M>
where
    C: Connection<
            Key = Key,
            Owner = resident::Shared<'d>,
            Fields = section::Owner,
            Output = storage::Shared,
        >,
    O: Copy + Eq + 'd,
    M: mode::Build<'a, 'd, H, C, O>,
{
    type App = M::Application;
    type Error = M::Error;

    fn bind(self) -> Result<Self::App, Self::Error> {
        self.mode.build(self.app, self.bank)
    }
}
