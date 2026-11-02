use std::convert;

use dope::core::driver::{
    settings,
    settings::{claim, errors},
};
use o3::buffer::{resident, resident::bound::Unbounded, storage};
use sark_core::manifold::server::{Connection, app};
use sark_h2::stream::Key;

use crate::{
    protocol::section,
    server::{
        Bank,
        mode::Build,
        runtime::{direct, run, state::active},
        service::handler::Handler,
    },
};

/// Zero-sized direct mode for synchronous handlers.
#[derive(Clone, Copy, Default)]
pub struct Direct;

impl settings::Resource for Direct {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl<'a, 'd, H, C, O> Build<'a, 'd, H, C, O> for Direct
where
    H: Handler,
    C: Connection<
            Key = Key,
            Owner = resident::Shared<'d>,
            Fields = section::Owner,
            Output = storage::Shared,
        >,
    O: Copy + Eq + 'd,
    run::Service<'a, direct::Policy<H, active::Entry<'a>>>: app::Application<
            'd,
            C,
            O,
            Fields = section::Plan,
            Failure = run::Failure,
            Egress<'d> = Unbounded,
        >,
{
    type Application = run::Service<'a, direct::Policy<H, active::Entry<'a>>>;
    type Error = convert::Infallible;

    fn build(self, app: H, bank: &'a Bank) -> Result<Self::Application, convert::Infallible> {
        Ok(run::Service::new(direct::Policy::new(app), bank))
    }
}

const _: () = assert!(size_of::<Direct>() == 0);
