use crate::server::{
    Bank,
    runtime::{direct, run, state::active},
    service::handler::Handler,
};
use dope::core::driver::{
    settings,
    settings::{claim, errors},
};

/// Zero-sized direct mode for synchronous handlers.
#[derive(Clone, Copy, Default)]
pub struct Direct;

impl settings::Resource for Direct {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl Direct {
    pub fn build<'a, H: Handler>(
        self,
        app: H,
        bank: &'a Bank,
    ) -> run::Service<'a, direct::Policy<H, active::Entry<'a>>> {
        run::Service::new(direct::Policy::new(app), bank)
    }
}

const _: () = assert!(size_of::<Direct>() == 0);
