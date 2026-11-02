use dope::core::random;
use sark_http::payload::compress::dynamic::hash;

use crate::{
    conn::{self, config},
    server::{self, sync::Route},
};

/// Router and prevalidated machine geometry shared by synchronous connections.
pub struct Endpoint<R> {
    route: Route<R>,
    machine: config::Validated<crate::ServerRole>,
    seed: hash::Seed,
}

impl<R> Endpoint<R> {
    pub fn try_new(router: R, plan: server::Plan) -> Result<Self, config::Error> {
        let machine = config::Validated::new(plan.build()?)?;
        Self::from_plan(router, plan, machine)
    }

    pub fn try_with(router: R, plan: server::Plan, streams: usize) -> Result<Self, config::Error> {
        let machine = config::Validated::new(plan.build_for(streams)?)?;
        Self::from_plan(router, plan, machine)
    }

    fn from_plan(
        router: R,
        plan: server::Plan,
        machine: config::Validated<crate::ServerRole>,
    ) -> Result<Self, config::Error> {
        let seed = random::Seed::acquire()
            .map_err(conn::Build::Entropy)
            .map_err(config::Error::Build)?;
        Ok(Self {
            route: Route::new(router, plan.config()),
            machine,
            seed: hash::Seed::new(&seed),
        })
    }

    pub(super) const fn router(&self) -> &R {
        self.route.router()
    }

    pub(super) const fn limits(&self) -> server::Config {
        self.route.limits()
    }

    pub(in crate::server) const fn machine(&self) -> config::Validated<crate::ServerRole> {
        self.machine
    }

    pub(super) fn seed(&self) -> hash::Seed {
        self.seed.clone()
    }
}
