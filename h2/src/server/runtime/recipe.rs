use dope::{
    core::driver::{
        route::Identity,
        settings,
        settings::{claim, errors},
    },
    runtime::topology::lending,
};

use crate::server::runtime::{Plan, modes};

/// Owns the H2 plan and service recipe until their storage is borrowed.
pub struct Recipe<ID: Identity, F> {
    plan: Plan<ID>,
    recipe: F,
}

impl<ID: Identity, F> Recipe<ID, F> {
    pub(in crate::server::runtime) const fn new(plan: Plan<ID>, recipe: F) -> Self {
        Self { plan, recipe }
    }
}

impl<ID: Identity, F> settings::Resource for Recipe<ID, F> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        self.plan.include_in(needs)
    }
}

impl<C, N, ID: Identity, F: modes::Family<C, N, ID>> lending::Installer<C, N> for Recipe<ID, F> {
    type Output = F::Output;
    type Error = F::Error;

    fn install(self, context: &mut C, next: N) -> Result<Self::Output, Self::Error> {
        self.recipe.install(self.plan, context, next)
    }
}
