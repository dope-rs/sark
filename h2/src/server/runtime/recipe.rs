use dope::{
    core::{
        self,
        driver::{
            route::Identity,
            settings,
            settings::{claim, errors},
            storage,
        },
    },
    manifold::{
        self,
        app::{self, env},
        network::listener,
        timing,
    },
    net::{
        tcp,
        wire::{self, domain},
    },
    runtime::{
        executor,
        topology::{lending, recipe},
    },
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

impl<'stage, 'scope, 'life: 'scope, 'e: 'life, 'd: 'e, S, Q, N, O, ID: Identity + 'd, F>
    lending::Installer<lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>, N> for Recipe<ID, F>
where
    S: storage::Plan + 'd,
    Q: executor::Startup,
    F: modes::Family<S, ID> + 'd,
    N: for<'g, 'n, 'p, 's> lending::Next<
            lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
            app::Host<
                listener::Listener<
                    'n,
                    'p,
                    's,
                    'd,
                    'd,
                    ID,
                    F::Application<'g, 'life, 'e, 'd>,
                    env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
                >,
            >,
            Output = O,
        >,
{
    type Output = O;
    type Error = manifold::BuildError<core::BranchError<F::Error, F::Failure>>;

    fn install(
        self,
        stage: &mut lending::Stage<'stage, 'scope, 'life, 'e, 'd, S, Q>,
        next: N,
    ) -> Result<O, Self::Error> {
        let service = recipe::Build::build(self.recipe, stage.storage())
            .map_err(core::BranchError::First)
            .map_err(manifold::BuildError::Factory)?;
        let (config, conn, bound, lifetime) = self.plan.into_parts();
        let install = listener::Install::<
            ID,
            env::Bundle<tcp::Tcp, wire::Identity, timing::Throughput>,
            _,
        >::new(config, domain::Local::new(wire::Identity))
        .with_bound(bound);
        let install = match lifetime {
            Some(lifetime) => install.with_life(lifetime),
            None => install,
        };
        let binding = install.binding(stage.driver().driver_ref());
        <F as modes::Family<S, ID>>::with(service, conn, binding, |app| {
            install
                .in_stage(stage, app, |listener, stage| {
                    next.run(app::Host::new(listener), stage)
                })
                .map_err(manifold::BuildError::Create)
        })
        .map_err(core::BranchError::Second)
        .map_err(manifold::BuildError::Factory)?
    }
}
