use dope::core::random;
use dope_quic::{
    conn::{
        scope,
        stream::resident::{Handle, Local},
    },
    stream::Recv,
    wire::stream::Id,
};
use o3::buffer::resident;
use sark_http::{field::metadata::section::rules, owner, payload::compress::dynamic::hash};

use crate::{
    conn::{Connection, event, io, options, options::plan},
    manifold::{Binding, Error, credits, flow},
};

pub struct Session<'d, P: rules::Policy = rules::RetainRaw> {
    pub(in crate::manifold) h3: Connection<'d, P>,
    pub(in crate::manifold) receive_credits: credits::Ledger,
}

impl Session<'_> {
    pub fn new() -> Result<Self, options::Error> {
        Self::with_role(event::Role::Client)
    }

    pub fn with_role(role: event::Role) -> Result<Self, options::Error> {
        Self::with_config(role, options::Config::default())
    }

    pub fn with_config(role: event::Role, config: options::Config) -> Result<Self, options::Error> {
        Self::from_plan(role, plan::Config::new(config)?)
    }
}

impl<'d, P: rules::Policy> Session<'d, P> {
    pub fn from_plan(role: event::Role, config: plan::Config) -> Result<Self, options::Error> {
        let seed = random::Seed::acquire().map_err(options::Error::Entropy)?;
        Self::from_seed(role, config, hash::Seed::new(&seed))
    }

    pub fn from_seed(
        role: event::Role,
        config: plan::Config,
        seed: hash::Seed,
    ) -> Result<Self, options::Error> {
        let ready = config.ready_plan();
        Ok(Self {
            h3: config.build_seeded(role, seed)?,
            receive_credits: credits::Ledger::try_new(ready)?,
        })
    }
}

impl<'d, P: rules::Policy> Session<'d, P> {
    pub(in crate::manifold) fn validate<const ID: u8, B, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
    ) -> Result<(), Error>
    where
        B: Recv,
    {
        let current = quic.streams().resident().origin();
        match self.h3.origin() {
            Some(found) => {
                if found == current {
                    Ok(())
                } else {
                    Err(Error::Origin {
                        expected: current,
                        found,
                    })
                }
            }
            None => self.h3.bind(current).map_err(Error::from),
        }
    }

    pub(in crate::manifold) fn bind_scope<'scope, const ID: u8, B, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'scope, ID, B, RECONCILE, resident::Shared<'d>>,
    ) -> Result<Binding<'scope>, Error>
    where
        B: Recv,
    {
        let bound = self.h3.origin();
        let table = quic.streams();
        let slots = table.resident();
        match bound {
            Some(origin) => Ok(Binding::new(origin, slots.owner(origin)?)),
            None => {
                let origin = slots.origin();
                let owner = slots.owner(origin)?;
                self.h3.bind(origin)?;
                Ok(Binding::new(origin, owner))
            }
        }
    }

    pub(in crate::manifold) fn bind<'scope, const ID: u8, B: Recv, const RECONCILE: bool>(
        &mut self,
        quic: &mut scope::Access<'scope, ID, B, RECONCILE, resident::Shared<'d>>,
        local: Local,
    ) -> Result<Handle<'scope>, Error> {
        let owner = self.bind_scope(quic)?.owner();
        Ok(quic.streams().resident().bind(owner, local)?)
    }

    pub fn h3(&self) -> &Connection<'d, P> {
        &self.h3
    }

    pub(in crate::manifold) fn h3_mut(&mut self) -> &mut Connection<'d, P> {
        &mut self.h3
    }

    pub fn send(&mut self) -> io::Send<'_, 'd, P> {
        io::Send::new(&mut self.h3)
    }

    pub fn ingress(&mut self) -> flow::Ingress<'_, 'd, P> {
        flow::Ingress::new(self)
    }

    pub fn egress(&mut self) -> flow::Egress<'_, 'd, P> {
        flow::Egress::new(self)
    }

    /// Consumes staged output before reporting each original owner's release.
    pub fn close(self, mut release: impl FnMut(owner::Effect<Id>)) {
        use owner::{Change, Effect, cause};

        self.h3.queues.close(|id, charge| {
            release(Effect::new(
                id,
                charge,
                Change::StagingReleased(cause::Staging::Closed),
            ));
        });
    }
}
