use core::marker::PhantomData;
use std::time;

use dope::{
    core::driver::{
        route::{Identity, KeyTag},
        settings,
        settings::{claim, errors},
        storage,
    },
    manifold::{
        client::{port, session::ingress},
        network::client::topology::plan,
        service::health,
    },
    net::link::{egress, egress::metadata::arena},
};
use dope_fiber::wait;
use o3::{alloc, depot::capacity};

use crate::client::{
    self, Config,
    channel::{self, layout},
    headers, protocol,
};

pub struct Factory<ID: Identity = KeyTag<0>> {
    pub(super) codec: protocol::Codec,
    pub(super) staging: port::Plan<()>,
    pub(super) egress: egress::Config,
    waiter_capacity: wait::Capacity,
    route: PhantomData<fn() -> ID>,
}

impl<ID: Identity> Factory<ID> {
    pub(super) fn new(config: Config, lanes: usize, waits: usize) -> Result<Self, client::Error> {
        headers::Request::validate(&config)?;
        let layout = layout::Layout::new(&config)?;
        let stage = layout.stage(lanes)?;
        let egress = layout.config(lanes, 1, headers::MAX_BYTES)?;
        let codec = protocol::Codec::try_new(config).map_err(client::Error::Staging)?;
        Ok(Self {
            codec,
            staging: stage,
            egress,
            waiter_capacity: wait::Capacity::new(waits),
            route: PhantomData,
        })
    }

    pub(super) fn spec(
        config: &Config,
        egress: egress::Config,
        base: time::Duration,
        max: time::Duration,
    ) -> Result<plan::Spec, health::Error> {
        let spec = plan::Spec::new(base, max)?.with_egress(egress);
        Ok(match config.max_connection_age {
            Some(life) => spec.with_ttl(life),
            None => spec,
        })
    }
}

impl<ID: Identity> settings::Resource for Factory<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        Ok(needs)
    }
}

impl<ID: Identity> storage::Plan for Factory<ID> {
    type Output<'p, 'd>
        = channel::Port<'p, 'd, ID>
    where
        Self: 'd,
        'd: 'p;
}

impl<ID: Identity> storage::Build for Factory<ID> {
    type Store<'d>
        = arena::Store<'d>
    where
        Self: 'd;
    type Error = alloc::Error;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd,
    {
        arena::Store::try_lanes(self.staging.retained(), self.staging.capacity())
    }

    /// Direct delivery drops each accepted `Head` before decoding again.
    /// Retry admission precedes retention; one temporary source account can be live.
    fn try_build<'p, 'd>(
        self,
        backing: &'p mut Self::Store<'d>,
        store: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        let shared = channel::SharedState::new(self.waiter_capacity, store.driver());
        let retained = self
            .codec
            .staging
            .ceiling()
            .max(store.receive().buffer_len().into_nonzero());
        let ingress = ingress::Plan::local(
            ingress::Bytes::new(retained.get() as usize),
            capacity::Count::new(1),
        );
        Ok(channel::Port {
            codec: self.codec,
            ingress,
            egress: self.egress,
            shared,
            io: port::Port::try_new(backing, store.region())?,
        })
    }
}
