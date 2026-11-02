use core::marker::PhantomData;
use std::{mem, num::NonZero, time};

use claim::Needs;
use dope::{
    core::{
        driver,
        driver::{
            route,
            settings::{claim, errors},
            storage,
        },
    },
    manifold::{
        app::env,
        client::{
            app::request,
            port,
            session::{codec::staging, ingress},
        },
        network::client::topology,
        service::{self, health, sync},
    },
    net,
    net::{
        link::{egress, egress::metadata::arena},
        wire::Identity,
    },
};
use driver::settings::Resource;
use errors::Claim;
use hold::limits;
use o3::{
    alloc,
    buffer::{hold, pool, resident::Shared},
    depot::{capacity, queue::resident},
};
use port::error;
use sark_core::manifold::client::domain;
use sark_h2::client;
use topology::plan::{Local, Plan, Spec, Standard};

use crate::{
    client::{
        config,
        manifold::{channel, conn},
        state::plan,
    },
    protocol::{request::Scheme, section},
};

const MAX_CONNECTIONS: usize = u32::MAX as usize;
const DEFAULT_WRITE_CHUNK: usize = 16 * 1024;
const DEFAULT_EGRESS_ENTRIES: u32 = 1024;

#[derive(Debug)]
pub enum Error {
    InboundResidentCapacity(limits::Error),
    WaiterCapacity { capacity: usize },
    EgressResidentOverflow { request: usize, prefixes: usize },
    EgressResidentWidth { resident: usize },
    Staging(error::Plan),
    Retry(health::Error),
    Claim(Claim),
    Prefixes(alloc::Exhausted),
    Change(resident::Error),
    Sender(resident::Error),
    Completion(resident::Error),
    Allocation(alloc::Error),
}

const _: () = assert!(mem::size_of::<Error>() <= 5 * mem::size_of::<usize>());

impl From<alloc::Error> for Error {
    fn from(error: alloc::Error) -> Self {
        Self::Allocation(error)
    }
}

impl From<Claim> for Error {
    fn from(error: Claim) -> Self {
        Self::Claim(error)
    }
}

type Address<E> = <<E as env::Spec>::Transport as net::Transport>::Addr;
type Install<ID, E> = topology::Install<
    ID,
    MAX_CONNECTIONS,
    E,
    channel::Binding,
    Standard,
    sync::Preserve,
    Address<E>,
    16,
    Local<staging::Direct>,
    request::Enabled,
>;

pub struct Settings<ID: route::Identity = route::KeyTag<0>> {
    pub(in crate::client::manifold) plan: Layout,
    route: PhantomData<fn() -> ID>,
}

impl<ID: route::Identity> Settings<ID> {
    /// Selects the origin scheme for every connection, including reconnects.
    pub const fn with_scheme(mut self, scheme: Scheme) -> Self {
        self.plan.protocol.scheme = scheme;
        self
    }

    pub fn try_new(
        carrier: client::Plan<section::Plan>,
        limits: config::Limits,
        waiters: usize,
    ) -> Result<Self, Error> {
        Ok(Self {
            plan: Layout::new(carrier, limits, waiters)?,
            route: PhantomData,
        })
    }

    pub fn install<E>(
        self,
        peers: service::Snapshot<service::Endpoint<Address<E>, Address<E>>>,
        base: time::Duration,
        max: time::Duration,
    ) -> Result<(Self, Install<ID, E>), Error>
    where
        E: env::Spec<Wire = Identity>,
        E::Transport: net::Transport,
    {
        let plan = Plan::new(
            1,
            Spec::new(base, max)
                .map_err(Error::Retry)?
                .with_egress(self.plan.staging.retained()),
        )?;
        let install = topology::Install::from_plan(peers, plan, channel::Binding);
        Ok((self, install))
    }
}

impl<ID: route::Identity> Resource for Settings<ID> {
    fn include_in(&self, needs: Needs) -> Result<Needs, Claim> {
        Ok(needs)
    }
}

impl<ID: route::Identity> storage::Plan for Settings<ID> {
    type Output<'p, 'd>
        = channel::Port<'p, 'd, ID>
    where
        Self: 'd,
        'd: 'p;
}

impl<ID: route::Identity> storage::Build for Settings<ID> {
    type Store<'d>
        = arena::Store<'d>
    where
        Self: 'd;
    type Error = Error;

    fn try_store<'d>(&self) -> Result<Self::Store<'d>, Self::Error>
    where
        Self: 'd,
    {
        arena::Store::try_lanes(self.plan.staging.retained(), self.plan.staging.capacity())
            .map_err(Error::from)
    }

    fn try_build<'p, 'd>(
        self,
        backing: &'p mut Self::Store<'d>,
        builder: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        let receive = builder.receive().buffer_len().into_nonzero().get() as usize;
        let policy = <client::Plan<section::Plan> as domain::Plan<Shared<'_>>>::RETENTION;
        let receive = policy
            .try_limit(receive)
            .map_err(Error::InboundResidentCapacity)?;
        let retained = self
            .plan
            .inbound
            .try_add(receive.bytes())
            .map_err(Error::InboundResidentCapacity)?;
        let state = conn::State::new(self.plan.protocol, self.plan.prefixes)?;
        Ok(channel::Port {
            ingress: ingress::Plan::local(
                ingress::Bytes::from_limit(retained),
                capacity::Count::EMPTY,
            ),
            io: port::Port::try_new(backing, builder.region())?,
            shared: channel::Shared::try_new(
                self.plan.waiters,
                state,
                builder.driver(),
                builder.state(),
            )?,
        })
    }
}

#[derive(Clone, Copy)]
pub(in crate::client::manifold) struct Layout {
    pub(in crate::client::manifold) protocol: plan::Plan<client::Plan<section::Plan>>,
    pub(in crate::client::manifold) prefixes: pool::Layout,
    pub(in crate::client::manifold) inbound: limits::Bound,
    pub(in crate::client::manifold) waiters: NonZero<usize>,
    pub(in crate::client::manifold) staging: port::Plan<()>,
}

impl Layout {
    /// Reserves carrier/control progress independently of payload limits.
    /// Application owners can still exhaust the resident budget; storage build
    /// adds the actual native receive allowance.
    fn new(
        carrier: client::Plan<section::Plan>,
        limits: config::Limits,
        waiters: usize,
    ) -> Result<Self, Error> {
        let protocol = plan::Plan::new(limits, carrier);
        let policy = <client::Plan<section::Plan> as domain::Plan<Shared<'_>>>::RETENTION;
        let inbound = policy
            .try_limit(limits.input_max())
            .and_then(|payload| {
                payload.try_add(policy.try_limit(carrier.inbound_capacity())?.bytes())
            })
            .map_err(Error::InboundResidentCapacity)?;
        let waiters = NonZero::new(waiters).ok_or(Error::WaiterCapacity { capacity: waiters })?;
        const {
            assert!(
                DEFAULT_WRITE_CHUNK
                    >= <client::Plan<section::Plan> as domain::Plan<Shared<'_>>>::MIN_PREFIX
            );
        }
        let prefixes =
            pool::Layout::constant::<{ DEFAULT_EGRESS_ENTRIES as usize }, DEFAULT_WRITE_CHUNK>();
        // Limits admits at least one max(message, 1) request; these prefixes
        // hold at least one write chunk, so their sum also proves progress.
        let request = limits.request_max();
        let bytes = prefixes.bytes();
        let resident = request
            .checked_add(bytes)
            .ok_or(Error::EgressResidentOverflow {
                request,
                prefixes: bytes,
            })?;
        if resident > u32::MAX as usize {
            return Err(Error::EgressResidentWidth { resident });
        }
        let retained = egress::Config::shared(egress::Credit::new(
            egress::Entries::new(DEFAULT_EGRESS_ENTRIES),
            egress::Resident::new(resident as u32),
        ));
        let staging = port::Plan::new(1, retained).map_err(Error::Staging)?;
        Ok(Self {
            protocol,
            prefixes,
            inbound,
            waiters,
            staging,
        })
    }
}
