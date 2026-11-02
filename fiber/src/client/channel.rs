//! Scoped fixed-client channel construction.

use std::{cell::Cell, marker, num, time};

use dope::{
    core::{
        driver,
        driver::{
            route::{Identity, KeyTag},
            settings,
            settings::{claim, errors},
        },
    },
    manifold::{
        app::creation,
        client::{
            conn, port, session,
            session::policy::{end, start},
        },
        network::client::topology::plan,
        service::bindings::endpoint,
    },
    net::{
        link::{egress, egress::metadata::arena, pool::change::opening},
        wire,
        wire::{failure, terminal},
    },
    runtime::client,
};
use dope_fiber::reply;
use o3::{base::cell::region, buffer::segments};
use session::ingress;

use crate::client::{
    config::{self, report},
    conn::{Connector, open},
    exchange::{net, protocol::Protocol, request},
    request::Handle,
};

#[repr(transparent)]
#[derive(Clone, Copy)]
struct Target(num::NonZero<usize>);

/// Separates fixed-client response storage from its physical connection target.
/// One physical connection is the default, independent of response-lane count.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct Topology {
    lanes: usize,
    target: usize,
    timers: usize,
}

impl Topology {
    pub const fn new(lanes: usize, timers: usize) -> Self {
        Self {
            lanes,
            target: 1,
            timers,
        }
    }

    pub const fn with_target(mut self, target: usize) -> Self {
        self.target = target;
        self
    }

    pub const fn lanes(self) -> usize {
        self.lanes
    }

    pub const fn target(self) -> usize {
        self.target
    }

    pub const fn timers(self) -> usize {
        self.timers
    }
}

impl Target {
    fn try_new(target: usize, lanes: usize) -> Result<Self, config::Error> {
        num::NonZero::new(target)
            .filter(|count| count.get() <= lanes)
            .map(Self)
            .ok_or(config::Error::ConnectorConnections {
                connections: target,
                response_lanes: lanes,
            })
    }

    fn plan<ID: Identity>(
        self,
        egress: egress::Config,
        base: time::Duration,
        max: time::Duration,
    ) -> Result<plan::Plan<ID, { u32::MAX as usize }>, config::Error> {
        let spec = plan::Spec::new(base, max).map_err(config::Error::ConnectorConfig)?;
        plan::Plan::new(self.0.get(), spec.with_egress(egress))
            .map_err(config::Error::ConnectorClaim)
    }
}

pub struct Port<
    'f,
    'e,
    'b,
    'c,
    'w: 'f,
    'd: 'f,
    ID: Identity = KeyTag<0>,
    W: wire::Spec + 'w = wire::Identity,
    F: 'w = report::Never,
> where
    F: report::Policy<terminal::Error<'w, W>>,
{
    pub(super) protocol: Protocol<'f, 'e, 'b, 'c, 'd, ID, (), terminal::Error<'w, W>, F>,
    pub(super) exhausted: Cell<Option<opening::Exhaustion>>,
    target: Target,
}

impl<'f, 'e, 'b, 'c, 'w: 'f, 'd: 'f, ID: Identity, W: wire::Spec + 'w, F: 'w>
    Port<'f, 'e, 'b, 'c, 'w, 'd, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    /// Binds this channel's protocol storage without an open-failure sink.
    pub fn bind(&self) -> session::Setup<'d, Connector<'_, ID, Self>, ingress::Local>
    where
        W: wire::Spec<OpenFailure = failure::Never>,
    {
        self.bind_with(open::Never)
    }

    /// Binds this channel's protocol storage to one exact open-failure sink.
    pub fn bind_with<'s, O>(
        &'s self,
        open: O,
    ) -> session::Setup<'d, Connector<'s, ID, Self, O>, ingress::Local>
    where
        O: start::Sink<'w, 'd, W, ID>,
    {
        let session = Connector::bind(self, open);
        session::Setup::new(session, self.protocol.ingress)
    }
}

impl<'f, 'e, 'b, 'c, 'w: 'f, 'd: 'f, ID: Identity, W, F> end::Sink<'w, 'd, W, ID, failure::Possible>
    for Port<'f, 'e, 'b, 'c, 'w, 'd, ID, W, F>
where
    W: wire::Spec<TerminalFailure = failure::Possible> + failure::Errors<failure::Terminal> + 'w,
    F: report::Policy<terminal::Error<'w, W>>,
{
    fn fail(
        &self,
        conn: conn::Id<'d, ID>,
        cause: terminal::Error<'w, W>,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        self.protocol
            .transport
            .pool
            .failed(region, conn, report::Failure::Wire(cause));
    }
}

impl<
    'life,
    'f: 'life,
    'e: 'life,
    'b: 'life,
    'c: 'life,
    'w: 'life + 'f,
    'd: 'life + 'f,
    ID: Identity + 'life,
    W: wire::Spec + 'w,
    F: 'w,
> endpoint::Provider<'life, 'd, ID, { u32::MAX as usize }>
    for Port<'f, 'e, 'b, 'c, 'w, 'd, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    type Client<'app, B: ?Sized + 'app>
        = Handle<'app, 'f, 'e, 'b, 'c, 'w, 'd, B, ID, W, F>
    where
        'life: 'app,
        'd: 'app;

    fn provide<'app, B: ?Sized + 'app>(
        &'life self,
        bound: client::Bound<'app, 'd, B>,
    ) -> Self::Client<'app, B>
    where
        'life: 'app,
        'd: 'app,
    {
        Handle { port: self, bound }
    }
}

pub struct Plan<ID: Identity = KeyTag<0>, W: wire::Spec = wire::Identity, F = report::Never> {
    timers: usize,
    target: Target,
    plan: config::Plan,
    wire: crate::client::Covariant<(W, F, ID)>,
}

impl<ID: Identity, W: wire::Spec, F> Plan<ID, W, F>
where
    F: report::Class,
{
    pub fn new(config: config::Config, topology: Topology) -> Result<Self, config::Error> {
        let plan = config::Plan::from_config::<F>(config, topology.lanes, topology.target)?;
        let target = Target::try_new(topology.target, topology.lanes)?;
        Ok(Self {
            timers: topology.timers,
            target,
            plan,
            wire: marker::PhantomData,
        })
    }

    /// Exact number of physical connections requested by the connector plan.
    pub const fn target(&self) -> usize {
        self.target.0.get()
    }

    pub fn plan(
        &self,
        base: time::Duration,
        max: time::Duration,
    ) -> Result<plan::Plan<ID, { u32::MAX as usize }>, config::Error> {
        self.target
            .plan::<ID>(self.plan.staging.retained(), base, max)
    }
}

impl<ID: Identity, W: wire::Spec, F> settings::Resource for Plan<ID, W, F> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        needs.include(claim::Claim::new(settings::Routes::EMPTY).domains(settings::Domain::ONE))
    }
}

impl<'f, 'e, 'b, 'c, 'w: 'f, 'd: 'f, ID: Identity, W: wire::Spec + 'w, F: 'w>
    Port<'f, 'e, 'b, 'c, 'w, 'd, ID, W, F>
where
    F: report::Policy<terminal::Error<'w, W>> + report::Class,
{
    pub fn capacity(&self) -> usize {
        self.protocol.io.capacity().count().get()
    }

    /// Exact retained-egress limit validated with the request pool plan.
    pub fn egress(&self) -> egress::Config {
        self.protocol.io.config()
    }

    /// Reconstructs the validated connector plan for custom installers that
    /// create discovery inside the driver's generative lifetime.
    pub fn plan(
        &self,
        base: time::Duration,
        max: time::Duration,
    ) -> Result<plan::Plan<ID, { u32::MAX as usize }>, config::Error> {
        self.target.plan::<ID>(self.egress(), base, max)
    }
}

impl<ID: Identity, W: wire::Spec, F> Plan<ID, W, F> {
    /// Keeps reply storage alive through the complete port consumer.
    pub fn try_with<'w, 'd, C, R>(
        self,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        run: impl for<'f, 'e, 'b, 'c> FnOnce(Port<'f, 'e, 'b, 'c, 'w, 'd, ID, W, F>, &mut C) -> R,
    ) -> Result<R, creation::Error>
    where
        Self: 'w,
        W: 'w,
        F: report::Policy<terminal::Error<'w, W>> + report::Class + 'w,
    {
        let Self {
            timers,
            target,
            plan,
            wire: _,
        } = self;
        let config::Plan {
            codec,
            decode,
            response_resident_capacity: resident,
            staging,
            requests,
            transport: net,
            response_segments: parts,
        } = plan;
        let mut egress = arena::Store::try_lanes(staging.retained(), staging.capacity())
            .map_err(creation::Error::Allocation)?;
        let reports = F::try_pool(net.reports()).map_err(creation::Error::Allocation)?;
        let proof = context(owner).regions().0.proof();
        reply::Arena::try_with(net.arena(), proof, |replies| {
            let port = {
                let mut driver = context(owner);
                let builder = &mut driver.storage;
                let ingress = ingress::Plan::local(ingress::Bytes::new(resident), parts);
                let parts = segments::Pool::try_new(segments::Plan {
                    segments: parts.get(),
                    chains: parts.get() / 2,
                })
                .map_err(creation::Error::Allocation)?;
                let network =
                    net::Transport::try_new(net, timers, F::source(&reports), builder, replies)?;
                let decode = decode.try_build(builder)?;
                Port {
                    protocol: Protocol {
                        io: port::Port::try_new(&mut egress, builder.region())
                            .map_err(creation::Error::Allocation)?,
                        codec,
                        decode,
                        ingress,
                        transport: network,
                        requests: request::Request::try_new(requests)
                            .map_err(creation::Error::Exhausted)?,
                        response_segments: parts,
                        timer: builder.timer(),
                    },
                    target,
                    exhausted: Cell::new(None),
                }
            };
            Ok(run(port, owner))
        })
        .map_err(creation::Error::Allocation)?
    }
}

impl<'s, 'f, 'e, 'b, 'c, 'w: 'f, 'd: 'f, ID: Identity, W: wire::Spec + 'w, F: 'w, O>
    client::Provider<'d> for Connector<'s, ID, Port<'f, 'e, 'b, 'c, 'w, 'd, ID, W, F>, O>
where
    F: report::Policy<terminal::Error<'w, W>>,
{
    type Client<'app>
        = Handle<'app, 'f, 'e, 'b, 'c, 'w, 'd, Self, ID, W, F>
    where
        Self: 'app,
        'd: 'app;

    fn provide<'app>(context: client::Context<'_, 'app, 'd, Self>) -> Self::Client<'app>
    where
        Self: 'app,
        'd: 'app,
    {
        let (provider, bound) = context.into_parts();
        Handle {
            port: provider.get_ref().root,
            bound,
        }
    }
}
