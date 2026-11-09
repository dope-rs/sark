use std::{marker::PhantomData, mem, num};

use dope::{
    core::driver::{
        self,
        route::{Identity, KeyTag, table::conn::Capacity},
        schedule::ready::wake::queue,
        settings,
        settings::{claim, errors},
    },
    manifold::{
        self,
        app::creation,
        client::{
            attempt::{self, queue::mailbox},
            close,
            conn::{self, engine},
            port, session,
        },
    },
    net::{
        link::egress::metadata::arena,
        tcp,
        wire::{self, domain},
    },
};
use dope_fiber::reply;
use dynamic::origin;
use o3::{
    alloc,
    buffer::segments,
    depot::{capacity, queue::tagged},
};
use sark_client::connect;
use session::ingress;

use crate::client::{
    config::{self, report},
    conn::{Connector, open},
    dynamic,
    exchange::{net, protocol::Protocol, request},
};

pub(in crate::client) enum Outcome<'d, ID: Identity> {
    Connected(conn::Id<'d, ID>),
    Failed(Failure),
}

const _: () =
    assert!(mem::size_of::<Outcome<'static, KeyTag<0>>>() == mem::size_of::<[usize; 3]>());

pub(in crate::client) enum Failure {
    IdentityExhausted,
    Activation(net::Open),
    Attempt(attempt::ConnectFault),
}

impl<M> From<Failure> for connect::Error<M> {
    fn from(error: Failure) -> Self {
        match error {
            Failure::IdentityExhausted => Self::IdentityExhausted,
            Failure::Activation(net::Open::Transport(error)) => Self::Activation(error),
            Failure::Activation(net::Open::Reply(error)) => Self::Reply(error),
            Failure::Attempt(error) => Self::Attempt(error),
        }
    }
}

pub struct Plan<ID: Identity = KeyTag<0>> {
    timers: usize,
    origins: num::NonZero<u32>,
    plan: config::Plan,
    claim: claim::Claim,
    limit: Capacity,
    route: crate::client::Covariant<ID>,
}

#[derive(Debug)]
pub enum CreateError {
    Config(config::Error),
    Capacity(capacity::Error),
    OriginCapacity { actual: usize },
    Origins(origin::BuildError),
    Queue(tagged::Error<num::NonZero<u32>, num::NonZero<u32>>),
    Requests(alloc::Exhausted),
    Claim(errors::Claim),
    Manifold(creation::Error),
}

impl From<config::Error> for CreateError {
    fn from(error: config::Error) -> Self {
        Self::Config(error)
    }
}

impl From<errors::Claim> for CreateError {
    fn from(error: errors::Claim) -> Self {
        Self::Claim(error)
    }
}

impl From<creation::Error> for CreateError {
    fn from(error: creation::Error) -> Self {
        Self::Manifold(error)
    }
}

impl From<manifold::ConfigError> for CreateError {
    fn from(error: manifold::ConfigError) -> Self {
        Self::Manifold(creation::Error::Config(error))
    }
}

impl From<alloc::Error> for CreateError {
    fn from(error: alloc::Error) -> Self {
        Self::Manifold(creation::Error::Allocation(error))
    }
}

pub struct Port<'o, 'q, 'e, 'b, 'c, 'd, ID: Identity = KeyTag<0>> {
    limit: Capacity,
    pub(in crate::client) protocol: Protocol<'d, 'e, 'b, 'c, 'd, ID, origin::Key<'o, 'q, 'd, ID>>,
    pub(in crate::client) origins: origin::Registry<'o, 'q, 'd, ID>,
    pub(in crate::client) pending:
        mailbox::Domain<'d, tcp::Tcp, origin::Key<'o, 'q, 'd, ID>, Outcome<'d, ID>, ID>,
}

impl<ID: Identity> settings::Resource for Plan<ID> {
    fn include_in(&self, needs: claim::Needs) -> Result<claim::Needs, errors::Claim> {
        let needs = needs.include(self.claim)?;
        needs.include(claim::Claim::new(settings::Routes::EMPTY).domains(settings::Domain::ONE))
    }
}

impl<ID: Identity> Plan<ID> {
    pub fn new(
        config: config::Config,
        capacity: usize,
        timers: usize,
        origins: usize,
    ) -> Result<Self, CreateError> {
        use dope::manifold::network::client::topology::plan::Layout;

        if origins == 0 || origins > u32::MAX as usize {
            return Err(CreateError::OriginCapacity { actual: origins });
        }
        let origins = num::NonZero::<u32>::MIN.saturating_add(origins as u32 - 1);
        let plan = config::Plan::from_config::<report::Never>(config, capacity, capacity)?;
        let layout = Layout::<ID, { u32::MAX as usize }>::new(capacity)?;
        let claim = layout.claim()?;
        let limit = layout.connections();
        Ok(Self {
            timers,
            origins,
            plan,
            claim,
            limit,
            route: PhantomData,
        })
    }
}

impl<'o, 'q, 'e, 'b, 'c, 'd, ID: Identity> Port<'o, 'q, 'e, 'b, 'c, 'd, ID> {
    pub(in crate::client::dynamic) fn reclaim(&self, outcome: Outcome<'d, ID>) {
        if let Outcome::Connected(conn) = outcome {
            self.protocol.io.close(
                conn,
                close::Directive::reconnect(close::Reason::Local(close::LocalOrigin::Superseded)),
            );
        }
    }

    pub(in crate::client) fn fail(
        &self,
        attempt: &attempt::Completion<'_, 'd, ID>,
        error: Failure,
    ) {
        if let Ok((origin, waiter)) = self.pending.try_settle(attempt, Outcome::Failed(error)) {
            if let Some(waiter) = waiter {
                waiter.wake();
            }
            self.protocol.transport.waiters.wake(origin);
        }
    }
    pub fn capacity(&self) -> usize {
        self.protocol.io.capacity().count().get()
    }

    pub fn try_with<'s, C, R>(
        &'s self,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        run: impl for<'p> FnOnce(dynamic::Connector<'s, 'p, 'o, 'q, 'e, 'b, 'c, 'd, ID>, &mut C) -> R,
    ) -> Result<R, creation::Error>
    where
        ID: 'd,
    {
        use session::app::State;

        let limit = self.limit;
        let setup = session::Setup::new(Connector::bind(self, open::Never), self.protocol.ingress);
        let app = State::try_new(setup, limit, &mut context(owner))?;
        engine::Startup::new(
            app,
            self.pending.source(),
            limit,
            self.protocol.io.config(),
            domain::Local::new(wire::Identity),
        )
        .try_with(owner, context, |engine, owner| {
            run(dynamic::Connector { engine }, owner)
        })
    }
}

impl<ID: Identity> Plan<ID> {
    /// Keeps reply storage alive through the complete dynamic port consumer.
    pub fn try_with<'d, C, R>(
        self,
        owner: &mut C,
        context: impl for<'a> Fn(&'a mut C) -> driver::Context<'a, 'd>,
        run: impl for<'o, 'q, 'e, 'b, 'c> FnOnce(Port<'o, 'q, 'e, 'b, 'c, 'd, ID>, &mut C) -> R,
    ) -> Result<R, CreateError> {
        let config::Plan {
            codec,
            decode,
            response_resident_capacity: resident,
            staging,
            requests,
            transport: net,
            response_segments: parts,
        } = self.plan;
        let mut egress = arena::Store::try_lanes(staging.retained(), staging.capacity())?;
        let proof = context(owner).regions().0.proof();
        let ready = net::Tagged::try_new(net, self.origins, &context(owner).regions().0)
            .map_err(CreateError::Queue)?;
        queue::Queue::try_with(
            context(owner).driver_ref(),
            self.timers,
            num::NonZero::<usize>::MIN.saturating_add(self.origins.get() as usize - 1),
            ready,
            |ready, active| {
                origin::Registry::try_with(self.origins, |origins| {
                    reply::Arena::try_with(net.arena(), proof, |replies| {
                        let port = {
                            let mut driver = context(owner);
                            let builder = &mut driver.storage;
                            let ingress =
                                ingress::Plan::local(ingress::Bytes::new(resident), parts);
                            let parts = segments::Pool::try_new(segments::Plan {
                                segments: parts.get(),
                                chains: parts.get() / 2,
                            })?;
                            let slots = staging.capacity().count();
                            let decode = decode.try_build(builder)?;
                            Port {
                                limit: self.limit,
                                protocol: Protocol {
                                    io: port::Port::try_new(&mut egress, builder.region())?,
                                    codec,
                                    decode,
                                    ingress,
                                    transport: net::Transport::try_ready(
                                        net,
                                        || Ok([]),
                                        active,
                                        ready,
                                        report::Never,
                                        builder,
                                        replies,
                                    )?,
                                    requests: request::Request::try_new(requests)
                                        .map_err(CreateError::Requests)?,
                                    response_segments: parts,
                                    timer: builder.timer(),
                                },
                                origins,
                                pending: mailbox::Domain::try_new(slots, builder)?,
                            }
                        };
                        Ok(run(port, owner))
                    })
                    .map_err(CreateError::from)?
                })
                .map_err(CreateError::Origins)?
            },
        )
        .map_err(creation::Error::Wake)?
    }
}
