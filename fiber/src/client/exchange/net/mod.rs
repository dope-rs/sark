use std::{convert::Infallible, time};

use dope::{
    core::driver::{self, route::Identity, schedule::ready::wake::queue, storage},
    manifold::{
        app::{creation, receive},
        client::{
            close,
            conn::{Activation, CloseStatus, Id},
            port::{self, error},
            session::Input,
        },
    },
};
use dope_fiber::reply::{self, layout};
use o3::{
    base::cell::region,
    buffer::pool,
    depot::{
        capacity::Count,
        queue::resident::{self, Fixed},
    },
};
use sark_client::{
    capacity,
    conn::{self, Kind},
    protocol::codec::output::Bound,
};

use crate::client::{
    config::{self, report},
    exchange::{request, reuse},
};

mod budgets;
mod claim;
mod outcome;
mod output;
mod pools;
mod queues;
mod tagged;
pub(in crate::client) mod waiters;

pub(in crate::client) use claim::Claim;
use claim::{Acquire, Arm};
pub(in crate::client) use outcome::Outcome;
pub(in crate::client) use output::Output;
pub(in crate::client) use pools::Pool;
pub(in crate::client) use queues::{PoolTag, ReadyQueue};
pub(in crate::client) use tagged::Tagged;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client) enum Open {
    Transport(error::Open),
    Reply(reply::Error),
}

impl Open {
    pub(in crate::client) fn close(self) -> close::Directive {
        let reason = match self {
            Self::Transport(error::Open::Unknown | error::Open::Exhausted) => {
                close::Reason::Capacity(close::Capacity::Transport)
            }
            Self::Transport(error::Open::Occupied) => {
                close::Reason::Local(close::LocalOrigin::ResourceFailure)
            }
            Self::Reply(_) => close::Reason::Capacity(close::Capacity::IngressRetention),
        };
        close::Directive::cut_final(reason)
    }
}

#[must_use = "close cause and category must be handled"]
pub(in crate::client) enum Close<E> {
    Capacity(capacity::Error),
    Transport(conn::Error<E>),
}

impl<E> Close<E> {
    pub(in crate::client) const fn from(reason: close::Reason) -> Self {
        use close::{Capacity, Reason};

        let kind = match reason {
            Reason::Capacity(Capacity::IngressStaging)
            | Reason::Ingress(receive::Failure::ParseCapacity | receive::Failure::FinishCapacity) =>
            {
                return Self::Capacity(capacity::Error::ResponseStaging);
            }
            Reason::Capacity(Capacity::IngressRetention) => {
                return Self::Capacity(capacity::Error::ResponseRetention);
            }
            Reason::Capacity(Capacity::Transport) => {
                return Self::Capacity(capacity::Error::Transport);
            }
            Reason::Capacity(Capacity::IdentityExhausted) => {
                return Self::Capacity(capacity::Error::IdentityExhausted);
            }
            Reason::Receive(error) => return Self::Transport(conn::Error::Park(error)),
            Reason::Ingress(error) => return Self::Transport(conn::Error::Ingress(error)),
            Reason::Egress(error) => return Self::Transport(conn::Error::Egress(error)),
            Reason::Remote => Kind::RemoteEof,
            Reason::Protocol => Kind::Protocol,
            Reason::Transport => Kind::Transport,
            Reason::Timeout(kind) => Kind::Timeout(kind),
            Reason::EndpointRetired => Kind::EndpointRetired,
            Reason::Local(origin) => Kind::Local(origin),
        };
        Self::Transport(conn::Error::Classified(kind))
    }
}

#[derive(Clone, Copy)]
pub(in crate::client) struct Plan {
    pool: budgets::Plan,
    idle_policy: reuse::Policy,
}

impl Plan {
    pub(in crate::client) const fn reports(&self) -> Count {
        self.pool.report_capacity
    }

    pub(in crate::client) const fn arena(&self) -> layout::Layout {
        self.pool.arena
    }

    pub(in crate::client) fn new(
        capacity: usize,
        entries: usize,
        bytes: usize,
        output: Bound,
        idle: time::Duration,
        reports: Count,
    ) -> Result<Self, config::Error> {
        Ok(Self {
            pool: budgets::Plan::new(capacity, entries, bytes, output, reports)?,
            idle_policy: reuse::Policy::new(idle),
        })
    }
}

pub(in crate::client) struct Transport<
    'f,
    'e,
    'b,
    'c,
    'd,
    ID: Identity,
    Tag = (),
    E = Infallible,
    F = report::Never,
> where
    Tag: PoolTag<'d, ID>,
    F: report::Policy<E>,
    E: 'f,
{
    pub(in crate::client) pool: pools::Pool<'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>,
    pub(in crate::client) waiters: waiters::Waiters<'d, ID, Tag>,
    idle_policy: reuse::Policy,
}

impl<'f, 'e, 'b, 'c, 'd, ID: Identity, Tag: PoolTag<'d, ID>, E, F>
    Transport<'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>
where
    F: report::Policy<E>,
    E: 'f,
{
    pub(in crate::client) fn input(
        &self,
        id: Id<'d, ID>,
        input: Input,
        close: CloseStatus,
        region: &mut region::Token<region::Identity<'d>>,
    ) {
        if !matches!(close, CloseStatus::Open) {
            self.pool.begin_close(region, id);
        } else if let Some(tag) = self.pool.input(region, id, input) {
            self.waiters.wake(tag);
        }
    }

    pub(in crate::client) fn try_activate(
        &self,
        io: &port::Port<'_, 'd, pool::Cursor, ID>,
        active: &Activation<'_, 'd, ID>,
    ) -> Result<(), Open> {
        let position = self.pool.directory.try_activate(io, active)?;
        let response = self.pool.responses.lane(position);
        response.bind_retire(active.ready());
        Ok(())
    }
}

impl<'f, 'e, 'b, 'c, 'd, ID: Identity, E, F> Transport<'f, 'e, 'b, 'c, 'd, ID, (), E, F>
where
    F: report::Policy<E>,
    E: 'f,
{
    pub(in crate::client) fn note_connect(
        &self,
        driver: &mut driver::Context<'_, 'd>,
        id: Id<'d, ID>,
    ) -> Result<(), Open> {
        let state = self
            .idle_policy
            .available(|| driver.driver_ref().clock().now());
        let token = driver.region_token();
        self.pool.try_connect(id)?(token, (), state);
        self.waiters.wake(());
        self.waiters.wake_io();
        Ok(())
    }
}

impl<'f, 'e, 'b, 'c, 'd, ID: Identity, Tag: PoolTag<'d, ID>, E, F>
    Transport<'f, 'e, 'b, 'c, 'd, ID, Tag, E, F>
where
    F: report::Policy<E>,
    E: 'f,
{
    pub(in crate::client) fn try_ready(
        plan: Plan,
        conn: impl FnOnce() -> Result<Tag::Connections, resident::Error>,
        active: queue::Queue<'d, Tag::Lanes>,
        ready: Tag::Ready,
        reports: F::Source<'f>,
        context: &storage::Context<'_, 'd>,
        replies: &'e reply::Arena<
            'b,
            'c,
            'd,
            Outcome<'d, F::Detail<'f>>,
            Outcome<'d, F::Detail<'f>>,
            request::Kind,
        >,
    ) -> Result<Self, creation::Error> {
        Ok(Self {
            pool: pools::Pool::try_new(ready, reports, context.region(), replies)
                .map_err(creation::Error::Allocation)?,
            waiters: waiters::Waiters::try_new(active, conn()).map_err(creation::Error::Wake)?,
            idle_policy: plan.idle_policy,
        })
    }
}

impl<'f, 'e, 'b, 'c, 'd, ID: Identity, E, F> Transport<'f, 'e, 'b, 'c, 'd, ID, (), E, F>
where
    F: report::Policy<E>,
    E: 'f,
{
    pub(in crate::client) fn try_new(
        plan: Plan,
        waiters: usize,
        reports: F::Source<'f>,
        context: &storage::Context<'_, 'd>,
        replies: &'e reply::Arena<
            'b,
            'c,
            'd,
            Outcome<'d, F::Detail<'f>>,
            Outcome<'d, F::Detail<'f>>,
            request::Kind,
        >,
    ) -> Result<Self, creation::Error> {
        let ready = queues::FixedReady::try_new(plan.pool.capacity(), context.region())
            .map_err(creation::Error::Allocation)?;
        Self::try_ready(
            plan,
            || {
                Ok([queue::Queue::try_new(
                    context.driver(),
                    waiters,
                    Fixed::<1>,
                )?])
            },
            queue::Queue::try_new(context.driver(), waiters, Fixed::<1>)
                .map_err(creation::Error::Wake)?,
            ready,
            reports,
            context,
            replies,
        )
    }
}
