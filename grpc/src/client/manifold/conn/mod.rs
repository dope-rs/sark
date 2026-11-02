use std::{mem, ops};

use dope::{
    core::{
        driver::{route::Identity, schedule::ready},
        random,
    },
    manifold::{
        client::{
            app::request,
            close, conn,
            session::{
                response::{self, work},
                turns,
            },
        },
        service::bindings::endpoint,
    },
    net::link::egress::data::{compose, pair},
};
use o3::buffer::{pool, resident};
use sark_h2::client;

use crate::{
    client::{
        config, io,
        manifold::{Driver, channel::Port, driver::MAX_CONNECTIONS, error, settings},
        receive::policies::Notify,
        session,
        state::plan,
    },
    protocol::section,
};

mod active;
mod phase;

use phase::Phase;

#[doc(hidden)]
pub enum Pending<'d> {
    Owner(resident::Shared<'d>),
    Resume,
}

pub(in crate::client::manifold) enum Progress<'d> {
    Blocked(Pending<'d>),
    Ready,
    Drained,
}

const _: () = assert!(
    mem::size_of::<Progress<'static>>() <= mem::size_of::<(Option<Pending<'static>>, bool)>()
);

pub(in crate::client::manifold) struct State<'d, ID: Identity> {
    protocol: plan::Plan<client::Plan<section::Plan>>,
    phase: Phase<'d, ID>,
    prefixes: pool::Pool,
}

impl<'d, ID: Identity> State<'d, ID> {
    pub(in crate::client::manifold) const fn limits(&self) -> config::Limits {
        self.protocol.limits
    }

    pub(in crate::client::manifold) fn new(
        protocol: plan::Plan<client::Plan<section::Plan>>,
        layout: pool::Layout,
    ) -> Result<Self, settings::Error> {
        Ok(Self {
            protocol,
            phase: Phase::new(),
            prefixes: pool::Pool::try_from(layout).map_err(settings::Error::Prefixes)?,
        })
    }

    pub(in crate::client::manifold) fn connect(
        &mut self,
        lifetime: conn::Lifetime<'d, ID>,
        ready: ready::Target<'d>,
        seed: &random::Seed,
    ) -> Result<(), close::Reason> {
        self.phase.connect(lifetime, ready, self.protocol, seed)
    }

    pub(in crate::client::manifold) fn signal(&self, conn: conn::Id<'d, ID>) {
        self.phase.wake(conn);
    }

    pub(in crate::client::manifold) fn flush<'scope>(
        &mut self,
        requests: &mut request::Requests<
            '_,
            '_,
            '_,
            'd,
            compose::Either<pool::Cursor, pair::Source<resident::Shared<'d>>>,
            ID,
        >,
        done: &mut Notify<'scope, '_, '_>,
    ) -> Result<io::Flush, io::Failure> {
        let Some(active) = self.phase.get_mut(requests.connection()) else {
            return Ok(io::Flush::Empty);
        };
        let Some(prefix) = self.prefixes.cursor() else {
            return Ok(io::Flush::Empty);
        };
        active.flush(prefix, requests, done)
    }

    pub(in crate::client::manifold) fn has_output(&self, conn: conn::Id<'d, ID>) -> bool {
        self.phase.has_output(conn)
    }

    pub(in crate::client::manifold) fn owner_cap(&self) -> usize {
        self.phase.owner_cap()
    }

    pub(in crate::client::manifold) fn get_mut(
        &mut self,
        conn: conn::Id<'d, ID>,
    ) -> Option<&mut session::Session<'d, client::Plan<section::Plan>, resident::Shared<'d>>> {
        Some(&mut self.phase.get_mut(conn)?.session)
    }

    pub(in crate::client::manifold) fn resume<'scope>(
        &mut self,
        conn: conn::Id<'d, ID>,
        pending: response::Item<Pending<'d>>,
        done: &mut Notify<'scope, '_, '_>,
    ) -> Result<Progress<'d>, crate::Status> {
        let Some(active) = self.phase.get_mut(conn) else {
            return Ok(Progress::Ready);
        };
        active.resume(pending, done)
    }
}

impl<'d, ID: Identity> ops::Deref for State<'d, ID> {
    type Target = Phase<'d, ID>;

    fn deref(&self) -> &Self::Target {
        &self.phase
    }
}

impl<'d, ID: Identity> ops::DerefMut for State<'d, ID> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.phase
    }
}
impl<'d> Pending<'d> {
    pub(in crate::client::manifold) fn resume<'e, ID: Identity>(
        item: response::Item<Self>,
        port: &Port<'e, 'd, ID>,
        context: &mut turns::Context<
            '_,
            '_,
            'd,
            (),
            compose::Either<pool::Cursor, pair::Source<resident::Shared<'d>>>,
            ID,
        >,
    ) -> response::Settlement<Pending<'d>> {
        let conn = context.conn_id;
        let mut parts = context.parts();
        let mut state = port.shared.source(parts.driver.state_token());
        let result = state.with(|state, done| state.resume(conn, item, done));
        let next = match result {
            Ok(Progress::Blocked(pending)) => response::Settlement::Blocked(pending),
            Ok(Progress::Ready) => response::Settlement::Ready,
            Ok(Progress::Drained) => {
                parts
                    .close
                    .request(close::Directive::reconnect(close::Reason::Remote));
                response::Settlement::Ready
            }
            Err(status) => {
                state.fail(conn, error::Error::Protocol(status));
                parts
                    .close
                    .request(close::Directive::reconnect(close::Reason::Protocol));
                response::Settlement::Ready
            }
        };
        port.shared.wake();
        port.shared.wake_space(&state);
        next
    }
}

impl<'life, 'e: 'life, 'd: 'life, ID: Identity>
    response::Action<'life, 'd, Port<'e, 'd, ID>, Driver, ID, MAX_CONNECTIONS> for Pending<'d>
{
    fn commit<'send, A: work::Admission>(
        item: response::Committed<Self>,
        owner: &mut endpoint::State<'life, 'd, Port<'e, 'd, ID>, Driver, ID, MAX_CONNECTIONS>,
        _: &mut A,
        context: &mut turns::Context<
            '_,
            'send,
            'd,
            (),
            compose::Either<pool::Cursor, pair::Source<resident::Shared<'d>>>,
            ID,
        >,
    ) -> response::Settlement<Self> {
        Self::resume(item.into_item(), owner.root(), context)
    }

    fn settle<'send, A: work::Admission>(
        item: response::Item<Self>,
        owner: &mut endpoint::State<'life, 'd, Port<'e, 'd, ID>, Driver, ID, MAX_CONNECTIONS>,
        work: &mut A,
        context: &mut turns::Context<
            '_,
            'send,
            'd,
            (),
            compose::Either<pool::Cursor, pair::Source<resident::Shared<'d>>>,
            ID,
        >,
    ) -> response::Settlement<Self> {
        let port = owner.root();
        match work.admit_with(item, |pending| Self::resume(pending, port, context)) {
            work::Capacity::Available(settle) => settle,
            work::Capacity::Exhausted(pending) => response::Settlement::Yield(pending.into_inner()),
        }
    }
}
