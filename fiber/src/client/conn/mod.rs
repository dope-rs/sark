use std::{marker::PhantomData, mem};

use app::{
    admit::Registered,
    request::{self, Turn},
};
use codec::{mode, staging, storage};
use dope::{
    core::{
        Errno, driver,
        driver::route::{Identity, KeyTag},
        io::socket,
    },
    manifold::{
        app::Park,
        client::{
            app::{self, connect::Full, failure::Observed},
            attempt, close, conn,
            session::{
                self, Connection,
                app::{admit, drain::Ready, request::Source},
                codec, policy, response, turns,
            },
        },
    },
    net::{
        link::{egress, event, pool::change::opening},
        wire::{self, Spec, terminal},
    },
};
use driver::schedule::{Progress, clock};
use dynamic::channel::{Failure, Port};
use o3::{
    base::{Call, cell::region, time::Positive},
    buffer,
    buffer::{hold, pool, resident},
};
use policy::{end, start};
use sark_client::protocol::codec::{Codec, State, Storage};

use crate::client::{
    channel,
    config::report,
    dynamic,
    exchange::{Reuse, net},
};

mod kind;
#[doc(hidden)]
pub mod open;
pub(in crate::client) mod route;
mod schedule;

#[doc(hidden)]
pub use kind::Kind;
use route::Route;

/// HTTP protocol state paired with its original channel borrow by dope.
pub struct Connector<'s, ID: Identity, P, O = open::Never> {
    pub(in crate::client) root: &'s P,
    open: O,
    route: crate::client::Covariant<ID>,
}

impl<'s, ID: Identity, P, O> Connector<'s, ID, P, O> {
    pub(crate) const fn bind(root: &'s P, open: O) -> Self {
        Self {
            root,
            open,
            route: PhantomData,
        }
    }
}

impl<'s, 'd, ID: Identity, P, O> session::Session<'d, ID> for Connector<'s, ID, P, O>
where
    P: Kind<'d, ID>,
{
    type Codec = Codec;
    type Connect = Full;
    type ConnState = Reuse;
    type Failure = Observed;
    type Receipt = ();
    type Requests = request::Enabled;
    type Send = pool::Cursor;

    fn codec(&self) -> &Self::Codec {
        self.root.codec()
    }
}

impl<'s, 'f: 's, 'e: 's, 'b: 'e, 'c: 'b, 'd: 'c + 'e + 'f, ID: Identity, P, O>
    session::Commit<'d, net::Output<'s, 'd, ID, P>, ID> for Connector<'s, ID, P, O>
where
    P: Route<'f, 'e, 'b, 'c, 'd, ID>,
{
    fn commit(
        &mut self,
        prepared: response::Committed<net::Output<'s, 'd, ID, P>>,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) {
        let mut parts = ctx.parts();
        if let Some(close) = net::Output::publish(prepared, &mut parts.driver, parts.state) {
            parts.request(close);
        }
    }
}

impl<'s, 'f: 's, 'e: 's, 'b: 'e, 'c: 'b, 'd: 'c + 'e + 'f, ID: Identity, P, O>
    session::Parser<'d, ID> for Connector<'s, ID, P, O>
where
    ID: 's,
    P: Route<'f, 'e, 'b, 'c, 'd, ID>,
    P::Tag: 's,
    P::State: 's,
    P::Position: 's,
{
    type Mode = mode::Stored;
    type Response = response::Commit<net::Output<'s, 'd, ID, P>>;
}

impl<'d, ID: Identity, P, O> session::Output<'d, ID> for Connector<'_, ID, P, O>
where
    P: Kind<'d, ID>,
{
    fn flush<'w, W: wire::Spec + 'w>(
        _: &W::Connection<'w, ID>,
    ) -> Option<
        impl for<'a, 'x, 't, 'io> Call<(
            &'a mut Self,
            &'x mut turns::Context<'t, 'io, 'd, Self::ConnState, Self::Send, ID>,
            session::Input,
        )>,
    > {
        Some(
            |(owner, ctx, input): (
                &mut Self,
                &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
                session::Input,
            )| {
                let id = ctx.conn_id;
                owner
                    .root
                    .input(id, input, ctx.close_status(), ctx.region());
            },
        )
    }
}

impl<'d, ID: Identity, P, O> session::Target<'d, ID, { u32::MAX as usize }>
    for Connector<'_, ID, P, O>
where
    P: Kind<'d, ID>,
    Self: session::Parser<'d, ID> + session::Retirement<'d, ID> + session::Scheduling<'d, ID>,
{
}

impl<'s, 'f: 's, 'e: 's, 'b: 'e, 'c: 'b, 'd: 'c + 'e + 'f, ID: Identity, P, O>
    session::Stored<'d, ID> for Connector<'s, ID, P, O>
where
    ID: 's,
    P: Route<'f, 'e, 'b, 'c, 'd, ID>,
    P::Tag: 's,
    P::State: 's,
    P::Position: 's,
{
    type Delivery = response::Delivery<net::Output<'s, 'd, ID, P>>;

    fn blocked(&self, _: conn::Id<'d, ID>, _: &mut State<'d>) -> session::Blocking {
        session::Blocking::Unsupported
    }

    fn decode<S>(
        &mut self,
        _: conn::Id<'d, ID>,
        storage: &mut Storage<S>,
        parser: &mut State<'d>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, Reuse, pool::Cursor, ID>,
    ) -> codec::Step<Self::Delivery>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        let root = self.root;
        let protocol = root.protocol();
        let pool = &protocol.transport.pool;
        let mut current = None;
        protocol.codec.bind(storage, parser, || {
            current
                .insert(pool.directory.try_current(ctx.conn_id))
                .as_ref()
                .ok()
                .and_then(|current| {
                    pool.responses
                        .lane(current.position())
                        .control()
                        .front_tag(ctx.region())
                        .copied()
                        .map(crate::client::exchange::request::Kind::transition)
                })
        });
        codec::Stateful::decode(&protocol.codec, storage, parser, budget, limit, move |_| {
            codec::Admission::Ready(move |parsed, parser: &mut State<'d>| {
                let current = current.unwrap_or_else(|| pool.directory.try_current(ctx.conn_id));
                net::Output::prepare(root, current, parsed, parser, ctx)
            })
        })
    }

    fn finish<'slot, 'id, S>(
        &mut self,
        input: storage::Owner<'slot, 'id, Storage<S>, State<'d>>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, Reuse, pool::Cursor, ID>,
    ) -> (
        storage::Owner<'slot, 'id, Storage<S>, State<'d>>,
        Result<codec::Finish<Self::Delivery>, Self::Delivery>,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        let root = self.root;
        codec::Stateful::finish(&root.protocol().codec, input, budget, limit, move |_| {
            codec::Admission::Ready(move |parsed, parser: &mut State<'d>| {
                let current = root
                    .protocol()
                    .transport
                    .pool
                    .directory
                    .try_current(ctx.conn_id);
                net::Output::prepare(root, current, parsed, parser, ctx)
            })
        })
    }
}

impl<'s, 'f: 's, 'e: 's, 'b: 'e, 'c: 'b, 'd: 'c + 'e + 'f, ID: Identity, P, O> Source<'d, ID>
    for Connector<'s, ID, P, O>
where
    Self: session::Parser<'d, ID, Requests = request::Enabled, Send = pool::Cursor>,
    P: Route<'f, 'e, 'b, 'c, 'd, ID>,
{
    fn drain<'w, 'io, 'turn, W: Spec + 'd>(
        &self,
        turn: &mut Turn<'turn, 'io, 'd, Connection<'w, 'd, ID, Self, W>, Self::Send, ID>,
    ) where
        'd: 'w,
    {
        let (mut requests, _) = turn.requests();
        self.root.protocol().io.drain(&mut requests);
    }
}

impl<'s, 'f: 's, 'e: 's, 'b: 'e, 'c: 'b, 'd: 'c + 'e + 'f, ID: Identity, P, O>
    admit::Register<'d, ID> for Connector<'s, ID, P, O>
where
    P: Route<'f, 'e, 'b, 'c, 'd, ID>,
{
    fn register(&self, entry: &conn::Activation<'_, 'd, ID>) -> app::Admission {
        let protocol = self.root.protocol();
        match protocol.transport.try_activate(&protocol.io, entry) {
            Ok(()) => app::Admission::Attached,
            Err(error) => self.root.reject(entry.completion(), error),
        }
    }
}

impl<'s, 'f: 's, 'e: 's, 'b: 'e, 'c: 'b, 'd: 'c + 'e + 'f, ID: Identity, P, O>
    session::Connected<'d, ID> for Connector<'s, ID, P, O>
where
    P: Route<'f, 'e, 'b, 'c, 'd, ID>,
{
    type Activation = Registered;

    fn connect(
        &mut self,
        attempt: attempt::Completion<'_, 'd, ID>,
        _: socket::Addr,
        mut ctx: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) {
        let conn = ctx.connection();
        match self.root.try_connect(attempt, conn, &mut ctx.driver) {
            Ok(()) => {}
            Err(route::Connect::Open(error)) => {
                ctx.close.request(error.close());
            }
            Err(route::Connect::Mailbox(_cause, _outcome)) => {
                ctx.close
                    .request(close::Directive::reconnect(close::Reason::Local(
                        close::LocalOrigin::Superseded,
                    )));
            }
        }
    }
}

impl<'s, 'f: 's, 'e: 's, 'b: 's, 'c: 'b, 'w: 'f, 'd: 'c + 'e + 'f, ID, W, F, O>
    session::Failed<'d, ID>
    for Connector<'s, ID, channel::Port<'f, 'e, 'b, 'c, 'w, 'd, ID, W, F>, O>
where
    ID: Identity,
    W: wire::Spec + 'w,
    F: 'w + report::Policy<terminal::Error<'w, W>>,
{
    fn failed(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        _: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.root.protocol.transport.waiters.wake_io();
    }
}

impl<'s, 'f: 's, 'e: 's, 'b: 's, 'c: 'b, 'w: 'f, 'd: 'c + 'e + 'f, ID, W, F, O>
    session::Exhausted<'d, ID>
    for Connector<'s, ID, channel::Port<'f, 'e, 'b, 'c, 'w, 'd, ID, W, F>, O>
where
    ID: Identity,
    W: wire::Spec + 'w,
    F: 'w + report::Policy<terminal::Error<'w, W>>,
{
    fn exhaust(
        &mut self,
        _: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        if self.root.exhausted.get().is_none() {
            self.root.exhausted.set(Some(cause));
        }
        let waiters = &self.root.protocol.transport.waiters;
        waiters.begin_drain();
    }
}

impl<'s, 'f: 's, 'e: 's, 'b: 'e, 'c: 'b, 'd: 'c + 'e + 'f, ID: Identity, P, O>
    session::Retirement<'d, ID> for Connector<'s, ID, P, O>
where
    P: Route<'f, 'e, 'b, 'c, 'd, ID>,
{
    type Drain = Ready;

    fn take_close(&self, id: conn::Id<'d, ID>) -> Option<close::Directive> {
        self.root.protocol().io.take_close(id)
    }

    fn transmit_failed(
        &self,
        id: conn::Id<'d, ID>,
        failure: conn::Transmit,
        regions: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) -> Option<conn::Transmit> {
        self.root.protocol().transport.pool.failed(
            &mut regions.0,
            id,
            report::Failure::Transmit(failure),
        );
        None
    }

    fn io_fail(
        &mut self,
        error: Errno,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) {
        let id = ctx.conn_id;
        self.root.protocol().transport.pool.failed(
            ctx.region(),
            id,
            report::Failure::Receive(error),
        );
    }

    fn disconnect(&mut self, ctx: turns::Retired<'_, 'd, Self::ConnState, ID, Self>) {
        let (drained, _, token) = ctx.into_parts();
        net::Pool::finish_close(self, drained, token);
    }

    fn quarantine(
        &self,
        conn: conn::Id<'d, ID>,
        _: &mut Self::ConnState,
        _: close::Reason,
        regions: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let region = &mut regions.0;
        self.root
            .protocol()
            .transport
            .pool
            .begin_close(region, conn);
    }

    fn clear_send<'turn>(
        &self,
        conn: conn::Id<'d, ID>,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        let (work, region) = ctx.parts();
        self.root.protocol().io.retire(conn, work, region, drop)
    }

    fn clear_recv<'turn>(
        &self,
        conn: conn::Id<'d, ID>,
        reason: close::Reason,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        let (work, region) = ctx.parts();
        let pool = &self.root.protocol().transport.pool;
        pool.retire(conn, work, region, reason)
    }
}

impl<'s, 'o, 'q, 'e, 'b, 'c, 'd, ID: Identity, O> session::Failed<'d, ID>
    for Connector<'s, ID, Port<'o, 'q, 'e, 'b, 'c, 'd, ID>, O>
{
    fn failed(
        &mut self,
        attempt: attempt::Completion<'_, 'd, ID>,
        cause: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        self.root.fail(&attempt, Failure::Attempt(cause));
    }
}

impl<'s, 'o, 'q, 'e, 'b, 'c, 'd, ID: Identity, O> session::Exhausted<'d, ID>
    for Connector<'s, ID, Port<'o, 'q, 'e, 'b, 'c, 'd, ID>, O>
{
    fn exhaust(
        &mut self,
        attempt: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        let cause = match cause {
            opening::Exhaustion::Identity => Failure::IdentityExhausted,
            opening::Exhaustion::Submission(error) => Failure::Attempt(
                attempt::ConnectFault::Transport(event::ConnectError::Admission(error)),
            ),
        };
        self.root.fail(&attempt, cause);
    }
}

impl<'s, 'w, 'd, W, ID: Identity, P, O> policy::Compatible<'w, 'd, W, ID>
    for Connector<'s, ID, P, O>
where
    W: Spec + 'w,
    P: Kind<'d, ID> + end::Sink<'w, 'd, W, ID>,
    O: start::Sink<'w, 'd, W, ID>,
{
    type Open = O;
    type Terminal = P;

    fn open(&self) -> &Self::Open {
        &self.open
    }

    fn terminal(&self) -> &Self::Terminal {
        self.root
    }
}

impl<'s, 'd, ID: Identity, P, O> session::Scheduling<'d, ID> for Connector<'s, ID, P, O>
where
    P: schedule::Schedule<'d, ID>,
    Self: session::Session<'d, ID>,
{
    fn inbound(
        &self,
        conn: conn::Id<'d, ID>,
        _: &Self::ConnState,
        _: Positive,
        _: &mut region::Token<region::Identity<'d>>,
    ) -> app::Inbound {
        self.root.inbound(conn)
    }

    fn park() -> Option<impl for<'v, 'w, 'x, 'y> Call<(&'v mut Self, &'w mut Park<'x, 'y, 'd>)>> {
        Some(|(session, park): (&mut Self, &mut Park<'_, '_, 'd>)| {
            session.root.park(park.work(), &mut park.regions().0)
        })
    }

    fn shutdown() -> Option<impl for<'v> Call<&'v mut Self>> {
        Some(|session: &mut Self| session.root.shutdown())
    }

    fn progress(
        &self,
        region: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> Progress<'d> {
        self.root.progress(region)
    }
}

const _: () = assert!(
    mem::size_of::<
        Connector<
            'static,
            KeyTag<0>,
            channel::Port<'static, 'static, 'static, 'static, 'static, 'static>,
        >,
    >() == mem::size_of::<&()>()
);
