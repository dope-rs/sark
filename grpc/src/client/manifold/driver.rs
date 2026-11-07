use core::convert::Infallible;

use bindings::endpoint::State;
use buffer::pool;
use dope::{
    core::{
        Errno,
        driver::{
            self,
            route::Identity,
            schedule::{Progress, app::Work, clock},
        },
        io::socket,
    },
    manifold::{
        client::{
            app::{self, admit::Registered, connect::Full, failure::Observed, request},
            attempt, close, conn, port,
            session::{
                app::drain::Ready,
                codec::{self, mode},
                policy,
                protocol::{Plain, Spec},
                response, turns,
            },
        },
        service::bindings,
    },
    net::link::{egress, pool::change::opening},
};
use egress::data::{compose, pair};
use o3::{
    base::{Call, cell::region},
    buffer,
    buffer::{hold, hold::scope::Deferred, resident, write::consume::scope},
};
use policy::{admit, output};
use port::error::Open;
use sark_core::manifold::client::domain;
use sark_h2::client;

use crate::{
    client::{
        io,
        manifold::{channel::Port, conn::Pending, error},
    },
    protocol::section,
};

pub(super) const MAX_CONNECTIONS: usize = u32::MAX as usize;

type Owner<'life, 'e, 'd, ID> = State<'life, 'd, Port<'e, 'd, ID>, Driver, ID, MAX_CONNECTIONS>;

#[doc(hidden)]
pub struct Driver {
    codec: codec::Bytes,
}

impl Driver {
    pub(super) fn new() -> Self {
        Self {
            codec: codec::Bytes,
        }
    }
}

const _: () = assert!(core::mem::size_of::<Driver>() == 0);

impl<'life, 'e: 'life, 'd: 'life, ID: Identity>
    Spec<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Driver
{
    type Codec = codec::Bytes;
    type Connect = Full;
    type ConnState = ();
    type Failure = Observed;
    type Mode = mode::Plain;
    type Receipt = ();
    type Requests = request::Enabled;
    type Response = response::Resume<Pending<'d>, Pending<'d>>;
    type Send = compose::Either<pool::Cursor, pair::Source<resident::Shared<'d>>>;

    fn codec<'a>(owner: &'a Owner<'life, 'e, 'd, ID>) -> &'a Self::Codec {
        &owner.logic().codec
    }
}

impl<'life, 'e: 'life, 'd: 'life, ID: Identity>
    Plain<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Driver
{
    fn reject(
        owner: &mut Owner<'life, 'e, 'd, ID>,
        cause: scope::Error,
        context: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) -> response::Delivery<Pending<'d>> {
        let port = owner.root();
        let conn = context.conn_id;
        let mut parts = context.parts();
        let state = port.shared.get_mut(parts.driver.state_token());
        state.fail(conn, error::Error::Input(cause));
        parts
            .close
            .request(close::Directive::reconnect(close::Reason::Protocol));
        port.shared.wake();
        port.shared.wake_space(state);
        state.signal(conn);
        response::Delivery::Committed
    }

    fn deliver<'input, R: buffer::Cursor + hold::raw::Retain<'d>>(
        owner: &mut Owner<'life, 'e, 'd, ID>,
        head: Deferred<'input, 'd, R>,
        context: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) -> response::Delivery<Pending<'d>>
    where
        'd: 'input,
    {
        let port = owner.root();
        let conn = context.conn_id;
        let mut parts = context.parts();
        let state = port.shared.get_mut(parts.driver.state_token());
        let Some(session) = state.get_mut(conn) else {
            return response::Delivery::Committed;
        };
        let retained = match session.try_bind(*head.budget()) {
            Ok(()) => head
                .bind(
                    <client::Plan<section::Plan> as domain::Plan<resident::Shared<'d>>>::RETENTION,
                )
                .try_into()
                .map_err(error::Error::Retention),
            Err(cause) => Err(error::Error::Protocol(cause.into())),
        };
        match retained {
            Ok(head) => response::Delivery::Prepared(Pending::Owner(head)),
            Err(error) => {
                state.fail(conn, error);
                port.shared.wake_all();
                parts
                    .close
                    .request(close::Directive::reconnect(close::Reason::Local(
                        close::LocalOrigin::ResourceFailure,
                    )));
                response::Delivery::Committed
            }
        }
    }
}

impl<'life, 'e: 'life, 'd: 'life, ID: Identity + 'life>
    policy::Output<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Driver
{
    fn sent() -> impl for<'a, 'x> Call<(
        &'a mut Owner<'life, 'e, 'd, ID>,
        conn::Id<'d, ID>,
        egress::Pending,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<Infallible, _>::Connection(
            |(owner, conn, token): (
                &mut Owner<'life, 'e, 'd, ID>,
                conn::Id<'d, ID>,
                &mut region::Token<region::Identity<'d, 1>>,
            )| {
                let port = owner.root();
                let state = port.shared.get_mut(token);
                port.shared.wake_space(state);
                if state.has_output(conn) {
                    state.signal(conn);
                }
            },
        )
    }

    fn cancel() -> impl for<'a, 'x> Call<(
        &'a Owner<'life, 'e, 'd, ID>,
        Self::Send,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Cancel::<Infallible, Infallible, _>::Context(
            |(owner, _, token): (
                &Owner<'life, 'e, 'd, ID>,
                Self::Send,
                &mut region::Token<region::Identity<'d, 1>>,
            )| {
                let port = owner.root();
                let state = port.shared.get_mut(token);
                port.shared.wake_space(state);
            },
        )
    }
}

impl<'life, 'e: 'life, 'd: 'life, ID: Identity>
    policy::Requests<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Driver
{
    fn drain(
        owner: &Owner<'life, 'e, 'd, ID>,
        requests: &mut request::Requests<'_, '_, '_, 'd, Self::Send, ID>,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let conn = requests.connection();
        let port = owner.root();
        let mut state = port.shared.source(token);
        port.io.drain(requests);
        loop {
            match state.with(|state, done| state.flush(requests, done)) {
                Ok(io::Flush::Written) => continue,
                Ok(io::Flush::Exhausted) => state.signal(conn),
                Ok(io::Flush::Saturated | io::Flush::Empty) => {}
                Err(io::Failure::Prepare(failure)) => {
                    state.fail(conn, error::Error::Prepare(failure));
                    requests.close(close::Directive::cut_retry(close::Reason::Capacity(
                        close::Capacity::Transport,
                    )));
                }
                Err(io::Failure::Payload(failure)) => {
                    state.fail(conn, error::Error::Payload(failure));
                    requests.close(close::Directive::cut_retry(close::Reason::Capacity(
                        close::Capacity::Transport,
                    )));
                }
                Err(io::Failure::Reserve(cause)) => {
                    state.fail(conn, error::Error::Reserve(cause));
                    requests.close(close::Directive::cut_retry(close::Reason::Egress(cause)));
                }
                Err(io::Failure::Drive(cause)) => {
                    state.fail(conn, error::Error::Protocol(cause));
                    requests.close(close::Directive::cut_retry(close::Reason::Protocol));
                }
            }
            break;
        }
    }
}

impl<'life, 'e: 'life, 'd: 'life, ID: Identity>
    admit::Register<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Driver
{
    fn register(
        owner: &Owner<'life, 'e, 'd, ID>,
        active: &conn::Activation<'_, 'd, ID>,
    ) -> app::Admission {
        match owner.root().io.try_activate(active) {
            Ok(()) => app::Admission::Attached,
            Err(Open::Exhausted) => app::Admission::Rejected(close::Directive::cut_final(
                close::Reason::Capacity(close::Capacity::Transport),
            )),
            Err(Open::Unknown | Open::Occupied) => {
                app::Admission::Rejected(close::Directive::cut_final(close::Reason::Protocol))
            }
        }
    }
}

impl<'life, 'e: 'life, 'd: 'life, ID: Identity>
    policy::Connected<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Driver
{
    type Activation = Registered;

    fn connect(
        owner: &mut Owner<'life, 'e, 'd, ID>,
        _: attempt::Completion<'_, 'd, ID>,
        _: socket::Addr,
        mut ctx: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) {
        let port = owner.root();
        let conn = ctx.connection();
        let seed = ctx.driver.driver_ref().random_seed();
        let state = port.shared.get_mut(ctx.driver.state_token());
        if let Err(reason) = state.connect(ctx.lifetime, ctx.ready, seed) {
            ctx.close.request(close::Directive::reconnect(reason));
            return;
        }
        port.shared.cancels.ready(ctx.ready);
        port.shared.wake();
        port.shared.wake_senders();
        state.signal(conn);
    }
}

impl<'life, 'e: 'life, 'd: 'life, ID: Identity>
    policy::Failed<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Driver
{
    fn failed(
        owner: &mut Owner<'life, 'e, 'd, ID>,
        _: attempt::Completion<'_, 'd, ID>,
        _: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        owner.root().shared.wake();
        owner.root().shared.wake_senders();
    }
}

impl<'life, 'e: 'life, 'd: 'life, ID: Identity>
    policy::Exhausted<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Driver
{
    fn exhaust(
        owner: &mut Owner<'life, 'e, 'd, ID>,
        _: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        context: &mut driver::Context<'_, 'd>,
    ) {
        let port = owner.root();
        port.shared.get_mut(context.state_token()).exhaust(cause);
        port.shared.wake();
        port.shared.wake_senders();
    }
}

impl<'life, 'e: 'life, 'd: 'life, ID: Identity>
    policy::Lifecycle<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Driver
{
    type Drain = Ready;

    fn transmit_failed(
        owner: &Owner<'life, 'e, 'd, ID>,
        conn: conn::Id<'d, ID>,
        failure: conn::Transmit,
        regions: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) -> Option<conn::Transmit> {
        let port = owner.root();
        let state = port.shared.get_mut(&mut regions.1);
        state.fail(conn, error::Error::Transmit(failure));
        port.shared.wake_all();
        Some(failure)
    }

    fn io_fail(
        owner: &mut Owner<'life, 'e, 'd, ID>,
        failure: Errno,
        context: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) {
        let port = owner.root();
        let conn = context.conn_id;
        let mut parts = context.parts();
        let state = port.shared.get_mut(parts.driver.state_token());
        state.fail(conn, error::Error::Transport(failure));
        port.shared.wake_all();
    }

    fn clear_send<'turn>(
        owner: &Owner<'life, 'e, 'd, ID>,
        conn: conn::Id<'d, ID>,
        context: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        let (work, region) = context.parts();
        owner.root().io.retire(conn, work, region, drop)
    }

    fn quarantine(
        owner: &Owner<'life, 'e, 'd, ID>,
        conn: conn::Id<'d, ID>,
        _: &mut Self::ConnState,
        reason: close::Reason,
        regions: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let port = owner.root();
        let state = port.shared.get_mut(&mut regions.1);
        state.retire(conn, reason);
        port.shared.wake_all();
    }

    fn disconnect(
        owner: &mut Owner<'life, 'e, 'd, ID>,
        mut context: turns::Retired<'_, 'd, Self::ConnState, ID, Owner<'life, 'e, 'd, ID>>,
    ) {
        let reason = context.reason();
        let conn = context.connection();
        let port = owner.root();
        let state = port.shared.get_mut(context.state_token());
        state.disconnect(conn, reason);
        port.shared.wake_all();
    }
}

impl<'life, 'e: 'life, 'd: 'life, ID: Identity>
    policy::Scheduling<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Driver
{
    fn progress(
        owner: &Owner<'life, 'e, 'd, ID>,
        _: &region::Token<region::Identity<'d>>,
        _: clock::Clock<'_, 'd>,
    ) -> Progress<'d> {
        owner.root().shared.progress()
    }

    fn park<'turn>(
        owner: &mut Owner<'life, 'e, 'd, ID>,
        work: Work<'turn, 'd>,
        regions: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let shared = &owner.root().shared;
        shared.get_mut(&mut regions.1);
        shared.flush_wake(work);
    }

    fn shutdown(owner: &mut Owner<'life, 'e, 'd, ID>) {
        let shared = &owner.root().shared;
        shared.wake_all();
    }
}
