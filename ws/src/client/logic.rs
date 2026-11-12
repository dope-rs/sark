use codec::storage;
use core::convert::Infallible;
use std::{marker, mem};

use bindings::endpoint::State;
use dope::{
    core::{
        Backend, Errno,
        dope_sys::thread::Thread as _,
        driver::{
            self,
            route::{Identity, KeyTag},
            schedule::app::Work,
        },
        io::socket,
    },
    manifold::{
        app::dispatch::identity,
        client::{
            app::{self, admit::Registered, connect::Full, failure::Observed, request},
            attempt, close, conn, port,
            session::{
                self,
                app::drain::Ready,
                codec::{self, mode, staging},
                policy,
                protocol::{Spec, Stored},
                response, turns,
            },
        },
        service::bindings,
    },
    net::{
        link::{egress, pool::change::opening},
        wire,
    },
};
use o3::{
    alloc::StrExt as _,
    base::{Call, cell::region},
    buffer::{self, hold, resident, storage::Shared},
};
use policy::{admit, output};
use port::error;

use crate::{
    client::{
        self,
        channel::{Connection, Port},
        headers,
        mode::{Local, event::Event},
        protocol, send,
    },
    inbound,
};

const MAX_CONNECTIONS: usize = u32::MAX as usize;

type Owner<'life, 'e, 'd, H, M, ID> =
    State<'life, 'd, Port<'e, 'd, ID>, Logic<H, M, ID>, ID, MAX_CONNECTIONS>;

#[doc(hidden)]
pub struct Logic<H, M = Local, ID: Identity = KeyTag<0>> {
    handler: H,
    mode: marker::PhantomData<(M, ID)>,
}

impl<H, M, ID: Identity> Logic<H, M, ID> {
    pub const fn new(inner: H) -> Self {
        Self {
            handler: inner,
            mode: marker::PhantomData,
        }
    }

    fn close<'d>(
        parts: &mut turns::Parts<'_, '_, 'd, protocol::Connection, Shared, ID>,
        reason: client::CloseReason,
    ) {
        let rule = parts.state.close(reason);
        parts.request(rule);
    }

    /// Delivers accepted ownership; materialization failures terminate without replay.
    fn respond<'d>(
        &mut self,
        port: &Port<'_, 'd, ID>,
        head: Result<protocol::Head<'d>, client::Fault>,
        parser: &mut protocol::State,
        sent: Result<(), client::Error>,
        ctx: &mut turns::Context<'_, '_, 'd, protocol::Connection, Shared, ID>,
    ) where
        M: Event<'d, H, ID>,
    {
        if let Err(error) = sent {
            Self::close(&mut ctx.parts(), client::CloseReason::Send(error));
            return;
        }
        let head = match head {
            Ok(ref head) => head,
            Err(error) => {
                parser.phase = protocol::Phase::Closed;
                Self::close(&mut ctx.parts(), client::CloseReason::Decode(error));
                return;
            }
        };
        let mut parts = ctx.parts();
        let conn = parts.conn_id;
        let message = match &head.0 {
            protocol::Kind::Opened { accept } => {
                match parts.state.open(*accept) {
                    protocol::Handshake::Opened => {
                        match port.shared.conn_id.bind(conn, Connection::Open) {
                            identity::Bind::Bound => {}
                            identity::Bind::Occupied => {
                                Self::close(
                                    &mut parts,
                                    client::CloseReason::Local(close::LocalOrigin::Superseded),
                                );
                                return;
                            }
                        }
                    }
                    protocol::Handshake::Rejected => {
                        Self::close(&mut parts, client::CloseReason::Protocol);
                        return;
                    }
                }
                port.shared.wake_waiter();
                port.shared.wake();
                M::open(&mut self.handler, port, &mut parts);
                return;
            }
            protocol::Kind::Text(text) => client::Message::Text(text.as_str()),
            protocol::Kind::AssembledText(text) => client::Message::Text(text.as_str()),
            protocol::Kind::Binary(bytes) => client::Message::Binary(bytes.as_slice()),
            protocol::Kind::AssembledBinary(bytes) => client::Message::Binary(bytes.as_slice()),
            protocol::Kind::Pong(bytes) => client::Message::Pong(bytes.as_slice()),
            protocol::Kind::Ping(bytes) => {
                M::message(
                    &mut self.handler,
                    client::Message::Ping(bytes.as_slice()),
                    &mut parts,
                );
                return;
            }
            protocol::Kind::Continuation => return,
            protocol::Kind::Close(close) => {
                parser.phase = protocol::Phase::Closed;
                port.shared.conn_id.clear(conn);
                port.shared.wake();
                let reason = match close.reason().try_to_owned() {
                    Ok(reason) => client::CloseReason::RemoteClose {
                        code: close.code(),
                        reason,
                    },
                    Err(error) => client::CloseReason::Allocation(error),
                };
                Self::close(&mut parts, reason);
                return;
            }
        };
        M::message(&mut self.handler, message, &mut parts);
    }

    fn prepare<'d>(
        port: &Port<'_, 'd, ID>,
        preview: Result<Option<inbound::Reply<'_>>, &client::Fault>,
        active: bool,
        ctx: &mut turns::Context<'_, '_, 'd, protocol::Connection, Shared, ID>,
    ) -> Result<(), client::Error> {
        let bytes;
        let reply = match preview {
            Ok(Some(reply)) => reply,
            Err(error) if active => {
                bytes = (error.close_code() as u16).to_be_bytes();
                inbound::Reply::owed(0x8, &bytes)
            }
            Ok(None) | Err(_) => return Ok(()),
        };
        let mut parts = ctx.parts();
        let conn = parts.conn_id;
        if reply.opcode() == 0x8 && port.shared.is_closing(conn) {
            return Ok(());
        }
        send::Sender::new(
            conn,
            parts.sink.reborrow(),
            port,
            parts.driver.region_token(),
        )
        .reply(reply)
    }

    fn handshake<'d>(
        &mut self,
        port: &Port<'_, 'd, ID>,
        state: &mut protocol::Connection,
        region: &mut region::Token<region::Identity<'d>>,
    ) -> Result<headers::Encoded, client::Error>
    where
        M: Event<'d, H, ID>,
    {
        let mut bytes = mem::MaybeUninit::uninit();
        let nonce = Backend::entropy::<16>(&mut bytes).map_err(client::Error::Entropy)?;
        let key = crate::upgrade::Key::from_nonce(*nonce);
        state.begin(key.accept());
        let req = headers::Request::new(&port.codec.config, key.as_bytes());
        M::handshake(&mut self.handler, req, region)
    }
}

impl<'life, 'e: 'life, 'd: 'e, ID: Identity, H, M>
    Spec<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Logic<H, M, ID>
{
    type Codec = protocol::Codec;
    type Connect = Full;
    type ConnState = protocol::Connection;
    type Failure = Observed;
    type Mode = mode::Stored;
    type Receipt = ();
    type Requests = request::Enabled;
    type Response = response::Immediate;
    type Send = Shared;

    fn codec<'a>(owner: &'a Owner<'life, 'e, 'd, H, M, ID>) -> &'a Self::Codec {
        &owner.root().codec
    }
}

impl<'life, 'e: 'life, 'd: 'e, ID: Identity, H, M: Event<'d, H, ID>>
    Stored<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Logic<H, M, ID>
{
    fn blocked(
        _: &Owner<'life, 'e, 'd, H, M, ID>,
        _: conn::Id<'d, ID>,
        _: &mut protocol::State,
    ) -> session::Blocking {
        session::Blocking::Unsupported
    }

    fn decode<S>(
        owner: &mut Owner<'life, 'e, 'd, H, M, ID>,
        _: conn::Id<'d, ID>,
        storage: &mut protocol::Storage<S>,
        parser: &mut <Self::Codec as codec::Stateful>::State<'d>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) -> codec::Step<response::Direct>
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        use codec::Stateful as _;
        let port = owner.root();
        let logic = owner.logic_mut();
        let active = matches!(parser.phase, protocol::Phase::Active(_));
        port.codec
            .decode(storage, parser, budget, limit, |preview| {
                let sent = Self::prepare(port, preview, active, ctx);
                if matches!(&sent, Err(client::Error::Admission(_))) {
                    return codec::Admission::RetryInput;
                }
                codec::Admission::Ready(move |head, parser: &mut protocol::State| {
                    logic.respond(port, head, parser, sent, ctx)
                })
            })
    }

    fn finish<'slot, 'id, S>(
        owner: &mut Owner<'life, 'e, 'd, H, M, ID>,
        mut input: storage::Owner<'slot, 'id, protocol::Storage<S>, protocol::State>,
        budget: &resident::Budget<'d>,
        limit: staging::Limit,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) -> (
        storage::Owner<'slot, 'id, protocol::Storage<S>, protocol::State>,
        Result<codec::Finish<response::Direct>, response::Direct>,
    )
    where
        S: buffer::Cursor + hold::raw::Retain<'d> + buffer::raw::Consume,
    {
        use codec::Stateful as _;
        let port = owner.root();
        let logic = owner.logic_mut();
        let (_, parser) = input.parts();
        let active = matches!(parser.phase, protocol::Phase::Active(_));
        port.codec.finish(input, budget, limit, |preview| {
            let sent = Self::prepare(port, preview, active, ctx);
            if matches!(&sent, Err(client::Error::Admission(_))) {
                return codec::Admission::RetryInput;
            }
            codec::Admission::Ready(move |head, parser: &mut protocol::State| {
                logic.respond(port, head, parser, sent, ctx)
            })
        })
    }
}

impl<'life, 'e: 'life, 'd: 'e, ID: Identity, H, M>
    admit::Register<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Logic<H, M, ID>
{
    fn register(
        owner: &Owner<'life, 'e, 'd, H, M, ID>,
        entry: &conn::Activation<'_, 'd, ID>,
    ) -> app::Admission {
        match owner.root().io.try_activate(entry) {
            Ok(()) => app::Admission::Attached,
            Err(error::Open::Unknown | error::Open::Exhausted) => app::Admission::Rejected(
                close::Directive::cut_final(close::Reason::Capacity(close::Capacity::Transport)),
            ),
            Err(error::Open::Occupied) => app::Admission::Rejected(close::Directive::cut_final(
                close::Reason::Local(close::LocalOrigin::ResourceFailure),
            )),
        }
    }
}

impl<'life, 'e: 'life, 'd: 'e, ID: Identity, H, M: Event<'d, H, ID>>
    policy::Connected<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Logic<H, M, ID>
{
    type Activation = Registered;

    fn connect(
        owner: &mut Owner<'life, 'e, 'd, H, M, ID>,
        _: attempt::Completion<'_, 'd, ID>,
        _: socket::Addr,
        mut ctx: turns::Startup<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) {
        let port = owner.root();
        let logic = owner.logic_mut();
        let req = match logic.handshake(port, ctx.state, ctx.driver.region_token()) {
            Ok(req) => req,
            Err(error) => {
                ctx.close
                    .request(ctx.state.close(client::CloseReason::Handshake(error)));
                return;
            }
        };
        match ctx
            .sink
            .try_enqueue(ctx.driver.region_token(), req.into_shared())
        {
            egress::Enqueue::Committed => {}
            egress::Enqueue::Rejected(rejected) => {
                ctx.close.request(
                    ctx.state
                        .close(client::CloseReason::Egress(*rejected.error())),
                );
            }
        }
    }
}

impl<'life, 'e: 'life, 'd: 'e, ID: Identity, H, M: Event<'d, H, ID>>
    policy::Lifecycle<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Logic<H, M, ID>
{
    type Drain = Ready;

    fn io_fail(
        _: &mut Owner<'life, 'e, 'd, H, M, ID>,
        error: Errno,
        ctx: &mut turns::Context<'_, '_, 'd, Self::ConnState, Self::Send, ID>,
    ) {
        Self::close(&mut ctx.parts(), client::CloseReason::Io(error));
    }

    fn clear_send<'turn>(
        owner: &Owner<'life, 'e, 'd, H, M, ID>,
        conn: conn::Id<'d, ID>,
        ctx: &mut turns::Retirement<'_, 'turn, 'd>,
    ) -> egress::Cleared {
        let (work, region) = ctx.parts();
        owner.root().io.retire(conn, work, region, mem::drop)
    }

    fn quarantine(
        owner: &Owner<'life, 'e, 'd, H, M, ID>,
        conn: conn::Id<'d, ID>,
        _: &mut Self::ConnState,
        _: close::Reason,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        let shared = &owner.root().shared;
        shared.conn_id.clear(conn);
        shared.wake_waiter();
        shared.wake();
    }

    fn disconnect(
        owner: &mut Owner<'life, 'e, 'd, H, M, ID>,
        mut ctx: turns::Retired<'_, 'd, Self::ConnState, ID, Owner<'life, 'e, 'd, H, M, ID>>,
    ) {
        let reason = ctx.reason();
        let transmit = ctx.take_transmit();
        let (link, state, region) = ctx.into_parts();
        let conn = link.connection();
        let port = owner.root();
        let logic = owner.logic_mut();
        port.shared.conn_id.clear(conn);
        port.shared.wake_waiter();
        port.shared.wake();
        let closed = client::Closed {
            reason: state.take_reason(reason),
            transmit,
        };
        M::close(&mut logic.handler, closed, region);
    }
}

impl<'life, 'e: 'life, 'd: 'e, ID: Identity + 'life, H, M>
    policy::Output<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Logic<H, M, ID>
{
    fn flush<'w, W: wire::Spec + 'w>(
        _: &W::Connection<'w, ID>,
    ) -> Option<
        impl for<'a, 'x, 'y, 'z> Call<(
            &'a mut Owner<'life, 'e, 'd, H, M, ID>,
            &'x mut turns::Context<'y, 'z, 'd, Self::ConnState, Self::Send, ID>,
            session::Input,
        )>,
    > {
        Some(
            |(owner, _, _): output::BoundArgs<
                '_, '_, '_, '_, 'life, 'd, Port<'e, 'd, ID>, Self, ID, MAX_CONNECTIONS,
            >| owner.root().shared.wake_waiter(),
        )
    }

    fn sent() -> impl for<'a, 'x> Call<(
        &'a mut Owner<'life, 'e, 'd, H, M, ID>,
        conn::Id<'d, ID>,
        egress::Pending,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Sent::<_>::Ready(|owner: &mut Owner<'life, 'e, 'd, H, M, ID>| {
            owner.root().shared.wake_waiter();
        })
    }

    fn cancel() -> impl for<'a, 'x> Call<(
        &'a Owner<'life, 'e, 'd, H, M, ID>,
        Self::Send,
        &'x mut region::Token<region::Identity<'d, 1>>,
    )> {
        output::Cancel::<Infallible, Infallible, Infallible, _, _>::Ready(
            mem::drop,
            |owner: &Owner<'life, 'e, 'd, H, M, ID>| owner.root().shared.wake_waiter(),
        )
    }
}

impl<'life, 'e: 'life, 'd: 'e, ID: Identity, H, M>
    policy::Requests<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Logic<H, M, ID>
{
    fn drain(
        owner: &Owner<'life, 'e, 'd, H, M, ID>,
        reqs: &mut request::Requests<'_, '_, '_, 'd, Self::Send, ID>,
        _: &mut region::Token<region::Identity<'d, 1>>,
    ) {
        let port = owner.root();
        if port.io.drain_ready(reqs).is_some_and(|sender| {
            port.shared
                .send_plan
                .get()
                .is_some_and(|plan| sender.can_admit(plan))
        }) {
            port.shared.wake_waiter();
        }
    }
}

impl<'life, 'e: 'life, 'd: 'e, ID: Identity, H, M>
    policy::Scheduling<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Logic<H, M, ID>
{
    fn park<'turn>(
        owner: &mut Owner<'life, 'e, 'd, H, M, ID>,
        work: Work<'turn, 'd>,
        _: &mut (
            region::Token<region::Identity<'d>>,
            region::Token<region::Identity<'d, 1>>,
        ),
    ) {
        owner.root().shared.flush_wake(work);
    }

    fn shutdown(owner: &mut Owner<'life, 'e, 'd, H, M, ID>) {
        owner.root().shared.wake_waiter();
        owner.root().shared.wake();
    }
}

impl<'life, 'e: 'life, 'd: 'e, ID: Identity, H, M>
    policy::Failed<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Logic<H, M, ID>
{
    fn failed(
        owner: &mut Owner<'life, 'e, 'd, H, M, ID>,
        _: attempt::Completion<'_, 'd, ID>,
        _: attempt::ConnectFault,
        _: &mut driver::Context<'_, 'd>,
    ) {
        owner.root().shared.wake();
    }
}

impl<'life, 'e: 'life, 'd: 'e, ID: Identity, H, M>
    policy::Exhausted<'life, 'd, Port<'e, 'd, ID>, ID, MAX_CONNECTIONS> for Logic<H, M, ID>
{
    fn exhaust(
        owner: &mut Owner<'life, 'e, 'd, H, M, ID>,
        _: attempt::Completion<'_, 'd, ID>,
        cause: opening::Exhaustion,
        _: &mut driver::Context<'_, 'd>,
    ) {
        owner.root().shared.exhaust(cause);
    }
}
