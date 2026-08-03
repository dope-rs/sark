use dope::manifold::connector::state::IOV_CAP;
use dope::manifold::connector::{codec, lifecycle, session};
use dope_net::link::egress::queue::Queue;
use o3::buffer::{Bytes, Retained, Shared};
use o3::cell::RegionToken;

use crate::{
    conn::{self, Conn, ConnError},
    role::ClientRole,
};

pub trait Handler: 'static {
    fn event(&mut self, event: conn::Event, conn: &mut Conn<ClientRole>);
}

#[derive(Default)]
pub struct ConnState {
    pub conn: Conn<ClientRole>,
}

impl lifecycle::Lifecycle for ConnState {
    fn wants_close(&self) -> lifecycle::Close {
        use lifecycle::Close;
        if self.conn.goaway_received().is_some() || self.conn.goaway_sent() {
            Close::Reconnect
        } else {
            Close::Keep
        }
    }

    fn defer_close(&self) -> bool {
        false
    }

    fn is_drained(&self) -> bool {
        true
    }
}

#[derive(Default)]
pub struct State;

pub struct Head(pub Shared);

pub struct Codec;

impl codec::Codec for Codec {
    type Head = Head;
    type ParseState = State;

    fn parse(&self, _state: &mut State, buf: &Shared) -> Option<(Head, usize)> {
        let len = buf.as_slice().len();
        if len == 0 {
            return None;
        }
        Some((Head(buf.clone()), len))
    }
}

pub struct Session<H: Handler> {
    codec: Codec,
    handler: H,
}

impl<H: Handler> Session<H> {
    pub fn new(handler: H) -> Self {
        Self {
            codec: Codec,
            handler,
        }
    }

    pub fn handler(&self) -> &H {
        &self.handler
    }

    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.handler
    }

    pub fn connect<'d>(
        &mut self,
        state: &mut ConnState,
        sink: &mut Queue<'_, 'd, '_, { IOV_CAP }>,
        region: &mut RegionToken<'d>,
    ) {
        Self::drain_into(&mut state.conn, sink, region);
    }

    pub fn response<'d>(
        &mut self,
        head: Head,
        state: &mut ConnState,
        sink: &mut Queue<'_, 'd, '_, { IOV_CAP }>,
        region: &mut RegionToken<'d>,
    ) {
        let Head(buf) = head;
        let conn = &mut state.conn;
        let mut result = conn.ingest_retained(Bytes::<Retained>::from(buf));
        loop {
            let mut drained = false;
            while let Some(ev) = conn.poll_event() {
                drained = true;
                self.handler.event(ev, conn);
            }
            match result {
                Ok(()) => break,
                Err(ConnError::Overload) if drained => result = conn.resume(),
                Err(_) => return,
            }
        }
        Self::drain_into(conn, sink, region);
    }
}

impl<'d, H: Handler> session::Session<'d> for Session<H> {
    type Codec = Codec;
    type ConnState = ConnState;
    type Send = Shared;

    fn codec(&self) -> &Codec {
        &self.codec
    }

    fn connect(&mut self, ctx: &mut session::Ctx<'_, '_, 'd, Self>) {
        self.connect(ctx.state, &mut ctx.sink, ctx.region);
    }

    fn response(&mut self, head: Head, ctx: &mut session::Ctx<'_, '_, 'd, Self>) {
        self.response(head, ctx.state, &mut ctx.sink, ctx.region);
    }

    fn disconnect(&mut self, _ctx: &mut session::Ctx<'_, '_, 'd, Self>) {}
}

impl<H: Handler> Session<H> {
    fn drain_into<'d>(
        conn: &mut Conn<ClientRole>,
        sink: &mut Queue<'_, 'd, '_, { IOV_CAP }>,
        region: &mut RegionToken<'d>,
    ) {
        let out = conn.outbound();
        if out.is_empty() {
            return;
        }
        let len = out.len();
        if sink
            .try_enqueue(region, Shared::copy_from_slice(out))
            .is_ok()
        {
            conn.drain_outbound(len);
        }
    }
}
