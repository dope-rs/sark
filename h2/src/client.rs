use dope::manifold::connector;
use o3::buffer::Shared;

use crate::conn::{self, Conn, ConnError};
use crate::role::ClientRole;

pub trait Handler: 'static {
    fn event(&mut self, event: conn::Event, conn: &mut Conn<ClientRole>);
}

#[derive(Default)]
pub struct ConnState {
    pub conn: Conn<ClientRole>,
}

impl connector::Lifecycle for ConnState {
    fn wants_close(&self) -> connector::Close {
        if self.conn.goaway_received().is_some() || self.conn.goaway_sent() {
            connector::Close::Reconnect
        } else {
            connector::Close::Keep
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

impl connector::Codec for Codec {
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

    pub fn connect(
        &mut self,
        state: &mut ConnState,
        sink: &mut connector::state::Queue<{ connector::state::IOV_CAP }>,
    ) {
        Self::drain_into(&mut state.conn, sink);
    }

    pub fn response(
        &mut self,
        head: Head,
        state: &mut ConnState,
        sink: &mut connector::state::Queue<{ connector::state::IOV_CAP }>,
    ) {
        let Head(buf) = head;
        let conn = &mut state.conn;
        let mut result = conn.ingest(buf.as_slice());
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
        Self::drain_into(conn, sink);
    }
}

impl<'d, H: Handler> connector::Session<'d> for Session<H> {
    type Codec = Codec;
    type ConnState = ConnState;
    type Send = o3::buffer::Shared;

    fn codec(&self) -> &Codec {
        &self.codec
    }

    fn connect(&mut self, ctx: &mut connector::Ctx<'_, 'd, Self>) {
        self.connect(ctx.state, ctx.sink);
    }

    fn response(&mut self, head: Head, ctx: &mut connector::Ctx<'_, 'd, Self>) {
        self.response(head, ctx.state, ctx.sink);
    }

    fn disconnect(&mut self, _ctx: &mut connector::Ctx<'_, 'd, Self>) {}
}

impl<H: Handler> Session<H> {
    fn drain_into(
        conn: &mut Conn<ClientRole>,
        sink: &mut connector::state::Queue<{ connector::state::IOV_CAP }>,
    ) {
        let out = conn.outbound();
        if out.is_empty() {
            return;
        }
        let len = out.len();
        if sink.try_enqueue(Shared::copy_from_slice(out)).is_ok() {
            conn.drain_outbound(len);
        }
    }
}
