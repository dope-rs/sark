use dope::manifold::connector;
use o3::buffer::Shared;

use crate::conn::{self, Conn};
use crate::role::ClientRole;

pub trait Handler: 'static {
    fn on_event(&mut self, event: conn::Event, conn: &mut Conn<ClientRole>);
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
}

impl<H: Handler> connector::Session for Session<H> {
    type Codec = Codec;
    type ConnState = ConnState;

    fn codec(&self) -> &Codec {
        &self.codec
    }

    fn connect(&mut self, ctx: &mut connector::Ctx<'_, Self>) {
        Self::drain_into(&mut ctx.state.conn, ctx.sink);
    }

    fn response(&mut self, head: Head, ctx: &mut connector::Ctx<'_, Self>) {
        let Head(buf) = head;
        let conn = &mut ctx.state.conn;
        if conn.ingest(buf.as_slice()).is_err() {
            return;
        }
        while let Some(ev) = conn.poll_event() {
            let release = ev.release_hint();
            self.handler.on_event(ev, conn);
            if let Some((stream_id, n)) = release {
                let _ = conn.release_capacity(stream_id, n);
            }
        }
        Self::drain_into(conn, ctx.sink);
    }

    fn disconnect(&mut self, _ctx: &mut connector::Ctx<'_, Self>) {}
}

impl<H: Handler> Session<H> {
    fn drain_into(
        conn: &mut Conn<ClientRole>,
        sink: &mut connector::session::Queue<{ connector::session::IOV_CAP }>,
    ) {
        let out = conn.outbound();
        if out.is_empty() {
            return;
        }
        let owned: Vec<u8> = out.to_vec();
        let len = owned.len();
        sink.push(Shared::from(owned));
        conn.drain_outbound(len);
    }
}
