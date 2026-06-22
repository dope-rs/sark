use std::marker::PhantomData;

use dope::Driver;
use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application};
use dope::transport::link::Slot;
use dope::transport::wire::{Identity, RecvChunk, Wire};

use crate::conn::{self, Conn, ConnError, Settings};
use crate::frame::{ErrorCode, ParseError};
use crate::role::ServerRole;

pub trait Handler: 'static {
    fn on_event(&mut self, event: conn::Event, conn: &mut Conn<ServerRole>);
}

pub struct ConnState {
    pub conn: Conn<ServerRole>,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            conn: Conn::<ServerRole>::with_local_settings(Settings {
                max_concurrent_streams: Some(256),
                ..Settings::DEFAULT
            }),
        }
    }
}

pub struct App<H: Handler, W: Wire = Identity> {
    user: H,
    _wire: PhantomData<W>,
}

impl<H: Handler, W: Wire> App<H, W> {
    pub fn new(user: H) -> Self {
        Self {
            user,
            _wire: PhantomData,
        }
    }

    pub fn handler(&self) -> &H {
        &self.user
    }

    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.user
    }
}

impl<H: Handler, W: Wire> Application for App<H, W> {
    type Conn = ConnState;
    type Wire = W;

    fn on_chunk(
        &mut self,
        slot: &mut Slot<W, listener::State<ConnState>>,
        chunk: RecvChunk<'_>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) -> Outcome {
        let bytes = chunk.as_slice();
        let state = &mut slot.state.conn;
        if state.conn.goaway_sent() || state.conn.goaway_received().is_some() {
            return Outcome::Ok;
        }
        if let Err(e) = state.conn.ingest(bytes) {
            let code = Self::map_error(&e);
            state.conn.goaway(code, b"");
            Self::flush_into(slot, aux, driver, true);
            return Outcome::Ok;
        }
        while let Some(ev) = state.conn.poll_event() {
            let release = ev.release_hint();
            self.user.on_event(ev, &mut state.conn);
            if let Some((stream_id, n)) = release {
                let _ = state.conn.release_capacity(stream_id, n);
            }
        }
        let close_after = state.conn.goaway_sent();
        Self::flush_into(slot, aux, driver, close_after);
        Outcome::Ok
    }

    fn on_send(
        &mut self,
        _slot: &mut Slot<W, listener::State<ConnState>>,
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &mut Driver,
    ) {
    }

    fn on_close(
        &mut self,
        _slot: &mut Slot<W, listener::State<ConnState>>,
        _aux: &mut listener::Aux,
    ) {
    }
}

impl<H: Handler, W: Wire> App<H, W> {
    fn flush_into(
        slot: &mut Slot<W, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
        close_after: bool,
    ) {
        let send_ud = slot.token();
        let write_buf = aux.write_buf_for(slot);
        let state = &mut slot.state.conn;
        let out = state.conn.outbound();
        let n = out.len().min(write_buf.len());
        write_buf[..n].copy_from_slice(&out[..n]);
        state.conn.drain_outbound(n);
        if close_after {
            slot.core.set_close_after();
        }
        slot.submit_buffered(write_buf, n, send_ud, driver);
    }

    fn map_error(e: &ConnError) -> ErrorCode {
        match e {
            ConnError::BadPreface
            | ConnError::Protocol
            | ConnError::BadStream
            | ConnError::Continuation
            | ConnError::BadSettings
            | ConnError::StreamGoneAway => ErrorCode::ProtocolError,
            ConnError::StreamClosed => ErrorCode::StreamClosed,
            ConnError::ParseError(ParseError::FrameSize)
            | ConnError::ParseError(ParseError::BadLength) => ErrorCode::FrameSize,
            ConnError::ParseError(_) => ErrorCode::ProtocolError,
            ConnError::FlowControl => ErrorCode::FlowControl,
            ConnError::FrameSize => ErrorCode::FrameSize,
            ConnError::Hpack(_) => ErrorCode::Compression,
            ConnError::GoAwayReceived(c) => *c,
            ConnError::StreamLimit => ErrorCode::RefusedStream,
            ConnError::HeaderListTooLarge | ConnError::Overload => ErrorCode::EnhanceYourCalm,
        }
    }
}
