use dope::{
    core::driver::{route::Identity, schedule::clock::timer::Timer},
    manifold::network::listener::conn,
    net::wire::{open, terminal},
};
use dope_tls::tls::Tls;
use o3::buffer::resident;
use sark_core::duplex;
use sark_h1::server::listener::failure::Policy;
use sark_http::payload::{owned, response::status::Code};
use sark_ws::{frame, server as ws};

use crate::common::{retained, server};

pub struct State<'a> {
    pub counters: &'a Duplex,
    pub finish_output_on_input: bool,
    pub hold_output_on_finish: bool,
}

pub struct Echo<'state> {
    counters: &'state Duplex,
    finish_output_on_input: bool,
    hold_output_on_finish: bool,
    output_finished: bool,
}

impl<'d> duplex::Session<'d> for Echo<'_> {
    fn receive(
        &mut self,
        input: owned::Segments<'d>,
        _: &mut [u8],
        _: &mut Vec<u8>,
    ) -> (usize, duplex::Action<'d>) {
        if self.output_finished || self.counters.input_finished.get() != 0 {
            self.counters.receive(input.len());
            return (0, duplex::Action::idle());
        }
        if self.finish_output_on_input {
            self.output_finished = true;
            return (0, duplex::Action::send_end(input));
        }
        (0, duplex::Action::send(input))
    }

    fn finish(&mut self, _: &mut [u8], _: &mut Vec<u8>) -> (usize, duplex::Action<'d>) {
        self.counters.finish();
        let action = if self.hold_output_on_finish {
            duplex::Action::send(resident::Shared::from(b"finished".as_slice()))
        } else if self.output_finished {
            duplex::Action::close()
        } else {
            duplex::Action::send_end(resident::Shared::from(b"finished".as_slice()))
        };
        (0, action)
    }

    fn activate(&mut self, output: &mut [u8], _: &mut Vec<u8>) -> (usize, duplex::Action<'d>) {
        self.counters.activate();
        let written = output.len().min(b"ready".len());
        output[..written].copy_from_slice(&b"ready"[..written]);
        (
            written,
            duplex::Action::send(resident::Shared::from(&b"ready"[written..])),
        )
    }
}

#[sark_fiber::request]
pub struct Connect {}

#[sark_fiber::duplex(pub)]
fn echo<'state>(_: Connect, state: &'state State<'_>) -> Echo<'state> {
    Echo {
        counters: state.counters,
        finish_output_on_input: state.finish_output_on_input,
        hold_output_on_finish: state.hold_output_on_finish,
        output_finished: false,
    }
}

pub struct Websocket<'state>(&'state Duplex);

impl ws::app::Sink for Websocket<'_> {
    fn opening(&self, request: ws::Opening<'_>) -> Result<(), ws::Error> {
        self.0.activate();
        assert_eq!(request.authority, "x");
        assert_eq!(request.target, "/ws");
        assert_eq!(request.cookie, Some(b"session=good".as_slice()));
        if request.origin.is_some() {
            Err(ws::Error::Forbidden)
        } else {
            Ok(())
        }
    }

    fn capacity(&self, payload: usize) -> Result<usize, ws::Error> {
        Ok(frame::Plan::new(0x1, true, payload, false)?.wire_len())
    }

    fn message(
        &self,
        message: ws::Message<'_>,
        out: &mut ws::Response<'_>,
    ) -> Result<ControlFlow<()>, ws::Error> {
        self.0.receive(1);
        match message {
            ws::Message::Text(text) => out.try_text(text)?,
            ws::Message::Binary(bytes) => out.try_binary(bytes)?,
        }
        Ok(ControlFlow::Continue(()))
    }

    fn failed(&self, error: ws::Error) {
        self.0.websocket_failures.push(error);
    }
}

impl Drop for Websocket<'_> {
    fn drop(&mut self) {
        self.0.closed.handle().hit();
    }
}

#[sark_fiber::duplex(pub)]
fn websocket<'state>(_: Connect, state: &'state State<'_>) -> ws::Session<Websocket<'state>> {
    ws::Session::new(Websocket(state.counters), 16 * 1024, 16 * 1024)
}

pub const WEBSOCKET_OPENING: &[u8] = b"GET /ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nCookie: session=good\r\n\r\n";
pub const WEBSOCKET_FRAMES: &[u8] = b"\x81\x84\0\0\0\0echo\x88\x80\0\0\0\0";
pub const WEBSOCKET_REPLY: &[u8] = b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n\x81\x04echo\x88\x00";

#[sark_fiber::handler(pub)]
async fn wait<'state>(state: &State<'state>, timer: Timer<'_>) -> super::OwnedReply {
    retained::State(&state.counters.timer_failures)
        .delay(timer, Duration::from_millis(50))
        .await;
    super::OwnedReply {
        status: Code::OK,
        body: b"waited".to_vec(),
    }
}

sark_fiber::define_cleartext_route! {
    pub ConnectDispatch<'state>: State<'state> => {
        CONNECT "*" => echo,
        WS "/ws" => websocket,
        GET "/wait" => async(capacity = 1, body = discarded) wait,
        GET "/stream" => stream(capacity = 1) super::stream::pending,
        GET "/empty" => async stream(capacity = 1, body = discarded) super::stream::empty,
    }
}

sark_fiber::define_route! {
    pub WebsocketOnly<'state>: State<'state> => {
        WS "/ws" => websocket,
    }
}

sark_fiber::define_route! {
    pub WebsocketFirst<'state>: State<'state> => {
        WS "/ws" => websocket,
        CONNECT "*" => websocket,
    }
}

sark_fiber::define_route! {
    pub ConnectFirst<'state>: State<'state> => {
        CONNECT "*" => websocket,
        WS "/ws" => websocket,
    }
}

use std::{cell::Cell, ops::ControlFlow, time::Duration};

use dope_test::harness::{
    observe::VecCellExt as _,
    peer::{Error, Stream},
    signals::{LocalGate, Progress as _},
};

#[derive(Default)]
pub struct Duplex {
    capability: Cell<usize>,
    input_finished: Cell<usize>,
    after_finish: Cell<usize>,
    pub closed: LocalGate,
    pub failures: Cell<Vec<server::Failure>>,
    pub timer_failures: Cell<Vec<retained::Failure>>,
    pub diagnostics: Cell<Vec<String>>,
    websocket_failures: Cell<Vec<ws::Error>>,
}

#[derive(Debug)]
pub struct Snapshot {
    pub capability: usize,
    pub input_finished: usize,
    pub after_finish: usize,
    pub dropped: u32,
    pub diagnostics: Vec<String>,
    pub websocket_failures: Vec<ws::Error>,
}

impl Duplex {
    pub fn activate(&self) {
        self.capability.set(self.capability.get() + 1);
    }

    pub fn finish(&self) {
        self.input_finished.set(self.input_finished.get() + 1);
    }

    pub fn receive(&self, bytes: usize) {
        self.after_finish.set(self.after_finish.get() + bytes);
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            capability: self.capability.get(),
            input_finished: self.input_finished.get(),
            after_finish: self.after_finish.get(),
            dropped: self.closed.handle().hits(),
            diagnostics: self.diagnostics.take(),
            websocket_failures: self.websocket_failures.take(),
        }
    }
}

impl Drop for Echo<'_> {
    fn drop(&mut self) {
        self.counters.closed.handle().hit();
    }
}

impl Policy<Tls> for State<'_> {
    fn open_failed<'w>(&self, cause: open::Error<'w, Tls>)
    where
        Tls: 'w,
    {
        Policy::open_failed(&server::State(&self.counters.failures), cause);
    }

    fn wire_failed<'w, 'd: 'w, ID: Identity>(
        &self,
        id: conn::Id<'d, ID>,
        cause: terminal::Error<'w, Tls>,
    ) {
        Policy::wire_failed(&server::State(&self.counters.failures), id, cause);
    }
}

pub trait Receive {
    fn read_until(&self, buffered: &mut Vec<u8>, expected: &[u8]) -> Result<(), Error>;
}

impl Receive for Stream<'_> {
    fn read_until(&self, buffered: &mut Vec<u8>, expected: &[u8]) -> Result<(), Error> {
        let mut incoming = [0_u8; 4096];
        while !buffered
            .windows(expected.len())
            .any(|window| window == expected)
        {
            let received = self.read(&mut incoming)?;
            if received == 0 {
                return Err(Error::UnexpectedEof);
            }
            buffered.extend_from_slice(&incoming[..received]);
        }
        Ok(())
    }
}
