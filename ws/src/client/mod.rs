use std::cell::Cell;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

mod handshake;
mod masking;

use masking::MaskSequence;

use dope::driver::token::Token;
use dope::manifold::connector;
use dope::manifold::connector::Connector;
use dope::manifold::connector::source::Dialer;
use dope::manifold::connector::state::{IOV_CAP, Queue};
use dope::manifold::env::Env;
use dope_fiber::WaitQueue;
use dope_fiber::{Fiber, poll_fn};
use dope_net::Transport;
use o3::buffer::Shared;

const DEFAULT_MAX_MESSAGE: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_OUTBOUND_FRAME: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    NotConnected,
    Backpressure,
    WaiterCapacity,
    InvalidHeader,
    MessageTooLarge,
}
#[derive(Debug)]
pub enum Message {
    Text(Shared),
    Binary(Shared),
    Ping(Shared),
    Pong(Shared),
}

#[derive(Debug)]
pub enum Head {
    HandshakeOk { accept: [u8; 28] },
    HandshakeFailed,
    Frame(Message),
    Continuation,
    Close(Shared),
}

pub trait Handler {
    fn handshake_headers(&mut self, _headers: &mut Vec<(String, String)>) -> Result<(), Error> {
        Ok(())
    }

    fn open(&mut self, _conn_id: Token) {}
    fn open_send(&mut self, conn_id: Token, _send: &mut SendCtx<'_>) {
        self.open(conn_id);
    }
    fn message(&mut self, _conn_id: Token, _msg: Message) {}
    fn close(&mut self, _conn_id: Token) {}
}

pub struct SendCtx<'a> {
    sink: &'a mut Queue<IOV_CAP>,
    rng: &'a MaskSequence,
    max_frame_payload: usize,
}

impl SendCtx<'_> {
    pub fn text(&mut self, payload: &[u8]) -> Result<(), Error> {
        self.message(0x1, payload)
    }

    pub fn binary(&mut self, payload: &[u8]) -> Result<(), Error> {
        self.message(0x2, payload)
    }

    pub fn ping(&mut self, payload: &[u8]) -> Result<(), Error> {
        self.control(0x9, payload)
    }

    pub fn pong(&mut self, payload: &[u8]) -> Result<(), Error> {
        self.control(0xA, payload)
    }

    pub fn close(&mut self, payload: &[u8]) -> Result<(), Error> {
        self.control(0x8, payload)
    }

    fn message(&mut self, opcode: u8, payload: &[u8]) -> Result<(), Error> {
        FrameEncoder::new(self.rng).enqueue(
            self.sink,
            opcode,
            payload,
            self.max_frame_payload.max(1),
            false,
        )
    }

    fn control(&mut self, opcode: u8, payload: &[u8]) -> Result<(), Error> {
        if payload.len() > 125 {
            return Err(Error::MessageTooLarge);
        }
        FrameEncoder::new(self.rng).enqueue(self.sink, opcode, payload, payload.len().max(1), true)
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub path: String,
    pub user_agent: String,
    pub headers: Vec<(String, String)>,
    pub max_frame_payload: usize,
    pub max_message_size: usize,
    pub max_outbound_frame_payload: usize,
}

impl Config {
    pub fn new(host: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            path: path.into(),
            user_agent: "sark-ws/0.1".into(),
            headers: Vec::new(),
            max_frame_payload: DEFAULT_MAX_MESSAGE,
            max_message_size: DEFAULT_MAX_MESSAGE,
            max_outbound_frame_payload: DEFAULT_MAX_OUTBOUND_FRAME,
        }
    }

    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Result<Self, Error> {
        let user_agent = user_agent.into();
        if !handshake::header_value(&user_agent) {
            return Err(Error::InvalidHeader);
        }
        self.user_agent = user_agent;
        Ok(self)
    }

    pub fn with_header(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, Error> {
        let name = name.into();
        let value = value.into();
        if !handshake::header_name(&name) || !handshake::header_value(&value) {
            return Err(Error::InvalidHeader);
        }
        self.headers.push((name, value));
        Ok(self)
    }

    pub fn max_message_size(mut self, max_message_size: usize) -> Self {
        self.max_message_size = max_message_size.max(1);
        self
    }

    pub fn max_frame_payload(mut self, max_frame_payload: usize) -> Self {
        self.max_frame_payload = max_frame_payload.max(1);
        self
    }

    pub fn max_outbound_frame_payload(mut self, max_outbound_frame_payload: usize) -> Self {
        self.max_outbound_frame_payload = max_outbound_frame_payload.max(1);
        self
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
enum ClientPhase {
    #[default]
    Connecting,
    Active,
    Closed,
}

pub struct State {
    phase: ClientPhase,
    fragments: crate::fragment::FragmentBuffer,
}

impl Default for State {
    fn default() -> Self {
        Self {
            phase: ClientPhase::Connecting,
            fragments: crate::fragment::FragmentBuffer::new(DEFAULT_MAX_MESSAGE),
        }
    }
}

#[derive(Default)]
pub struct ConnState {
    expected_accept: [u8; 28],
    closing: bool,
}

impl connector::Lifecycle for ConnState {
    fn wants_close(&self) -> connector::Close {
        if self.closing {
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

pub struct Codec {
    config: Config,
}

impl Codec {
    fn handshake_request(&self, key_b64: &[u8; 24], headers: &[(String, String)]) -> Vec<u8> {
        handshake::request(
            &self.config.host,
            &self.config.path,
            &self.config.user_agent,
            key_b64,
            headers,
        )
    }
}

impl connector::Codec for Codec {
    type Head = Head;
    type ParseState = State;

    fn parse(&self, state: &mut State, buf: &Shared) -> Option<(Head, usize)> {
        match state.phase {
            ClientPhase::Connecting => Self::parse_handshake_response(buf, state),
            ClientPhase::Active => self.parse_active_frame(buf, state),
            ClientPhase::Closed => None,
        }
    }
}

impl Codec {
    fn parse_handshake_response(buf: &Shared, state: &mut State) -> Option<(Head, usize)> {
        let bytes = buf.as_slice();
        let head_len = sark_core::http::codec::ParsedRequestHead::head_end(bytes)?.end;
        let head = std::str::from_utf8(&bytes[..head_len]).ok()?;

        let status_ok = head.starts_with("HTTP/1.1 101");
        let accept = crate::crypto::Crypto::ws_accept(head);

        match (status_ok, accept) {
            (true, Some(accept)) => {
                state.phase = ClientPhase::Active;
                Some((Head::HandshakeOk { accept }, head_len))
            }
            _ => {
                state.phase = ClientPhase::Closed;
                Some((Head::HandshakeFailed, head_len))
            }
        }
    }

    fn parse_active_frame(&self, buf: &Shared, state: &mut State) -> Option<(Head, usize)> {
        state
            .fragments
            .set_max_payload(self.config.max_message_size)
            .ok()?;
        let bytes = buf.as_slice();
        let head =
            crate::frame::FrameHead::parse(bytes, 0, self.config.max_frame_payload).ok()??;
        if bytes.len() < head.payload_end {
            return None;
        }
        if head.mask.is_some() {
            return None;
        }

        let opcode = head.opcode;
        let fin = head.fin;
        let consumed = head.payload_end;
        let payload = buf.slice(head.payload_start..head.payload_end);

        if opcode >= 0x8 {
            if !fin || payload.len() > 125 {
                return None;
            }
            return match opcode {
                0x8 => {
                    state.phase = ClientPhase::Closed;
                    Some((Head::Close(payload), consumed))
                }
                0x9 => Some((Head::Frame(Message::Ping(payload)), consumed)),
                0xA => Some((Head::Frame(Message::Pong(payload)), consumed)),
                _ => None,
            };
        }

        use crate::fragment::Push;
        match state.fragments.push(opcode, fin, payload.as_slice()) {
            Ok(Push::Direct(op, _p)) => {
                let msg = match op {
                    0x1 => Message::Text(payload),
                    0x2 => Message::Binary(payload),
                    _ => return None,
                };
                Some((Head::Frame(msg), consumed))
            }
            Ok(Push::Assembled(op, v)) => {
                let owned = Shared::from(v);
                let msg = match op {
                    0x1 => Message::Text(owned),
                    0x2 => Message::Binary(owned),
                    _ => return None,
                };
                Some((Head::Frame(msg), consumed))
            }
            Ok(Push::NeedMore) => Some((Head::Continuation, consumed)),
            Err(_) => None,
        }
    }
}

pub struct SharedState {
    conn_id: Cell<Option<Token>>,
    active_waiters: Pin<Box<WaitQueue>>,
    rng: MaskSequence,
}

impl SharedState {
    fn new(waiter_capacity: usize) -> Self {
        Self {
            conn_id: Cell::new(None),
            active_waiters: Box::pin(WaitQueue::with_capacity(waiter_capacity)),
            rng: MaskSequence::default(),
        }
    }

    fn wake(&self) {
        self.active_waiters.as_ref().wake();
    }

    fn try_register_active<'d>(
        &self,
        waiter: Pin<&dope_fiber::Waiter<'d>>,
        context: Pin<&dope_fiber::Context<'_, 'd>>,
    ) -> bool {
        self.active_waiters.as_ref().try_register(waiter, context)
    }
}

pub struct Port<'d> {
    codec: Codec,
    shared: SharedState,
    io: connector::Port<'d, Shared>,
}

pub struct PortFactory {
    config: Config,
    capacity: usize,
    waiter_capacity: usize,
}

impl<'d> Port<'d> {
    pub fn new(
        config: Config,
        capacity: usize,
        waiter_capacity: usize,
        driver: dope::DriverRef<'d>,
    ) -> Self {
        Self {
            codec: Codec { config },
            shared: SharedState::new(waiter_capacity),
            io: connector::Port::with_capacity(capacity, driver),
        }
    }

    pub fn capacity(&self) -> usize {
        self.io.capacity()
    }

    pub fn factory(config: Config, capacity: usize, waiter_capacity: usize) -> PortFactory {
        PortFactory {
            config,
            capacity,
            waiter_capacity,
        }
    }
}

impl dope::runtime::StorageFactory for PortFactory {
    type Output<'d> = Port<'d>;

    fn build<'d>(self, driver: &mut dope::DriverContext<'_, 'd>) -> Self::Output<'d> {
        Port::new(
            self.config,
            self.capacity,
            self.waiter_capacity,
            driver.driver_ref(),
        )
    }
}

pub struct Session<'d, H: Handler> {
    handler: H,
    port: &'d Port<'d>,
}

impl<'d, H: Handler> Session<'d, H> {
    pub fn new(handler: H, port: &'d Port<'d>) -> Self {
        Self { handler, port }
    }
}

#[dope_gen::connector_session(codec = port.codec, io = port.io)]
impl<'d, H: Handler> connector::Session<'d> for Session<'d, H> {
    type Codec = Codec;
    type ConnState = ConnState;
    type Send = o3::buffer::Shared;

    fn connect(&mut self, ctx: &mut connector::Ctx<'_, 'd, Self>) {
        let state = &mut *ctx.state;
        let out = &mut ctx.sink;
        let mut key_raw = [0u8; 16];
        getrandom::fill(&mut key_raw).expect("OS CSPRNG (getrandom) unavailable");
        let key_b64 = crate::crypto::Crypto::base64_encode(&key_raw);
        debug_assert_eq!(key_b64.len(), 24);
        let mut key = [0u8; 24];
        key.copy_from_slice(key_b64.as_bytes());

        let accept = crate::crypto::Crypto::expected_accept(&key_b64);
        debug_assert_eq!(accept.len(), 28);
        state.expected_accept.copy_from_slice(accept.as_bytes());
        state.closing = false;
        self.port.shared.conn_id.set(None);

        let mut headers = self.port.codec.config.headers.clone();
        if self.handler.handshake_headers(&mut headers).is_err()
            || !headers
                .iter()
                .all(|(name, value)| handshake::header_name(name) && handshake::header_value(value))
        {
            state.closing = true;
            return;
        }

        if out
            .try_enqueue(Shared::from(
                self.port.codec.handshake_request(&key, &headers),
            ))
            .is_err()
        {
            state.closing = true;
        }
    }

    fn response(&mut self, head: Head, ctx: &mut connector::Ctx<'_, 'd, Self>) {
        let conn_id = ctx.conn_id;
        let state = &mut *ctx.state;
        match head {
            Head::HandshakeOk { accept } => {
                if accept == state.expected_accept {
                    self.port.shared.conn_id.set(Some(conn_id));
                    self.port.shared.wake();
                    let mut send = SendCtx {
                        sink: ctx.sink,
                        rng: &self.port.shared.rng,
                        max_frame_payload: self.port.codec.config.max_outbound_frame_payload,
                    };
                    self.handler.open_send(conn_id, &mut send);
                } else {
                    state.closing = true;
                }
            }
            Head::HandshakeFailed => {
                self.port.shared.conn_id.set(None);
                self.port.shared.wake();
                state.closing = true;
            }
            Head::Frame(msg) => {
                if let Message::Ping(ref payload) = msg {
                    let mut send = SendCtx {
                        sink: ctx.sink,
                        rng: &self.port.shared.rng,
                        max_frame_payload: self.port.codec.config.max_outbound_frame_payload,
                    };
                    let _ = send.pong(payload.as_slice());
                }
                self.handler.message(conn_id, msg);
            }
            Head::Continuation => {}
            Head::Close(_payload) => {
                self.port.shared.conn_id.set(None);
                self.port.shared.wake();
                state.closing = true;
                self.handler.close(conn_id);
            }
        }
    }

    fn disconnect(&mut self, ctx: &mut connector::Ctx<'_, 'd, Self>) {
        self.port.io.deactivate(ctx.conn_id);
        self.port.shared.conn_id.set(None);
        self.port.shared.wake();
        self.handler.close(ctx.conn_id);
        ctx.state.closing = false;
    }
}

type HandleMarker<'a, H, S, E> = PhantomData<(&'a (), fn() -> (H, S, E))>;

pub struct WsHandle<'a, 'd, const ID: u8, H, S, E> {
    port: &'d Port<'d>,
    marker: HandleMarker<'a, H, S, E>,
}

impl<H, S, E, const ID: u8> Copy for WsHandle<'_, '_, ID, H, S, E> {}

impl<H, S, E, const ID: u8> Clone for WsHandle<'_, '_, ID, H, S, E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, 'd, const ID: u8, H, S, E> WsHandle<'a, 'd, ID, H, S, E>
where
    H: Handler + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    pub fn from_port(port: &'d Port<'d>) -> Self {
        Self {
            port,
            marker: PhantomData,
        }
    }

    pub fn from_cell(conn: Pin<&Connector<'d, ID, Session<'d, H>, S, E>>) -> Self {
        Self::from_port(conn.get_ref().session().port)
    }

    pub fn try_send_text(&self, payload: &[u8]) -> Result<(), Error> {
        Outbound::new(self.port).message(0x1, payload)
    }

    pub fn try_send_binary(&self, payload: &[u8]) -> Result<(), Error> {
        Outbound::new(self.port).message(0x2, payload)
    }
}

pub trait Client<'d, H, S, E>
where
    H: Handler + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn wait_active<'b>(&'b self) -> impl Fiber<'d, Output = Result<(), Error>> + 'b;

    fn send_text<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b;

    fn send_binary<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b;

    fn send_ping<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b;

    fn close<'b>(&'b self, payload: &'b [u8]) -> impl Fiber<'d, Output = Result<(), Error>> + 'b;
}

impl<'a, 'd, const ID: u8, H, S, E> Client<'d, H, S, E> for WsHandle<'a, 'd, ID, H, S, E>
where
    H: Handler + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn wait_active<'b>(&'b self) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        let handle = *self;
        dope_fiber::wait_fn(move |cx, waiter| {
            let shared = &handle.port.shared;
            if shared.conn_id.get().is_some() {
                return Poll::Ready(Ok(()));
            }
            if !shared.try_register_active(waiter, cx.as_ref()) {
                return Poll::Ready(Err(Error::Backpressure));
            }
            if shared.conn_id.get().is_some() {
                shared.wake();
                return Poll::Ready(Ok(()));
            }
            Poll::Pending
        })
    }

    fn send_text<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        Outbound::new(self.port).send::<false>(0x1, payload)
    }

    fn send_binary<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        Outbound::new(self.port).send::<false>(0x2, payload)
    }

    fn send_ping<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        Outbound::new(self.port).send::<true>(0x9, payload)
    }

    fn close<'b>(&'b self, payload: &'b [u8]) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        Outbound::new(self.port).send::<true>(0x8, payload)
    }
}

#[derive(Clone, Copy)]
struct Outbound<'p, 'd> {
    port: &'p Port<'d>,
}

impl<'p, 'd> Outbound<'p, 'd> {
    fn new(port: &'p Port<'d>) -> Self {
        Self { port }
    }

    fn send<'b, const CONTROL: bool>(
        self,
        opcode: u8,
        payload: &'b [u8],
    ) -> impl Fiber<'d, Output = Result<(), Error>> + 'b
    where
        'p: 'b,
        'd: 'b,
    {
        poll_fn(move |_cx| {
            if CONTROL && payload.len() > 125 {
                return Poll::Ready(Err(Error::MessageTooLarge));
            }
            let result = if CONTROL {
                self.frames(opcode, payload, payload.len().max(1), true)
            } else {
                self.message(opcode, payload)
            };
            Poll::Ready(result)
        })
    }

    fn message(self, opcode: u8, payload: &[u8]) -> Result<(), Error> {
        let max = self.port.codec.config.max_outbound_frame_payload.max(1);
        self.frames(opcode, payload, max, false)
    }

    fn frames(
        self,
        opcode: u8,
        payload: &[u8],
        max_payload: usize,
        control: bool,
    ) -> Result<(), Error> {
        let shared = &self.port.shared;
        let conn_id = shared.conn_id.get().ok_or(Error::NotConnected)?;
        let encoder = FrameEncoder::new(&shared.rng);
        let Some(result) = self.port.io.with_sender(conn_id, |mut sender| {
            encoder.enqueue(&mut sender, opcode, payload, max_payload, control)
        }) else {
            shared.conn_id.set(None);
            shared.wake();
            return Err(Error::NotConnected);
        };
        result
    }
}

#[derive(Clone, Copy)]
struct FrameEncoder<'a> {
    masks: &'a MaskSequence,
}

impl<'a> FrameEncoder<'a> {
    fn new(masks: &'a MaskSequence) -> Self {
        Self { masks }
    }

    fn enqueue<S: FrameSink + ?Sized>(
        self,
        sink: &mut S,
        opcode: u8,
        payload: &[u8],
        max_payload: usize,
        control: bool,
    ) -> Result<(), Error> {
        if control || payload.len() <= max_payload {
            if !sink.push_frame(self.frame(opcode, true, payload)) {
                return Err(Error::Backpressure);
            }
            return Ok(());
        }
        let mut off = 0;
        let mut first = true;
        while off < payload.len() {
            let end = (off + max_payload).min(payload.len());
            let fin = end == payload.len();
            let op = if first { opcode } else { 0x0 };
            if !sink.push_frame(self.frame(op, fin, &payload[off..end])) {
                return Err(Error::Backpressure);
            }
            first = false;
            off = end;
        }
        Ok(())
    }

    fn frame(self, opcode: u8, fin: bool, payload: &[u8]) -> Shared {
        let mask = self.masks.next();
        let mut frame = Vec::with_capacity(14 + payload.len());
        frame.push(if fin { 0x80 | opcode } else { opcode });
        crate::frame::FrameHead::encode_len(&mut frame, payload.len(), true);
        frame.extend_from_slice(&mask);
        let start = frame.len();
        frame.extend_from_slice(payload);
        crate::mask::Mask::unmask_in_place(&mut frame[start..], mask);
        Shared::from(frame)
    }
}

trait FrameSink {
    fn push_frame(&mut self, frame: Shared) -> bool;
}

impl FrameSink for Queue<IOV_CAP> {
    fn push_frame(&mut self, frame: Shared) -> bool {
        self.try_enqueue(frame).is_ok()
    }
}

impl FrameSink for connector::Sender<'_, '_, Shared> {
    fn push_frame(&mut self, frame: Shared) -> bool {
        self.try_enqueue(frame).is_ok()
    }
}
