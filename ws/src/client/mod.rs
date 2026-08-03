use std::{
    cell::{Cell, RefCell},
    marker::PhantomData,
    pin::Pin,
    task::Poll,
};

use dope::{
    driver::token::Token,
    manifold::{
        connector::{
            self, port,
            session::{Connector, Ctx},
            source::Dialer,
            state::IOV_CAP,
        },
        env::Env,
    },
    runtime::executor::StorageFactory,
};
use dope_fiber::abi::Fiber;
use dope_fiber::abi::pollfn::PollFn;
use dope_fiber::local::LocalContext;
use dope_fiber::raw::task::Context;
use dope_fiber::raw::wait::{WaitQueue, Waiter};
use dope_fiber::wait::WaitFn;
use dope_net::Transport;
use dope_net::link::egress::queue::Queue;
use o3::buffer::Shared;
use o3::cell::RegionToken;
use o3::collections::FixedQueue;

use self::masking::MaskSequence;
use crate::{crypto::Crypto, fragment::FragmentBuffer, frame::FrameHead};

mod handshake;
mod masking;

const DEFAULT_MAX_MESSAGE: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_OUTBOUND_FRAME: usize = 16 * 1024 * 1024;
const DEFAULT_OUTBOUND_CAPACITY: usize = 1_024;

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

pub trait Handler<'d> {
    fn handshake_headers(
        &mut self,
        _headers: &mut Vec<(String, String)>,
        _local: &mut LocalContext<'_, 'd>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn open(&mut self, _conn_id: Token, _local: &mut LocalContext<'_, 'd>) {}
    fn open_send(&mut self, conn_id: Token, _send: &mut SendCtx<'_, 'd, '_>) {
        self.open(conn_id, &mut _send.local_context());
    }
    fn message(&mut self, _conn_id: Token, _msg: Message, _local: &mut LocalContext<'_, 'd>) {}
    fn close(&mut self, _conn_id: Token, _local: &mut LocalContext<'_, 'd>) {}
}

pub struct SendCtx<'a, 'd, 'pool> {
    sink: Queue<'a, 'd, 'pool, IOV_CAP>,
    region: &'a mut RegionToken<'d>,
    rng: &'a MaskSequence,
    max_frame_payload: usize,
}

impl<'a, 'd, 'pool> SendCtx<'a, 'd, 'pool> {
    pub fn local_context(&mut self) -> LocalContext<'_, 'd> {
        LocalContext::from_region(self.region)
    }

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
        let sink = &mut self.sink;
        let region = &mut self.region;
        FrameEncoder::new(self.rng).enqueue(
            |frame| sink.try_enqueue(region, frame).is_ok(),
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
        let sink = &mut self.sink;
        let region = &mut self.region;
        FrameEncoder::new(self.rng).enqueue(
            |frame| sink.try_enqueue(region, frame).is_ok(),
            opcode,
            payload,
            payload.len().max(1),
            true,
        )
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
    pub outbound_capacity: usize,
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
            outbound_capacity: DEFAULT_OUTBOUND_CAPACITY,
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

    pub fn outbound_capacity(mut self, outbound_capacity: usize) -> Self {
        self.outbound_capacity = outbound_capacity.max(1);
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
    fragments: FragmentBuffer,
}

impl Default for State {
    fn default() -> Self {
        Self {
            phase: ClientPhase::Connecting,
            fragments: FragmentBuffer::new(DEFAULT_MAX_MESSAGE),
        }
    }
}

#[derive(Default)]
pub struct ConnState {
    expected_accept: [u8; 28],
    closing: bool,
}

impl connector::lifecycle::Lifecycle for ConnState {
    fn wants_close(&self) -> connector::lifecycle::Close {
        use dope::manifold::connector::lifecycle::Close;
        if self.closing {
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

impl connector::codec::Codec for Codec {
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
        use std::str::from_utf8;

        use sark_core::http::codec::request_head_end;
        let bytes = buf.as_slice();
        let head_len = request_head_end(bytes)?.end;
        let head = from_utf8(&bytes[..head_len]).ok()?;

        let status_ok = head.starts_with("HTTP/1.1 101");
        let accept = Crypto::ws_accept(head);

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
        let head = FrameHead::parse(bytes, 0, self.config.max_frame_payload).ok()??;
        if bytes.len() < head.payload_end {
            return None;
        }
        if head.mask.is_some() {
            return None;
        }

        let opcode = head.opcode;
        let fin = head.fin;
        let consumed = head.payload_end;
        let payload = buf.get(head.payload_start..head.payload_end)?;

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
    pending_egress: RefCell<FixedQueue<(Token, Shared)>>,
}

impl SharedState {
    fn new(waiter_capacity: usize, outbound_capacity: usize) -> Self {
        Self {
            conn_id: Cell::new(None),
            active_waiters: Box::pin(WaitQueue::with_capacity(waiter_capacity)),
            rng: MaskSequence::default(),
            pending_egress: RefCell::new(FixedQueue::with_capacity(outbound_capacity)),
        }
    }

    fn wake(&self) {
        self.active_waiters.as_ref().wake();
    }

    fn try_register_active<'d>(
        &self,
        waiter: Pin<&Waiter<'d>>,
        context: Pin<&Context<'_, 'd>>,
    ) -> bool {
        self.active_waiters.as_ref().try_register(waiter, context)
    }
}

pub struct Port<'d> {
    codec: Codec,
    shared: SharedState,
    io: port::Port<'d, Shared>,
    egress: dope_net::link::egress::storage::Storage,
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
        driver: &mut dope::DriverContext<'_, 'd>,
    ) -> Self {
        let outbound_capacity = config.outbound_capacity;
        Self {
            codec: Codec { config },
            shared: SharedState::new(waiter_capacity, outbound_capacity),
            io: port::Port::with_capacity(capacity, driver.region_token_ref(), driver.driver_ref()),
            egress: dope_net::link::egress::storage::Storage::default(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.io.capacity()
    }

    pub fn egress(&self) -> &dope_net::link::egress::storage::Storage {
        &self.egress
    }

    pub fn factory(config: Config, capacity: usize, waiter_capacity: usize) -> PortFactory {
        PortFactory {
            config,
            capacity,
            waiter_capacity,
        }
    }
}

impl StorageFactory for PortFactory {
    type Output<'d> = Port<'d>;

    fn build<'d>(self, driver: &mut dope::DriverContext<'_, 'd>) -> Self::Output<'d> {
        Port::new(self.config, self.capacity, self.waiter_capacity, driver)
    }
}

pub struct Session<'d, H: Handler<'d>> {
    handler: H,
    port: &'d Port<'d>,
}

impl<'d, H: Handler<'d>> Session<'d, H> {
    pub fn new(handler: H, port: &'d Port<'d>) -> Self {
        Self { handler, port }
    }
}

#[dope_gen::connector_session(codec = port.codec, io = port.io)]
impl<'d, H: Handler<'d>> connector::session::Session<'d> for Session<'d, H> {
    type Codec = Codec;
    type ConnState = ConnState;
    type Send = Shared;

    fn connect(&mut self, ctx: &mut Ctx<'_, '_, 'd, Self>) {
        let state = &mut *ctx.state;
        let out = &mut ctx.sink;
        let mut key_raw = [0u8; 16];
        getrandom::fill(&mut key_raw).expect("OS CSPRNG (getrandom) unavailable");
        let key_b64 = Crypto::base64_encode(&key_raw);
        debug_assert_eq!(key_b64.len(), 24);
        let mut key = [0u8; 24];
        key.copy_from_slice(key_b64.as_bytes());

        let accept = Crypto::expected_accept(&key_b64);
        debug_assert_eq!(accept.len(), 28);
        state.expected_accept.copy_from_slice(accept.as_bytes());
        state.closing = false;
        self.port.shared.conn_id.set(None);

        let mut headers = self.port.codec.config.headers.clone();
        let mut local = LocalContext::from_region(ctx.region);
        if self
            .handler
            .handshake_headers(&mut headers, &mut local)
            .is_err()
            || !headers
                .iter()
                .all(|(name, value)| handshake::header_name(name) && handshake::header_value(value))
        {
            state.closing = true;
            return;
        }

        if out
            .try_enqueue(
                ctx.region,
                Shared::from(self.port.codec.handshake_request(&key, &headers)),
            )
            .is_err()
        {
            state.closing = true;
        }
    }

    fn response(&mut self, head: Head, ctx: &mut Ctx<'_, '_, 'd, Self>) {
        let conn_id = ctx.conn_id;
        let state = &mut *ctx.state;
        match head {
            Head::HandshakeOk { accept } => {
                if accept == state.expected_accept {
                    self.port.shared.conn_id.set(Some(conn_id));
                    self.port.shared.wake();
                    let mut send = SendCtx {
                        sink: ctx.sink.reborrow(),
                        region: ctx.region,
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
                        sink: ctx.sink.reborrow(),
                        region: ctx.region,
                        rng: &self.port.shared.rng,
                        max_frame_payload: self.port.codec.config.max_outbound_frame_payload,
                    };
                    let _ = send.pong(payload.as_slice());
                }
                self.handler
                    .message(conn_id, msg, &mut LocalContext::from_region(ctx.region));
            }
            Head::Continuation => {}
            Head::Close(_payload) => {
                self.port.shared.conn_id.set(None);
                self.port.shared.wake();
                state.closing = true;
                self.handler
                    .close(conn_id, &mut LocalContext::from_region(ctx.region));
            }
        }
    }

    fn disconnect(&mut self, ctx: &mut Ctx<'_, '_, 'd, Self>) {
        self.port.io.deactivate(ctx.region, ctx.conn_id);
        self.port.shared.conn_id.set(None);
        self.port.shared.wake();
        self.handler
            .close(ctx.conn_id, &mut LocalContext::from_region(ctx.region));
        ctx.state.closing = false;
    }

    fn pre_park(&mut self, region: &mut RegionToken<'d>) {
        loop {
            let Some((target, frame)) = self.port.shared.pending_egress.borrow_mut().pop_front()
            else {
                break;
            };
            if let Err(frame) = self.port.io.try_enqueue(region, target, frame)
                && self.port.io.is_active(target)
            {
                let restored = self
                    .port
                    .shared
                    .pending_egress
                    .borrow_mut()
                    .push_front((target, frame))
                    .is_ok();
                debug_assert!(restored, "popped handoff slot must remain available");
                break;
            }
        }
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
    H: Handler<'d> + 'd,
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
        Outbound::new(self.port).message_pending(0x1, payload)
    }

    pub fn try_send_binary(&self, payload: &[u8]) -> Result<(), Error> {
        Outbound::new(self.port).message_pending(0x2, payload)
    }
}

pub trait Client<'d, H, S, E>
where
    H: Handler<'d> + 'd,
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
    H: Handler<'d> + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn wait_active<'b>(&'b self) -> impl Fiber<'d, Output = Result<(), Error>> + 'b {
        let handle = *self;
        WaitFn::new(move |cx, waiter| {
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
        PollFn::new(move |mut cx| {
            if CONTROL && payload.len() > 125 {
                return Poll::Ready(Err(Error::MessageTooLarge));
            }
            let result = if CONTROL {
                self.frames(
                    cx.as_mut().region_token(),
                    opcode,
                    payload,
                    payload.len().max(1),
                    true,
                )
            } else {
                self.message(cx.as_mut().region_token(), opcode, payload)
            };
            Poll::Ready(result)
        })
    }

    fn message(
        self,
        region: &mut RegionToken<'d>,
        opcode: u8,
        payload: &[u8],
    ) -> Result<(), Error> {
        let max = self.port.codec.config.max_outbound_frame_payload.max(1);
        self.frames(region, opcode, payload, max, false)
    }

    fn frames(
        self,
        region: &mut RegionToken<'d>,
        opcode: u8,
        payload: &[u8],
        max_payload: usize,
        control: bool,
    ) -> Result<(), Error> {
        let shared = &self.port.shared;
        let conn_id = shared.conn_id.get().ok_or(Error::NotConnected)?;
        let encoder = FrameEncoder::new(&shared.rng);
        let Some(result) = self.port.io.with_sender(conn_id, |sender| {
            encoder.enqueue(
                |frame| sender.try_enqueue(region, frame).is_ok(),
                opcode,
                payload,
                max_payload,
                control,
            )
        }) else {
            shared.conn_id.set(None);
            shared.wake();
            return Err(Error::NotConnected);
        };
        result
    }

    fn message_pending(self, opcode: u8, payload: &[u8]) -> Result<(), Error> {
        let max_payload = self.port.codec.config.max_outbound_frame_payload.max(1);
        let shared = &self.port.shared;
        let conn_id = shared.conn_id.get().ok_or(Error::NotConnected)?;
        let required = payload.len().max(1).div_ceil(max_payload);
        let mut pending = shared.pending_egress.borrow_mut();
        if pending.capacity() - pending.len() < required {
            return Err(Error::Backpressure);
        }
        let result = FrameEncoder::new(&shared.rng).enqueue(
            |frame| pending.push_back((conn_id, frame)).is_ok(),
            opcode,
            payload,
            max_payload,
            false,
        );
        debug_assert!(
            result.is_ok(),
            "reserved handoff capacity must be sufficient"
        );
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

    fn enqueue(
        self,
        mut push: impl FnMut(Shared) -> bool,
        opcode: u8,
        payload: &[u8],
        max_payload: usize,
        control: bool,
    ) -> Result<(), Error> {
        if control || payload.len() <= max_payload {
            if !push(self.frame(opcode, true, payload)) {
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
            if !push(self.frame(op, fin, &payload[off..end])) {
                return Err(Error::Backpressure);
            }
            first = false;
            off = end;
        }
        Ok(())
    }

    fn frame(self, opcode: u8, fin: bool, payload: &[u8]) -> Shared {
        use crate::mask::Mask;
        let mask = self.masks.next();
        let mut frame = Vec::with_capacity(14 + payload.len());
        frame.push(if fin { 0x80 | opcode } else { opcode });
        FrameHead::encode_len(&mut frame, payload.len(), true);
        frame.extend_from_slice(&mask);
        let start = frame.len();
        frame.extend_from_slice(payload);
        Mask::unmask_in_place(&mut frame[start..], mask);
        Shared::from(frame)
    }
}
