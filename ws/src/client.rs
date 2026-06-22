use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;
use std::time::{SystemTime, UNIX_EPOCH};

use dope::fiber::{Fiber, Holding};
use dope::manifold::connector;
use dope::manifold::connector::Connector;
use dope::manifold::connector::session::{IOV_CAP, Queue};
use dope::manifold::connector::source::Dialer;
use dope::manifold::env::Env;
use dope::runtime::token::Token;
use dope::transport::Transport;
use dope::{WakeRef, WakerSet};
use o3::buffer::Shared;

const DEFAULT_MAX_MESSAGE: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_OUTBOUND_FRAME: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    NotConnected,
    Backpressure,
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

pub trait Handler: 'static {
    fn on_handshake_headers(&mut self, _headers: &mut Vec<(String, String)>) -> Result<(), Error> {
        Ok(())
    }

    fn on_open(&mut self, _conn_id: Token) {}
    fn on_open_send(&mut self, conn_id: Token, _send: &mut SendCtx<'_>) {
        self.on_open(conn_id);
    }
    fn on_message(&mut self, _conn_id: Token, _msg: Message) {}
    fn on_close(&mut self, _conn_id: Token) {}
}

pub struct SendCtx<'a> {
    state: &'a mut ConnState,
    sink: &'a mut Queue<IOV_CAP>,
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
        Encode::frames_into(
            self.sink,
            &mut self.state.rng,
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
        Encode::frames_into(
            self.sink,
            &mut self.state.rng,
            opcode,
            payload,
            payload.len().max(1),
            true,
        )
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub host: String,
    pub path: String,
    pub user_agent: String,
    pub headers: Vec<(String, String)>,
    pub max_frame_payload: usize,
    pub max_message_size: usize,
    pub max_outbound_frame_payload: usize,
}

impl ClientConfig {
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

    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Result<Self, Error> {
        let user_agent = user_agent.into();
        if !Validate::header_value(&user_agent) {
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
        if !Validate::header_name(&name) || !Validate::header_value(&value) {
            return Err(Error::InvalidHeader);
        }
        self.headers.push((name, value));
        Ok(self)
    }

    pub fn with_max_message_size(mut self, n: usize) -> Self {
        self.max_message_size = n.max(1);
        self
    }

    pub fn with_max_frame_payload(mut self, n: usize) -> Self {
        self.max_frame_payload = n.max(1);
        self
    }

    pub fn with_max_outbound_frame_payload(mut self, n: usize) -> Self {
        self.max_outbound_frame_payload = n.max(1);
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
    rng: MaskRng,
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

#[derive(Default)]
struct MaskRng {
    state: u64,
}

impl MaskRng {
    fn fill(&mut self, buf: &mut [u8]) {
        let mut s = self.state;
        if s == 0 {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0xCAFE_BABE_DEAD_BEEF);
            s = now ^ (buf.as_ptr() as u64).rotate_left(17);
            if s == 0 {
                s = 0x9E37_79B9_7F4A_7C15;
            }
        }
        for chunk in buf.chunks_mut(8) {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            let bytes = s.to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
        self.state = s;
    }

    fn mask(&mut self) -> [u8; 4] {
        let mut buf = [0u8; 4];
        self.fill(&mut buf);
        buf
    }
}

pub struct Codec {
    config: ClientConfig,
}

impl Codec {
    fn handshake_request(&self, key_b64: &[u8; 24], headers: &[(String, String)]) -> Vec<u8> {
        let extra_headers: usize = headers
            .iter()
            .map(|(name, value)| name.len() + value.len() + 4)
            .sum();
        let mut req = Vec::with_capacity(
            192 + self.config.host.len() + self.config.path.len() + extra_headers,
        );
        req.extend_from_slice(b"GET ");
        req.extend_from_slice(self.config.path.as_bytes());
        req.extend_from_slice(b" HTTP/1.1\r\nHost: ");
        req.extend_from_slice(self.config.host.as_bytes());
        req.extend_from_slice(b"\r\nUser-Agent: ");
        req.extend_from_slice(self.config.user_agent.as_bytes());
        req.extend_from_slice(b"\r\nAccept: ");
        req.push(b'*');
        req.push(b'/');
        req.push(b'*');
        req.extend_from_slice(
            b"\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ",
        );
        req.extend_from_slice(key_b64);
        req.extend_from_slice(b"\r\nConnection: Upgrade\r\n");
        for (name, value) in headers {
            req.extend_from_slice(name.as_bytes());
            req.extend_from_slice(b": ");
            req.extend_from_slice(value.as_bytes());
            req.extend_from_slice(b"\r\n");
        }
        req.extend_from_slice(b"\r\n");
        req
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
        let head_len = sark_core::http::codec::Parse::find_double_crlf(bytes)?.end;
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
    conn_id: Option<Token>,
    active_wakers: WakerSet,
}

impl SharedState {
    fn new() -> Self {
        Self {
            conn_id: None,
            active_wakers: WakerSet::new(),
        }
    }
}

pub struct Session<H: Handler> {
    codec: Codec,
    handler: H,
    shared: SharedState,
}

impl<H: Handler> Session<H> {
    pub fn new(handler: H, host: &'static str, path: &'static str) -> Self {
        Self::with_config(handler, ClientConfig::new(host, path))
    }

    pub fn with_config(handler: H, config: ClientConfig) -> Self {
        Self {
            codec: Codec { config },
            handler,
            shared: SharedState::new(),
        }
    }
}

impl<H: Handler> connector::Session for Session<H> {
    type Codec = Codec;
    type ConnState = ConnState;

    fn codec(&self) -> &Codec {
        &self.codec
    }

    fn connect(&mut self, ctx: &mut connector::Ctx<'_, Self>) {
        let state = &mut *ctx.state;
        let out = &mut ctx.sink;
        let mut key_raw = [0u8; 16];
        state.rng.fill(&mut key_raw);
        let key_b64 = crate::crypto::Crypto::base64_encode(&key_raw);
        debug_assert_eq!(key_b64.len(), 24);
        let mut key = [0u8; 24];
        key.copy_from_slice(key_b64.as_bytes());

        let accept = crate::crypto::Crypto::expected_accept(&key_b64);
        debug_assert_eq!(accept.len(), 28);
        state.expected_accept.copy_from_slice(accept.as_bytes());
        state.closing = false;
        self.shared.conn_id = None;

        let mut headers = self.codec.config.headers.clone();
        if self.handler.on_handshake_headers(&mut headers).is_err()
            || !headers
                .iter()
                .all(|(name, value)| Validate::header_name(name) && Validate::header_value(value))
        {
            state.closing = true;
            return;
        }

        out.push(Shared::from(self.codec.handshake_request(&key, &headers)));
    }

    fn response(&mut self, head: Head, ctx: &mut connector::Ctx<'_, Self>) {
        let conn_id = ctx.conn_id;
        let state = &mut *ctx.state;
        match head {
            Head::HandshakeOk { accept } => {
                if accept == state.expected_accept {
                    self.shared.conn_id = Some(conn_id);
                    self.shared.active_wakers.drain_wake();
                    let mut send = SendCtx {
                        state,
                        sink: ctx.sink,
                        max_frame_payload: self.codec.config.max_outbound_frame_payload,
                    };
                    self.handler.on_open_send(conn_id, &mut send);
                } else {
                    state.closing = true;
                }
            }
            Head::HandshakeFailed => {
                self.shared.conn_id = None;
                self.shared.active_wakers.drain_wake();
                state.closing = true;
            }
            Head::Frame(msg) => {
                if let Message::Ping(ref payload) = msg {
                    let mut send = SendCtx {
                        state,
                        sink: ctx.sink,
                        max_frame_payload: self.codec.config.max_outbound_frame_payload,
                    };
                    let _ = send.pong(payload.as_slice());
                }
                self.handler.on_message(conn_id, msg);
            }
            Head::Continuation => {}
            Head::Close(_payload) => {
                self.shared.conn_id = None;
                self.shared.active_wakers.drain_wake();
                state.closing = true;
                self.handler.on_close(conn_id);
            }
        }
    }

    fn disconnect(&mut self, ctx: &mut connector::Ctx<'_, Self>) {
        self.shared.conn_id = None;
        self.shared.active_wakers.drain_wake();
        self.handler.on_close(ctx.conn_id);
        ctx.state.closing = false;
    }
}

pub trait Client<'d, H, S, E>
where
    H: Handler + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn wait_active<'b>(&'b self) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b>;

    fn send_text<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b>;

    fn send_binary<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b>;

    fn send_ping<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b>;

    fn close<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b>;
}

impl<'d, const ID: u8, H, S, E> Client<'d, H, S, E> for Holding<'d, Connector<ID, Session<H>, S, E>>
where
    H: Handler + 'd,
    S: Dialer<E::Transport> + 'd,
    E: Env + 'd,
    E::Transport: Transport<Addr: Clone>,
{
    fn wait_active<'b>(&'b self) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b> {
        let holding = *self;
        Fiber::new(poll_fn(move |cx| {
            let mut h = holding.hold();
            let shared = &mut h.as_mut().session_mut().shared;
            if shared.conn_id.is_some() {
                return Poll::Ready(Ok(()));
            }
            // SAFETY: cx.waker() was minted by the dope dispatcher's Slot::make_waker.
            shared.active_wakers.register(WakeRef::verified(cx.waker()));
            Poll::Pending
        }))
    }

    fn send_text<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b> {
        Encode::message(*self, 0x1, payload)
    }

    fn send_binary<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b> {
        Encode::message(*self, 0x2, payload)
    }

    fn send_ping<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b> {
        Encode::control(*self, 0x9, payload)
    }

    fn close<'b>(
        &'b self,
        payload: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b> {
        Encode::control(*self, 0x8, payload)
    }
}

struct Encode;

impl Encode {
    fn message<'d, 'b, const ID: u8, H, S, E>(
        holding: Holding<'d, Connector<ID, Session<H>, S, E>>,
        opcode: u8,
        payload: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b>
    where
        H: Handler + 'd,
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport<Addr: Clone>,
        'd: 'b,
    {
        Fiber::new(poll_fn(move |_cx| {
            let mut h = holding.hold();
            Poll::Ready(Self::message_now(h.as_mut(), opcode, payload))
        }))
    }

    fn control<'d, 'b, const ID: u8, H, S, E>(
        holding: Holding<'d, Connector<ID, Session<H>, S, E>>,
        opcode: u8,
        payload: &'b [u8],
    ) -> Fiber<'d, impl Future<Output = Result<(), Error>> + 'b>
    where
        H: Handler + 'd,
        S: Dialer<E::Transport> + 'd,
        E: Env + 'd,
        E::Transport: Transport<Addr: Clone>,
        'd: 'b,
    {
        Fiber::new(poll_fn(move |_cx| {
            let mut h = holding.hold();
            if payload.len() > 125 {
                return Poll::Ready(Err(Error::MessageTooLarge));
            }
            Poll::Ready(Self::frames_now(
                h.as_mut(),
                opcode,
                payload,
                payload.len().max(1),
                true,
            ))
        }))
    }

    fn message_now<const ID: u8, H, S, E>(
        pool: Pin<&mut Connector<ID, Session<H>, S, E>>,
        opcode: u8,
        payload: &[u8],
    ) -> Result<(), Error>
    where
        H: Handler,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let max = pool
            .as_ref()
            .session()
            .codec
            .config
            .max_outbound_frame_payload
            .max(1);
        Self::frames_now(pool, opcode, payload, max, false)
    }

    fn frames_now<const ID: u8, H, S, E>(
        mut pool: Pin<&mut Connector<ID, Session<H>, S, E>>,
        opcode: u8,
        payload: &[u8],
        max_payload: usize,
        control: bool,
    ) -> Result<(), Error>
    where
        H: Handler,
        S: Dialer<E::Transport>,
        E: Env,
        E::Transport: Transport<Addr: Clone>,
    {
        let conn_id = pool
            .as_ref()
            .session()
            .shared
            .conn_id
            .ok_or(Error::NotConnected)?;
        let Some(channel) = pool.as_mut().state_for(conn_id) else {
            let shared = &mut pool.as_mut().session_mut().shared;
            shared.conn_id = None;
            shared.active_wakers.drain_wake();
            return Err(Error::NotConnected);
        };
        if control || payload.len() <= max_payload {
            let frame =
                Self::client_frame(&mut channel.conn_state_mut().rng, opcode, true, payload);
            if !channel.enqueue(frame) {
                return Err(Error::Backpressure);
            }
        } else {
            let mut off = 0;
            let mut first = true;
            while off < payload.len() {
                let end = (off + max_payload).min(payload.len());
                let fin = end == payload.len();
                let op = if first { opcode } else { 0x0 };
                let frame = Self::client_frame(
                    &mut channel.conn_state_mut().rng,
                    op,
                    fin,
                    &payload[off..end],
                );
                if !channel.enqueue(frame) {
                    return Err(Error::Backpressure);
                }
                first = false;
                off = end;
            }
        }
        pool.request_flush(conn_id);
        Ok(())
    }

    fn frames_into(
        sink: &mut Queue<IOV_CAP>,
        rng: &mut MaskRng,
        opcode: u8,
        payload: &[u8],
        max_payload: usize,
        control: bool,
    ) -> Result<(), Error> {
        if control || payload.len() <= max_payload {
            if sink.over_cap() {
                return Err(Error::Backpressure);
            }
            sink.push(Self::client_frame(rng, opcode, true, payload));
            return Ok(());
        }
        let mut off = 0;
        let mut first = true;
        while off < payload.len() {
            let end = (off + max_payload).min(payload.len());
            let fin = end == payload.len();
            let op = if first { opcode } else { 0x0 };
            if sink.over_cap() {
                return Err(Error::Backpressure);
            }
            sink.push(Self::client_frame(rng, op, fin, &payload[off..end]));
            first = false;
            off = end;
        }
        Ok(())
    }

    fn client_frame(rng: &mut MaskRng, opcode: u8, fin: bool, payload: &[u8]) -> Shared {
        let mask = rng.mask();
        let mut frame = Vec::with_capacity(14 + payload.len());
        frame.push(if fin { 0x80 | opcode } else { opcode });
        crate::frame::FrameHead::encode_len(&mut frame, payload.len(), true);
        frame.extend_from_slice(&mask);
        let start = frame.len();
        frame.extend_from_slice(payload);
        crate::mask::Mask::unmask_inline(&mut frame[start..], mask);
        Shared::from(frame)
    }
}

struct Validate;

impl Validate {
    fn header_name(s: &str) -> bool {
        !s.is_empty()
            && s.bytes().all(|b| {
                matches!(
                    b,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                        | b'0'..=b'9'
                        | b'A'..=b'Z'
                        | b'a'..=b'z'
                )
            })
    }

    fn header_value(s: &str) -> bool {
        !s.bytes().any(|b| matches!(b, b'\r' | b'\n'))
    }
}
