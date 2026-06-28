use dope::Driver;
use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application};
use dope::transport::link::Slot;
use dope::transport::wire::{Identity, RecvChunk};

use crate::crypto::Crypto;
use crate::fragment::{FragmentBuffer, Push};
use crate::frame::{FrameError, FrameHead};
use crate::mask::Mask;

const MAX_HANDSHAKE_BYTES: usize = 16 * 1024;
const WS_MAX_MESSAGE: usize = 16 * 1024 * 1024;
const WS_MAX_ACC: usize = WS_MAX_MESSAGE + 64 * 1024;

const CLOSE_PROTOCOL_ERROR: u16 = 1002;
const CLOSE_INVALID_PAYLOAD: u16 = 1007;
const CLOSE_MESSAGE_TOO_BIG: u16 = 1009;

const HANDSHAKE_PREFIX: &[u8] = b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: ";
const HANDSHAKE_SUFFIX: &[u8] = b"\r\n\r\n";
const BAD_REQUEST: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Handshake,
    Active,
    Closed,
}

pub struct ConnState {
    phase: Phase,
    acc: Vec<u8>,
    fragments: FragmentBuffer,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            phase: Phase::Handshake,
            acc: Vec::new(),
            fragments: FragmentBuffer::new(WS_MAX_MESSAGE),
        }
    }
}

use sark_core::identity_mut;

#[derive(Debug, Clone, Copy)]
pub enum Message<'a> {
    Text(&'a str),
    Binary(&'a [u8]),
}

pub struct Response<'a> {
    write: &'a mut [u8],
    written: usize,
    close_after: bool,
}

impl<'a> Response<'a> {
    fn new(write: &'a mut [u8]) -> Self {
        Self {
            write,
            written: 0,
            close_after: false,
        }
    }

    pub fn text(&mut self, s: &str) -> bool {
        self.frame(0x1, s.as_bytes())
    }

    pub fn binary(&mut self, data: &[u8]) -> bool {
        self.frame(0x2, data)
    }

    pub fn close(&mut self, payload: &[u8]) -> bool {
        let ok = self.frame(0x8, payload);
        if ok {
            self.close_after = true;
        }
        ok
    }

    fn put_raw(&mut self, src: &[u8]) -> bool {
        if self.write.len() - self.written < src.len() {
            return false;
        }
        self.write[self.written..self.written + src.len()].copy_from_slice(src);
        self.written += src.len();
        true
    }

    fn frame(&mut self, opcode: u8, payload: &[u8]) -> bool {
        let off = self.written;
        let total = FrameHead::header_len(payload.len()) + payload.len();
        if self.write.len() - off < total {
            return false;
        }
        let hlen =
            FrameHead::encode_header_into(&mut self.write[off..], opcode, payload.len(), false);
        self.write[off + hlen..off + total].copy_from_slice(payload);
        self.written += total;
        true
    }

    fn send_close_code(&mut self, code: u16) {
        let _ = self.frame(0x8, &code.to_be_bytes());
        self.close_after = true;
    }

    fn valid_close_code(code: u16) -> bool {
        matches!(code, 1000..=1003 | 1007..=1011 | 3000..=4999)
    }

    fn handle_close(&mut self, payload: &[u8]) {
        if payload.is_empty() {
            let _ = self.close(&[]);
            return;
        }
        if payload.len() < 2 {
            self.send_close_code(CLOSE_PROTOCOL_ERROR);
            return;
        }
        let code = u16::from_be_bytes([payload[0], payload[1]]);
        if !Self::valid_close_code(code) {
            self.send_close_code(CLOSE_PROTOCOL_ERROR);
            return;
        }
        if std::str::from_utf8(&payload[2..]).is_err() {
            self.send_close_code(CLOSE_INVALID_PAYLOAD);
            return;
        }
        let _ = self.close(payload);
    }

    fn handle_control(&mut self, opcode: u8, fin: bool, payload: &[u8]) {
        if !fin || payload.len() > 125 {
            self.send_close_code(CLOSE_PROTOCOL_ERROR);
            return;
        }
        match opcode {
            0x9 => {
                let _ = self.frame(0xA, payload);
            }
            0xA => {}
            0x8 => self.handle_close(payload),
            _ => self.send_close_code(CLOSE_PROTOCOL_ERROR),
        }
    }
}

/// Byte source for [`App::drive_frames_over`], monomorphized over the buffered
/// ([`AccSource`]) and zero-copy ([`ChunkSource`]) paths.
trait FrameSource {
    fn bytes(&self) -> &[u8];
    /// Unmask `[start, end)` (masked with `mask`) and return it; valid until the
    /// next call.
    fn unmask(&mut self, start: usize, end: usize, mask: [u8; 4]) -> &[u8];
    /// Finalize after `pos` bytes of [`bytes`](Self::bytes) were consumed.
    fn commit(&mut self, pos: usize);
}

struct AccSource<'a> {
    acc: &'a mut Vec<u8>,
}

impl FrameSource for AccSource<'_> {
    fn bytes(&self) -> &[u8] {
        self.acc
    }
    fn unmask(&mut self, start: usize, end: usize, mask: [u8; 4]) -> &[u8] {
        Mask::unmask_inline(&mut self.acc[start..end], mask);
        &self.acc[start..end]
    }
    fn commit(&mut self, pos: usize) {
        self.acc.drain(..pos);
    }
}

/// Parses straight from the read-only recv `chunk`, unmask-copying each payload
/// into reusable `scratch`. [`commit`](FrameSource::commit) stashes only the
/// trailing partial frame — still masked — into `acc` for the next recv.
struct ChunkSource<'a> {
    chunk: &'a [u8],
    scratch: &'a mut Vec<u8>,
    acc: &'a mut Vec<u8>,
}

impl FrameSource for ChunkSource<'_> {
    fn bytes(&self) -> &[u8] {
        self.chunk
    }
    fn unmask(&mut self, start: usize, end: usize, mask: [u8; 4]) -> &[u8] {
        let len = end - start;
        if self.scratch.len() < len {
            self.scratch.resize(len, 0);
        }
        Mask::unmask_copy(&mut self.scratch[..len], &self.chunk[start..end], mask);
        &self.scratch[..len]
    }
    fn commit(&mut self, pos: usize) {
        self.acc.clear();
        self.acc.extend_from_slice(&self.chunk[pos..]);
    }
}

pub trait Handler: Clone + 'static {
    fn on_message<'a>(&self, msg: Message<'a>, response: &mut Response<'_>);
}

impl<F> Handler for F
where
    F: Fn(Message<'_>, &mut Response<'_>) + Clone + 'static,
{
    fn on_message<'a>(&self, msg: Message<'a>, response: &mut Response<'_>) {
        (self)(msg, response)
    }
}

pub struct App<H: Handler> {
    user: H,
    expected_path: &'static str,
    max_frame_payload: usize,
    /// Per-worker unmask scratch for the zero-copy path; grows to the largest
    /// payload seen.
    scratch: Vec<u8>,
}

impl<H: Handler> App<H> {
    pub fn new(user: H, expected_path: &'static str, max_frame_payload: usize) -> Self {
        Self {
            user,
            expected_path,
            max_frame_payload,
            scratch: Vec::new(),
        }
    }

    #[inline]
    fn frame_cap(&self) -> usize {
        self.max_frame_payload.min(WS_MAX_MESSAGE)
    }
}

impl<H: Handler> Application for App<H> {
    type Conn = ConnState;
    type Wire = Identity;

    fn on_chunk(
        &mut self,
        slot: &mut Slot<Identity, listener::State<ConnState>>,
        chunk: RecvChunk<'_>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) -> Outcome {
        self.on_chunk_proj(slot, identity_mut, chunk, aux, driver)
    }

    fn on_send(
        &mut self,
        slot: &mut Slot<Identity, listener::State<ConnState>>,
        sent: usize,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) {
        self.on_send_proj(slot, sent, identity_mut, aux, driver)
    }

    fn on_close(
        &mut self,
        _slot: &mut Slot<Identity, listener::State<ConnState>>,
        _aux: &mut listener::Aux,
    ) {
    }
}

impl<H: Handler> App<H> {
    pub fn on_chunk_proj<C: Default + 'static>(
        &mut self,
        slot: &mut Slot<Identity, listener::State<C>>,
        project: impl Fn(&mut C) -> &mut ConnState,
        chunk: RecvChunk<'_>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) -> Outcome {
        if project(&mut slot.state.conn).phase == Phase::Closed {
            return Outcome::Ok;
        }
        let chunk = chunk.as_slice();

        // Zero-copy when nothing is buffered and no send is in flight: parse the
        // recv chunk in place. A leftover partial frame or send backpressure
        // falls through to the buffered path, which keeps the WS_MAX_ACC bound.
        let fast = {
            let state = project(&mut slot.state.conn);
            state.phase == Phase::Active && state.acc.is_empty()
        } && !slot.core.is_send_inflight()
            && chunk.len() <= WS_MAX_ACC;

        if fast {
            let send_ud = slot.token();
            let write_buf = aux.write_buf_for(slot);
            let frame_cap = self.frame_cap();
            let user = &self.user;
            let scratch = &mut self.scratch;
            let (written, closed) = {
                let mut response = Response::new(&mut *write_buf);
                let state = project(&mut slot.state.conn);
                let src = ChunkSource {
                    chunk,
                    scratch,
                    acc: &mut state.acc,
                };
                let closed = Self::drive_frames_over(
                    user,
                    frame_cap,
                    src,
                    &mut state.fragments,
                    &mut response,
                );
                if closed {
                    state.phase = Phase::Closed;
                }
                (response.written, closed)
            };
            if closed {
                slot.core.set_close_after();
            }
            if written > 0 {
                slot.submit_buffered(write_buf, written, send_ud, driver);
            }
            return Outcome::Ok;
        }

        project(&mut slot.state.conn).acc.extend_from_slice(chunk);
        if project(&mut slot.state.conn).acc.len() > WS_MAX_ACC {
            let state = project(&mut slot.state.conn);
            state.phase = Phase::Closed;
            state.acc = Vec::new();
            if !slot.core.is_send_inflight() {
                self.emit_close_proj(slot, CLOSE_MESSAGE_TOO_BIG, aux, driver, &project);
            }
            return Outcome::CloseAfter;
        }
        if !slot.core.is_send_inflight() {
            self.pump_proj(slot, aux, driver, &project);
        }
        Outcome::Ok
    }

    pub fn on_send_proj<C: Default + 'static>(
        &mut self,
        slot: &mut Slot<Identity, listener::State<C>>,
        _sent: usize,
        project: impl Fn(&mut C) -> &mut ConnState,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) {
        if project(&mut slot.state.conn).phase != Phase::Closed {
            self.pump_proj(slot, aux, driver, &project);
        }
    }

    fn pump_proj<C: Default + 'static, P: Fn(&mut C) -> &mut ConnState>(
        &self,
        slot: &mut Slot<Identity, listener::State<C>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
        project: &P,
    ) {
        let send_ud = slot.token();
        let write_buf = aux.write_buf_for(slot);
        let (written, close_after) = {
            let mut response = Response::new(&mut *write_buf);
            let state = project(&mut slot.state.conn);
            if state.phase == Phase::Handshake {
                self.try_handshake(state, &mut response);
            }
            if state.phase == Phase::Active {
                self.drive_frames(state, &mut response);
            }
            (response.written, response.close_after)
        };
        if close_after {
            slot.core.set_close_after();
        }
        if written > 0 {
            slot.submit_buffered(write_buf, written, send_ud, driver);
        }
    }

    fn emit_close_proj<C: Default + 'static, P: Fn(&mut C) -> &mut ConnState>(
        &self,
        slot: &mut Slot<Identity, listener::State<C>>,
        code: u16,
        aux: &mut listener::Aux,
        driver: &mut Driver,
        _project: &P,
    ) {
        let send_ud = slot.token();
        let write_buf = aux.write_buf_for(slot);
        let written = {
            let mut response = Response::new(&mut *write_buf);
            response.send_close_code(code);
            response.written
        };
        slot.core.set_close_after();
        if written > 0 {
            slot.submit_buffered(write_buf, written, send_ud, driver);
        }
    }

    fn try_handshake(&self, state: &mut ConnState, response: &mut Response<'_>) {
        if state.acc.len() > MAX_HANDSHAKE_BYTES {
            state.phase = Phase::Closed;
            response.put_raw(BAD_REQUEST);
            response.close_after = true;
            return;
        }
        let Some(crlf) = sark_core::http::codec::Parse::find_double_crlf(&state.acc) else {
            return;
        };
        let head_len = crlf.end;
        let key = std::str::from_utf8(&state.acc[..head_len])
            .ok()
            .and_then(|s| Self::validate_handshake(s, self.expected_path).ok());
        let Some(key) = key else {
            state.phase = Phase::Closed;
            response.put_raw(BAD_REQUEST);
            response.close_after = true;
            return;
        };
        let accept = Crypto::expected_accept(&key);
        debug_assert_eq!(accept.len(), 28);
        let ok = response.put_raw(HANDSHAKE_PREFIX)
            && response.put_raw(accept.as_bytes())
            && response.put_raw(HANDSHAKE_SUFFIX);
        if !ok {
            state.phase = Phase::Closed;
            response.close_after = true;
            return;
        }
        state.phase = Phase::Active;
        state.acc.drain(..head_len);
    }

    fn validate_handshake(head: &str, expected_path: &str) -> Result<String, ()> {
        let (request, rest) = head.split_once("\r\n").unwrap_or((head, ""));
        let mut parts = request.split_whitespace();
        let method = parts.next().unwrap_or("");
        let target = parts.next().unwrap_or("");
        let version = parts.next().unwrap_or("");
        if method != "GET" || version != "HTTP/1.1" {
            return Err(());
        }
        if !expected_path.is_empty() && target != expected_path {
            return Err(());
        }
        let mut upgrade = None::<&str>;
        let mut connection = None::<&str>;
        let mut ws_version = None::<&str>;
        let mut key = None::<&str>;
        for (name, value) in sark_core::http::head::header_lines(rest.as_bytes()) {
            let Ok(value) = std::str::from_utf8(value) else {
                continue;
            };
            if name.eq_ignore_ascii_case(b"upgrade") {
                upgrade = Some(value);
            } else if name.eq_ignore_ascii_case(b"connection") {
                connection = Some(value);
            } else if name.eq_ignore_ascii_case(b"sec-websocket-version") {
                ws_version = Some(value);
            } else if name.eq_ignore_ascii_case(b"sec-websocket-key") {
                key = Some(value);
            }
        }
        let upgrade = upgrade.ok_or(())?;
        if !upgrade.eq_ignore_ascii_case("websocket") {
            return Err(());
        }
        let connection = connection.ok_or(())?;
        if !connection
            .split(',')
            .any(|p| p.trim().eq_ignore_ascii_case("upgrade"))
        {
            return Err(());
        }
        let ws_version = ws_version.ok_or(())?;
        if ws_version != "13" {
            return Err(());
        }
        let key = key.ok_or(())?;
        Ok(key.to_string())
    }

    fn drive_frames(&self, state: &mut ConnState, response: &mut Response<'_>) {
        let src = AccSource {
            acc: &mut state.acc,
        };
        if Self::drive_frames_over(
            &self.user,
            self.frame_cap(),
            src,
            &mut state.fragments,
            response,
        ) {
            state.phase = Phase::Closed;
        }
    }

    /// The single framing loop, monomorphized over [`FrameSource`]: parse, unmask,
    /// handle control/fragment frames, dispatch data. Returns whether to close.
    fn drive_frames_over<S: FrameSource>(
        user: &H,
        frame_cap: usize,
        mut src: S,
        fragments: &mut FragmentBuffer,
        response: &mut Response<'_>,
    ) -> bool {
        let mut pos = 0;
        let mut closed = false;
        loop {
            let head = match FrameHead::parse(src.bytes(), pos, frame_cap) {
                Ok(Some(h)) => h,
                Ok(None) => break,
                Err(FrameError::PayloadTooLarge) => {
                    response.send_close_code(CLOSE_MESSAGE_TOO_BIG);
                    closed = true;
                    break;
                }
                Err(_) => {
                    response.send_close_code(CLOSE_PROTOCOL_ERROR);
                    closed = true;
                    break;
                }
            };
            let fin = head.fin;
            let opcode = head.opcode;
            let (start, end) = (head.payload_start, head.payload_end);
            let Some(mask) = head.mask else {
                response.send_close_code(CLOSE_PROTOCOL_ERROR);
                closed = true;
                break;
            };
            let payload = src.unmask(start, end, mask);
            pos = end;

            if opcode >= 0x8 {
                response.handle_control(opcode, fin, payload);
                if opcode == 0x8 || response.close_after {
                    closed = true;
                    break;
                }
                continue;
            }

            match fragments.push(opcode, fin, payload) {
                Ok(Push::Direct(op, p)) => Self::dispatch(user, op, p, response),
                Ok(Push::Assembled(op, p)) => Self::dispatch(user, op, &p, response),
                Ok(Push::NeedMore) => {}
                Err(crate::fragment::FragmentError::PayloadTooLarge) => {
                    response.send_close_code(CLOSE_MESSAGE_TOO_BIG);
                    closed = true;
                    break;
                }
                Err(_) => {
                    response.send_close_code(CLOSE_PROTOCOL_ERROR);
                    closed = true;
                    break;
                }
            }
            if response.close_after {
                closed = true;
                break;
            }
        }
        src.commit(pos);
        closed
    }

    fn dispatch(user: &H, opcode: u8, payload: &[u8], response: &mut Response<'_>) {
        match opcode {
            0x1 => match std::str::from_utf8(payload) {
                Ok(s) => user.on_message(Message::Text(s), response),
                Err(_) => response.send_close_code(CLOSE_INVALID_PAYLOAD),
            },
            0x2 => user.on_message(Message::Binary(payload), response),
            _ => response.send_close_code(CLOSE_PROTOCOL_ERROR),
        }
    }
}
