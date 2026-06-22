use dope::Driver;
use dope::manifold::Outcome;
use dope::manifold::listener::{self, Application};
use dope::transport::link::Slot;
use dope::transport::wire::{Identity, RecvChunk};

use crate::crypto::Crypto;
use crate::fragment::{FragmentBuffer, Push};
use crate::frame::FrameHead;
use crate::mask::Mask;

const MAX_HANDSHAKE_BYTES: usize = 16 * 1024;
const WS_MAX_MESSAGE: usize = 16 * 1024 * 1024;

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
        let mut tmp = Vec::with_capacity(14 + payload.len());
        FrameHead::encode_header(&mut tmp, opcode, payload.len(), false);
        tmp.extend_from_slice(payload);
        self.put_raw(&tmp)
    }

    fn handle_control(&mut self, opcode: u8, fin: bool, payload: &[u8]) {
        if !fin || payload.len() > 125 {
            self.close_after = true;
            return;
        }
        match opcode {
            0x9 => {
                let _ = self.frame(0xA, payload);
            }
            0xA => {}
            0x8 => {
                let _ = self.close(payload);
            }
            _ => self.close_after = true,
        }
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
}

impl<H: Handler> App<H> {
    pub fn new(user: H, expected_path: &'static str, max_frame_payload: usize) -> Self {
        Self {
            user,
            expected_path,
            max_frame_payload,
        }
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
        let bytes = chunk.as_slice();
        if slot.state.conn.phase == Phase::Closed {
            return Outcome::Ok;
        }
        slot.state.conn.acc.extend_from_slice(bytes);
        let send_ud = slot.token();
        let write_buf = aux.write_buf_for(slot);
        let (written, close_after) = {
            let mut response = Response::new(&mut *write_buf);
            let state = &mut slot.state.conn;
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
        slot.submit_buffered(write_buf, written, send_ud, driver);
        Outcome::Ok
    }

    fn on_send(
        &mut self,
        _slot: &mut Slot<Identity, listener::State<ConnState>>,
        _sent: usize,
        _aux: &mut listener::Aux,
        _driver: &mut Driver,
    ) {
    }

    fn on_close(
        &mut self,
        _slot: &mut Slot<Identity, listener::State<ConnState>>,
        _aux: &mut listener::Aux,
    ) {
    }
}

impl<H: Handler> App<H> {
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
        let mut lines = head.split("\r\n");
        let request = lines.next().unwrap_or("");
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
        for line in lines {
            if line.is_empty() {
                continue;
            }
            if let Some((name, value)) = line.split_once(':') {
                let name = name.trim();
                let value = value.trim();
                if name.eq_ignore_ascii_case("upgrade") {
                    upgrade = Some(value);
                } else if name.eq_ignore_ascii_case("connection") {
                    connection = Some(value);
                } else if name.eq_ignore_ascii_case("sec-websocket-version") {
                    ws_version = Some(value);
                } else if name.eq_ignore_ascii_case("sec-websocket-key") {
                    key = Some(value);
                }
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
        let mut pos = 0;
        loop {
            let head = match FrameHead::parse(&state.acc, pos, self.max_frame_payload) {
                Ok(Some(h)) => h,
                Ok(None) => break,
                Err(_) => {
                    state.phase = Phase::Closed;
                    response.close_after = true;
                    break;
                }
            };
            let fin = head.fin;
            let opcode = head.opcode;
            let (start, end) = (head.payload_start, head.payload_end);
            let Some(mask) = head.mask else {
                state.phase = Phase::Closed;
                response.close_after = true;
                break;
            };
            Mask::unmask_inline(&mut state.acc[start..end], mask);
            pos = end;

            if opcode >= 0x8 {
                response.handle_control(opcode, fin, &state.acc[start..end]);
                if opcode == 0x8 {
                    state.phase = Phase::Closed;
                    break;
                }
                continue;
            }

            match state.fragments.push(opcode, fin, &state.acc[start..end]) {
                Ok(Push::Direct(op, payload)) => self.dispatch(op, payload, response),
                Ok(Push::Assembled(op, payload)) => self.dispatch(op, &payload, response),
                Ok(Push::NeedMore) => {}
                Err(_) => {
                    state.phase = Phase::Closed;
                    response.close_after = true;
                    break;
                }
            }
        }
        state.acc.drain(..pos);
    }

    fn dispatch(&self, opcode: u8, payload: &[u8], response: &mut Response<'_>) {
        match opcode {
            0x1 => match std::str::from_utf8(payload) {
                Ok(s) => self.user.on_message(Message::Text(s), response),
                Err(_) => response.close_after = true,
            },
            0x2 => self.user.on_message(Message::Binary(payload), response),
            _ => response.close_after = true,
        }
    }
}
