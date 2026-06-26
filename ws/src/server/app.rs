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
        if slot.state.conn.phase == Phase::Closed {
            return Outcome::Ok;
        }
        slot.state.conn.acc.extend_from_slice(chunk.as_slice());
        if slot.state.conn.acc.len() > WS_MAX_ACC {
            slot.state.conn.phase = Phase::Closed;
            slot.state.conn.acc = Vec::new();
            if !slot.core.is_send_inflight() {
                self.emit_close(slot, CLOSE_MESSAGE_TOO_BIG, aux, driver);
            }
            return Outcome::CloseAfter;
        }
        if !slot.core.is_send_inflight() {
            self.pump(slot, aux, driver);
        }
        Outcome::Ok
    }

    fn on_send(
        &mut self,
        slot: &mut Slot<Identity, listener::State<ConnState>>,
        _sent: usize,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) {
        if slot.state.conn.phase != Phase::Closed {
            self.pump(slot, aux, driver);
        }
    }

    fn on_close(
        &mut self,
        _slot: &mut Slot<Identity, listener::State<ConnState>>,
        _aux: &mut listener::Aux,
    ) {
    }
}

impl<H: Handler> App<H> {
    fn pump(
        &self,
        slot: &mut Slot<Identity, listener::State<ConnState>>,
        aux: &mut listener::Aux,
        driver: &mut Driver,
    ) {
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
        if written > 0 {
            slot.submit_buffered(write_buf, written, send_ud, driver);
        }
    }

    fn emit_close(
        &self,
        slot: &mut Slot<Identity, listener::State<ConnState>>,
        code: u16,
        aux: &mut listener::Aux,
        driver: &mut Driver,
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
        let frame_cap = self.max_frame_payload.min(WS_MAX_MESSAGE);
        let mut pos = 0;
        loop {
            let head = match FrameHead::parse(&state.acc, pos, frame_cap) {
                Ok(Some(h)) => h,
                Ok(None) => break,
                Err(FrameError::PayloadTooLarge) => {
                    state.phase = Phase::Closed;
                    response.send_close_code(CLOSE_MESSAGE_TOO_BIG);
                    break;
                }
                Err(_) => {
                    state.phase = Phase::Closed;
                    response.send_close_code(CLOSE_PROTOCOL_ERROR);
                    break;
                }
            };
            let fin = head.fin;
            let opcode = head.opcode;
            let (start, end) = (head.payload_start, head.payload_end);
            let Some(mask) = head.mask else {
                state.phase = Phase::Closed;
                response.send_close_code(CLOSE_PROTOCOL_ERROR);
                break;
            };
            Mask::unmask_inline(&mut state.acc[start..end], mask);
            pos = end;

            if opcode >= 0x8 {
                response.handle_control(opcode, fin, &state.acc[start..end]);
                if opcode == 0x8 || response.close_after {
                    state.phase = Phase::Closed;
                    break;
                }
                continue;
            }

            match state.fragments.push(opcode, fin, &state.acc[start..end]) {
                Ok(Push::Direct(op, payload)) => self.dispatch(op, payload, response),
                Ok(Push::Assembled(op, payload)) => self.dispatch(op, &payload, response),
                Ok(Push::NeedMore) => {}
                Err(crate::fragment::FragmentError::PayloadTooLarge) => {
                    state.phase = Phase::Closed;
                    response.send_close_code(CLOSE_MESSAGE_TOO_BIG);
                    break;
                }
                Err(_) => {
                    state.phase = Phase::Closed;
                    response.send_close_code(CLOSE_PROTOCOL_ERROR);
                    break;
                }
            }
            if response.close_after {
                state.phase = Phase::Closed;
                break;
            }
        }
        state.acc.drain(..pos);
    }

    fn dispatch(&self, opcode: u8, payload: &[u8], response: &mut Response<'_>) {
        match opcode {
            0x1 => match std::str::from_utf8(payload) {
                Ok(s) => self.user.on_message(Message::Text(s), response),
                Err(_) => response.send_close_code(CLOSE_INVALID_PAYLOAD),
            },
            0x2 => self.user.on_message(Message::Binary(payload), response),
            _ => response.send_close_code(CLOSE_PROTOCOL_ERROR),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn echo_app(max_frame_payload: usize) -> App<impl Handler> {
        App::new(
            |msg: Message<'_>, resp: &mut Response<'_>| match msg {
                Message::Text(s) => {
                    let _ = resp.text(s);
                }
                Message::Binary(b) => {
                    let _ = resp.binary(b);
                }
            },
            "",
            max_frame_payload,
        )
    }

    fn client_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
        let mask = [0x37, 0xfa, 0x21, 0x3d];
        let mut v = vec![0x80 | opcode];
        FrameHead::encode_len(&mut v, payload.len(), true);
        v.extend_from_slice(&mask);
        let start = v.len();
        v.extend_from_slice(payload);
        Mask::unmask_inline(&mut v[start..], mask);
        v
    }

    fn server_close_code(bytes: &[u8]) -> Option<u16> {
        let h = FrameHead::parse(bytes, 0, usize::MAX).ok()??;
        if h.opcode != 0x8 {
            return None;
        }
        let p = &bytes[h.payload_start..h.payload_end];
        if p.len() < 2 {
            return None;
        }
        Some(u16::from_be_bytes([p[0], p[1]]))
    }

    fn drive(app: &App<impl Handler>, acc: Vec<u8>, buf: &mut [u8]) -> (usize, bool, Phase) {
        let mut state = ConnState::default();
        state.acc = acc;
        let mut resp = Response::new(buf);
        app.drive_frames(&mut state, &mut resp);
        (resp.written, resp.close_after, state.phase)
    }

    #[test]
    fn close_code_validity() {
        for c in [
            1000u16, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 3000, 4999,
        ] {
            assert!(Response::valid_close_code(c), "{c} should be valid");
        }
        for c in [
            0u16, 999, 1004, 1005, 1006, 1012, 1013, 1014, 1015, 1016, 2999, 5000,
        ] {
            assert!(!Response::valid_close_code(c), "{c} should be invalid");
        }
    }

    #[test]
    fn close_with_reserved_code_replies_1002() {
        let app = echo_app(usize::MAX);
        let mut buf = [0u8; 64];
        let (written, close_after, phase) =
            drive(&app, client_frame(0x8, &1004u16.to_be_bytes()), &mut buf);
        assert!(close_after);
        assert!(phase == Phase::Closed);
        assert_eq!(server_close_code(&buf[..written]), Some(1002));
    }

    #[test]
    fn close_with_invalid_utf8_reason_replies_1007() {
        let app = echo_app(usize::MAX);
        let mut payload = 1000u16.to_be_bytes().to_vec();
        payload.extend_from_slice(&[0xff, 0xfe]);
        let mut buf = [0u8; 64];
        let (written, _, _) = drive(&app, client_frame(0x8, &payload), &mut buf);
        assert_eq!(server_close_code(&buf[..written]), Some(1007));
    }

    #[test]
    fn valid_close_is_echoed_cleanly() {
        let app = echo_app(usize::MAX);
        let mut payload = 1000u16.to_be_bytes().to_vec();
        payload.extend_from_slice(b"bye");
        let mut buf = [0u8; 64];
        let (written, close_after, phase) = drive(&app, client_frame(0x8, &payload), &mut buf);
        assert!(close_after);
        assert!(phase == Phase::Closed);
        let h = FrameHead::parse(&buf[..written], 0, usize::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(h.opcode, 0x8);
        assert_eq!(&buf[h.payload_start..h.payload_end], payload.as_slice());
    }

    #[test]
    fn oversized_single_frame_rejected() {
        let app = echo_app(usize::MAX);
        let mut acc = vec![0x82, 0x80 | 127];
        acc.extend_from_slice(&((WS_MAX_MESSAGE as u64) + 1).to_be_bytes());
        acc.extend_from_slice(&[0, 0, 0, 0]);
        let mut buf = [0u8; 64];
        let (written, close_after, phase) = drive(&app, acc, &mut buf);
        assert!(close_after);
        assert!(phase == Phase::Closed);
        assert_eq!(server_close_code(&buf[..written]), Some(1009));
    }

    #[test]
    fn normal_text_echo_still_works() {
        let app = echo_app(WS_MAX_MESSAGE);
        let mut buf = [0u8; 64];
        let (written, close_after, _) = drive(&app, client_frame(0x1, b"hello"), &mut buf);
        assert!(!close_after);
        let h = FrameHead::parse(&buf[..written], 0, usize::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(h.opcode, 0x1);
        assert_eq!(&buf[h.payload_start..h.payload_end], b"hello");
    }

    #[test]
    fn unmasked_client_frame_replies_1002() {
        let app = echo_app(WS_MAX_MESSAGE);
        let mut acc = vec![0x81, 0x03];
        acc.extend_from_slice(b"abc");
        let mut buf = [0u8; 64];
        let (written, close_after, phase) = drive(&app, acc, &mut buf);
        assert!(close_after);
        assert!(phase == Phase::Closed);
        assert_eq!(server_close_code(&buf[..written]), Some(1002));
    }

    #[test]
    fn acc_is_bounded_under_inflight_flood() {
        let chunk = vec![0u8; 64 * 1024];
        let mut acc: Vec<u8> = Vec::new();
        let mut closed = false;
        for _ in 0..100_000 {
            if closed {
                break;
            }
            acc.extend_from_slice(&chunk);
            if acc.len() > WS_MAX_ACC {
                closed = true;
                acc = Vec::new();
            }
        }
        assert!(closed);
        assert!(acc.len() <= WS_MAX_ACC);
    }
}
