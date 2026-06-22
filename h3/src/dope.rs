use std::collections::{BTreeSet, HashMap};

use crate::{Event, Role, StreamId, StreamTransport, pump_stream_event, pump_writes};

#[derive(Debug)]
pub enum Error {
    QuicStream(dope_quic::StreamError),
    H3(crate::ConnError),
}

impl From<dope_quic::StreamError> for Error {
    fn from(err: dope_quic::StreamError) -> Self {
        Self::QuicStream(err)
    }
}

impl From<crate::ConnError> for Error {
    fn from(err: crate::ConnError) -> Self {
        Self::H3(err)
    }
}

pub struct QuicTransport<'a> {
    conn: &'a mut dope_quic::Conn,
}

impl<'a> QuicTransport<'a> {
    pub fn new(conn: &'a mut dope_quic::Conn) -> Self {
        Self { conn }
    }
}

impl StreamTransport for QuicTransport<'_> {
    fn recv_stream(&mut self, stream_id: u64, out: &mut Vec<u8>) -> usize {
        self.conn.stream_recv(stream_id, out)
    }

    fn recv_stream_finished(&self, stream_id: u64) -> bool {
        self.conn.stream_recv_eof(stream_id)
    }

    fn send_stream(&mut self, stream_id: u64, bytes: &[u8]) {
        self.conn.stream_send(stream_id, bytes);
    }

    fn finish_stream(&mut self, stream_id: u64) {
        self.conn.stream_send_fin(stream_id);
    }
}

pub struct Session {
    h3: crate::Conn,
    fin_pumped: BTreeSet<u64>,
    control_stream_id: Option<u64>,
}

impl Session {
    pub fn new() -> Self {
        Self::with_role(crate::Role::Client)
    }

    pub fn with_role(role: crate::Role) -> Self {
        Self {
            h3: crate::Conn::with_role(role),
            fin_pumped: BTreeSet::new(),
            control_stream_id: None,
        }
    }

    pub fn h3(&self) -> &crate::Conn {
        &self.h3
    }

    pub fn h3_mut(&mut self) -> &mut crate::Conn {
        &mut self.h3
    }

    pub fn start_control_stream(&mut self, quic: &mut dope_quic::Conn) -> Result<u64, Error> {
        let stream_id = quic.open_uni_stream()?;
        self.h3.start_control_stream(StreamId::new(stream_id))?;
        self.control_stream_id = Some(stream_id);
        self.flush(quic);
        Ok(stream_id)
    }

    pub fn open_request_stream(&mut self, quic: &mut dope_quic::Conn) -> Result<u64, Error> {
        Ok(quic.open_bidi_stream()?)
    }

    pub fn on_quic_stream_event(
        &mut self,
        quic: &mut dope_quic::Conn,
        event: dope_quic::StreamEvent,
    ) -> Result<(), Error> {
        let stream_id = match event {
            dope_quic::StreamEvent::Data { stream_id }
            | dope_quic::StreamEvent::Finished { stream_id } => stream_id,
            dope_quic::StreamEvent::Reset {
                stream_id,
                error_code,
            } => {
                self.h3.ingest_reset(StreamId::new(stream_id), error_code);
                self.fin_pumped.insert(stream_id);
                return Ok(());
            }
            dope_quic::StreamEvent::Stopped {
                stream_id,
                error_code,
            } => {
                self.h3.ingest_stopped(StreamId::new(stream_id), error_code);
                return Ok(());
            }
        };
        if self.fin_pumped.contains(&stream_id) {
            return Ok(());
        }
        let mut transport = QuicTransport::new(quic);
        pump_stream_event(&mut self.h3, &mut transport, stream_id)?;
        if transport.conn.stream_recv_eof(stream_id) {
            self.fin_pumped.insert(stream_id);
        }
        self.flush(transport.conn);
        Ok(())
    }

    pub fn flush(&mut self, quic: &mut dope_quic::Conn) {
        let mut transport = QuicTransport::new(quic);
        pump_writes(&mut self.h3, &mut transport);
    }

    pub fn poll_event(&mut self) -> Option<crate::Event> {
        self.h3.poll_event()
    }

    pub fn control_stream_id(&self) -> Option<u64> {
        self.control_stream_id
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

pub struct H3Encoder<'a> {
    conn: &'a mut crate::Conn,
    stream_id: StreamId,
    ok: bool,
}

impl<'a> H3Encoder<'a> {
    pub fn new(conn: &'a mut crate::Conn, stream_id: StreamId) -> Self {
        Self {
            conn,
            stream_id,
            ok: true,
        }
    }

    pub fn ok(&self) -> bool {
        self.ok
    }
}

impl sark::dispatch::ResponseEncoder for H3Encoder<'_> {
    fn emit(
        &mut self,
        status: sark::sark_core::http::StatusCode,
        headers_wire: &[u8],
        body: &[u8],
    ) {
        let status_str = status.as_str();
        let mut fields: Vec<sark::sark_core::http::Field> = Vec::with_capacity(8);
        fields.push(sark::sark_core::http::Field::new(
            b":status",
            status_str.as_bytes(),
        ));
        for line in headers_wire.split(|&b| b == b'\n') {
            let line = match line.strip_suffix(b"\r") {
                Some(stripped) => stripped,
                None => line,
            };
            if line.is_empty() {
                continue;
            }
            if let Some(colon) = line.iter().position(|&b| b == b':') {
                let name = &line[..colon];
                let mut value = &line[colon + 1..];
                while let Some((&b' ', rest)) = value.split_first() {
                    value = rest;
                }
                fields.push(sark::sark_core::http::Field::new(name, value));
            }
        }
        if self
            .conn
            .send_headers(self.stream_id, fields, false)
            .is_err()
        {
            self.ok = false;
            return;
        }
        if self.conn.send_data(self.stream_id, body, true).is_err() {
            self.ok = false;
        }
    }
}

#[derive(Default)]
struct Pending {
    fields: Option<Vec<sark::sark_core::http::OwnedField>>,
    body: Vec<u8>,
}

struct ServerSession {
    h3: Session,
    pending: HashMap<u64, Pending>,
}

pub struct Server<R> {
    router: R,
    sessions: HashMap<dope_quic::ConnHandle, ServerSession>,
}

impl<R> Server<R> {
    pub fn new(router: R) -> Self {
        Self {
            router,
            sessions: HashMap::new(),
        }
    }

    pub fn router(&self) -> &R {
        &self.router
    }
}

impl<R: sark::dispatch::Decode> Server<R> {
    fn respond(router: &R, h3: &mut Session, stream_id: StreamId, pending: Pending) {
        let Some(fields) = pending.fields else {
            return;
        };
        let mut method: Option<sark::sark_core::http::Method> = None;
        let mut path: Option<Vec<u8>> = None;
        let mut head: Vec<u8> = Vec::new();
        let mut pairs: Vec<(&[u8], core::ops::Range<usize>)> = Vec::new();
        for field in &fields {
            let name = field.name.as_slice();
            if name == b":method" {
                method = sark::sark_core::http::Method::from_bytes(&field.value).ok();
            } else if name == b":path" {
                path = Some(field.value.clone());
            } else if name.first() != Some(&b':') {
                let start = head.len();
                head.extend_from_slice(&field.value);
                pairs.push((name, start..head.len()));
            }
        }
        let (Some(method), Some(path)) = (method, path) else {
            return;
        };
        let mut encoder = H3Encoder::new(h3.h3_mut(), stream_id);
        let _ = router.dispatch_decoded(method, &path, &pairs, &head, &pending.body, &mut encoder);
    }
}

impl<R: sark::dispatch::Decode + 'static> dope_quic::Handler for Server<R> {
    fn on_established(&mut self, conn: &mut dope_quic::Conn, handle: dope_quic::ConnHandle) {
        let mut h3 = Session::with_role(Role::Server);
        let _ = h3.start_control_stream(conn);
        self.sessions.insert(
            handle,
            ServerSession {
                h3,
                pending: HashMap::new(),
            },
        );
    }

    fn on_stream_event(
        &mut self,
        conn: &mut dope_quic::Conn,
        handle: dope_quic::ConnHandle,
        event: dope_quic::StreamEvent,
    ) {
        let Self { router, sessions } = self;
        let Some(session) = sessions.get_mut(&handle) else {
            return;
        };
        if session.h3.on_quic_stream_event(conn, event).is_err() {
            return;
        }
        while let Some(event) = session.h3.poll_event() {
            match event {
                Event::Headers {
                    stream_id, fields, ..
                } => {
                    session.pending.entry(stream_id.0).or_default().fields = Some(fields);
                }
                Event::Data { stream_id, data } => {
                    session
                        .pending
                        .entry(stream_id.0)
                        .or_default()
                        .body
                        .extend_from_slice(&data);
                }
                Event::Finished { stream_id } => {
                    if let Some(pending) = session.pending.remove(&stream_id.0) {
                        Self::respond(router, &mut session.h3, stream_id, pending);
                    }
                }
                _ => {}
            }
        }
        session.h3.flush(conn);
    }

    fn on_close(&mut self, handle: dope_quic::ConnHandle) {
        self.sessions.remove(&handle);
    }
}
