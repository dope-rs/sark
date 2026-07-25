use std::collections::{BTreeSet, HashMap};
use std::mem;

use dope_quic::{ConnHandle, Handler, StreamError, StreamEvent};
use sark::dispatch::{BodyPlan, BodySource, Decode, PreparedRequest, ResponseEncoder};
use sark::sark_core::http::{Field, StatusCode};
use sark::service::BodyPolicy;

use crate::{
    Conn, ConnError, ErrorCode, Event, Payload, Role, StreamId, StreamTransport, pump_stream_event,
    pump_writes,
};

#[derive(Debug)]
pub enum Error {
    QuicStream(StreamError),
    H3(ConnError),
}

impl From<StreamError> for Error {
    fn from(err: StreamError) -> Self {
        Self::QuicStream(err)
    }
}

impl From<ConnError> for Error {
    fn from(err: ConnError) -> Self {
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
    type SendError = StreamError;

    fn recv_stream(&mut self, stream_id: u64) -> Option<Vec<u8>> {
        self.conn.stream_recv_owned(stream_id)
    }

    fn recv_stream_finished(&self, stream_id: u64) -> bool {
        self.conn.stream_recv_eof(stream_id)
    }

    fn send_stream(&mut self, stream_id: u64, bytes: &[u8]) -> Result<(), Self::SendError> {
        self.conn.stream_send(stream_id, bytes)
    }

    fn finish_stream(&mut self, stream_id: u64) -> Result<(), Self::SendError> {
        self.conn.stream_send_fin(stream_id)
    }
}

pub struct Session {
    h3: Conn,
    fin_pumped: BTreeSet<u64>,
    control_stream_id: Option<u64>,
}

impl Session {
    pub fn new() -> Self {
        Self::with_role(Role::Client)
    }

    pub fn with_role(role: Role) -> Self {
        Self {
            h3: Conn::with_role(role),
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
        self.flush(quic)?;
        Ok(stream_id)
    }

    pub fn open_request_stream(&mut self, quic: &mut dope_quic::Conn) -> Result<u64, Error> {
        Ok(quic.open_bidi_stream()?)
    }

    pub fn quic_stream_event(
        &mut self,
        quic: &mut dope_quic::Conn,
        event: StreamEvent,
    ) -> Result<(), Error> {
        let stream_id = match event {
            StreamEvent::Data { stream_id } | StreamEvent::Finished { stream_id } => stream_id,
            StreamEvent::Reset {
                stream_id,
                error_code,
            } => {
                self.h3.ingest_reset(StreamId::new(stream_id), error_code);
                self.fin_pumped.insert(stream_id);
                return Ok(());
            }
            StreamEvent::Stopped {
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
        self.flush(transport.conn)?;
        Ok(())
    }

    pub fn flush(&mut self, quic: &mut dope_quic::Conn) -> Result<(), Error> {
        let mut transport = QuicTransport::new(quic);
        pump_writes(&mut self.h3, &mut transport).map_err(Error::QuicStream)
    }

    pub fn poll_event(&mut self) -> Option<Event> {
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
    conn: &'a mut Conn,
    stream_id: StreamId,
    ok: bool,
}

impl<'a> H3Encoder<'a> {
    pub fn new(conn: &'a mut Conn, stream_id: StreamId) -> Self {
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

impl ResponseEncoder for H3Encoder<'_> {
    fn emit(&mut self, status: StatusCode, headers_wire: &[u8], body: &[u8]) {
        let status_str = status.as_str();
        let mut fields: Vec<Field> = Vec::with_capacity(8);
        fields.push(Field::new(b":status", status_str.as_bytes()));
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
                fields.push(Field::new(name, value));
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

const DEFAULT_MAX_BODY_CHUNKS: usize = 4096;

/// Resource bounds for the built-in HTTP/3 server adapter.
#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    /// Maximum number of retained DATA payloads per buffered request.
    pub max_body_chunks: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_body_chunks: DEFAULT_MAX_BODY_CHUNKS,
        }
    }
}

struct Pending<P> {
    prepared: P,
    plan: BodyPlan,
    body: PendingBody,
}

impl<P: PreparedRequest> Pending<P> {
    fn new(prepared: P) -> Result<Self, BodyError> {
        let plan = prepared.body_plan();
        if plan
            .content_length
            .is_some_and(|content_length| content_length > plan.max_body)
        {
            return Err(BodyError::TooLarge);
        }
        Ok(Self {
            prepared,
            plan,
            body: PendingBody::new(plan.policy),
        })
    }

    fn push(&mut self, data: Payload, max_chunks: usize) -> Result<(), BodyError> {
        let next_len = self
            .body
            .len()
            .checked_add(data.len())
            .ok_or(BodyError::TooLarge)?;
        if self
            .plan
            .content_length
            .is_some_and(|content_length| next_len > content_length)
        {
            return Err(BodyError::LengthMismatch);
        }
        self.body.push(data, self.plan.max_body, max_chunks)
    }

    fn content_length_matches(&self) -> bool {
        self.plan
            .content_length
            .is_none_or(|declared| declared == self.body.len())
    }
}

enum PendingBody {
    Discarded {
        len: usize,
    },
    Empty,
    Single(Payload),
    Segmented {
        first: Payload,
        rest: Vec<Payload>,
        len: usize,
    },
}

#[derive(Debug)]
enum BodyError {
    TooLarge,
    TooManyChunks,
    LengthMismatch,
}

fn body_error_code(error: BodyError) -> ErrorCode {
    match error {
        BodyError::TooLarge | BodyError::TooManyChunks => ErrorCode::ExcessiveLoad,
        BodyError::LengthMismatch => ErrorCode::Message,
    }
}

impl PendingBody {
    fn new(policy: BodyPolicy) -> Self {
        match policy {
            BodyPolicy::Discarded => Self::Discarded { len: 0 },
            BodyPolicy::Buffered => Self::Empty,
        }
    }

    fn push(&mut self, data: Payload, max_body: usize, max_chunks: usize) -> Result<(), BodyError> {
        let next_len = self
            .len()
            .checked_add(data.len())
            .ok_or(BodyError::TooLarge)?;
        if next_len > max_body {
            return Err(BodyError::TooLarge);
        }
        match self {
            Self::Discarded { len } => *len = next_len,
            Self::Empty if data.is_empty() => {}
            Self::Empty => {
                if max_chunks == 0 {
                    return Err(BodyError::TooManyChunks);
                }
                *self = Self::Single(data);
            }
            Self::Single(_) if data.is_empty() => {}
            Self::Single(first) => {
                if max_chunks < 2 {
                    return Err(BodyError::TooManyChunks);
                }
                let first = mem::take(first);
                let mut rest = Vec::with_capacity(3.min(max_chunks - 1));
                rest.push(data);
                *self = Self::Segmented {
                    first,
                    rest,
                    len: next_len,
                };
            }
            Self::Segmented { .. } if data.is_empty() => {}
            Self::Segmented { rest, len, .. } => {
                if rest.len() >= max_chunks.saturating_sub(1) {
                    return Err(BodyError::TooManyChunks);
                }
                rest.push(data);
                *len = next_len;
            }
        }
        Ok(())
    }

    fn len(&self) -> usize {
        match self {
            Self::Discarded { len } | Self::Segmented { len, .. } => *len,
            Self::Empty => 0,
            Self::Single(body) => body.len(),
        }
    }
}

impl BodySource for PendingBody {
    fn body_len(&self) -> usize {
        Self::len(self)
    }

    fn contiguous(&mut self) -> &[u8] {
        if let Self::Segmented { first, rest, len } = self {
            let first = mem::take(first);
            let mut joined = first.into_vec_with_capacity(*len);
            for chunk in mem::take(rest) {
                joined.extend_from_slice(chunk.as_slice());
            }
            *self = Self::Single(Payload::from(joined));
        }
        match self {
            Self::Discarded { .. } | Self::Empty => &[],
            Self::Single(body) => body.as_slice(),
            Self::Segmented { .. } => unreachable!("segmented body materialized above"),
        }
    }
}

struct ServerSession<P> {
    h3: Session,
    pending: HashMap<u64, Pending<P>>,
}

pub struct Server<R: Decode> {
    router: R,
    config: ServerConfig,
    sessions: HashMap<ConnHandle, ServerSession<R::Prepared>>,
}

impl<R: Decode> Server<R> {
    pub fn new(router: R) -> Self {
        Self::with_config(router, ServerConfig::default())
    }

    pub fn with_config(router: R, config: ServerConfig) -> Self {
        Self {
            router,
            config,
            sessions: HashMap::new(),
        }
    }

    pub fn router(&self) -> &R {
        &self.router
    }
}

impl<R: Decode> Server<R> {
    fn respond(router: &R, h3: &mut Session, stream_id: StreamId, pending: Pending<R::Prepared>) {
        let mut encoder = H3Encoder::new(h3.h3_mut(), stream_id);
        let _ = router.dispatch_prepared(pending.prepared, pending.body, &mut encoder);
    }
}

impl<R: Decode> Handler for Server<R> {
    fn established(&mut self, conn: &mut dope_quic::Conn, handle: ConnHandle) {
        let mut h3 = Session::with_role(Role::Server);
        if h3.start_control_stream(conn).is_err() {
            conn.close(ErrorCode::Internal as u64, Vec::new());
            return;
        }
        self.sessions.insert(
            handle,
            ServerSession {
                h3,
                pending: HashMap::new(),
            },
        );
    }

    fn stream_event(&mut self, conn: &mut dope_quic::Conn, handle: ConnHandle, event: StreamEvent) {
        let Self {
            router,
            config,
            sessions,
        } = self;
        let Some(session) = sessions.get_mut(&handle) else {
            return;
        };
        if session.h3.quic_stream_event(conn, event).is_err() {
            conn.close(ErrorCode::Internal as u64, Vec::new());
            return;
        }
        while let Some(event) = session.h3.poll_event() {
            match event {
                Event::Headers {
                    stream_id,
                    fields,
                    trailing,
                } => {
                    if !trailing && let Ok(prepared) = router.prepare_decoded(fields) {
                        match Pending::new(prepared) {
                            Ok(pending) => {
                                session.pending.insert(stream_id.0, pending);
                            }
                            Err(error) => {
                                conn.close(body_error_code(error) as u64, Vec::new());
                                return;
                            }
                        }
                    }
                }
                Event::Data { stream_id, data } => {
                    if let Some(pending) = session.pending.get_mut(&stream_id.0)
                        && let Err(error) = pending.push(data, config.max_body_chunks)
                    {
                        conn.close(body_error_code(error) as u64, Vec::new());
                        return;
                    }
                }
                Event::Finished { stream_id } => {
                    if let Some(pending) = session.pending.remove(&stream_id.0) {
                        if !pending.content_length_matches() {
                            conn.close(ErrorCode::Message as u64, Vec::new());
                            return;
                        }
                        Self::respond(router, &mut session.h3, stream_id, pending);
                    }
                }
                _ => {}
            }
        }
        if session.h3.flush(conn).is_err() {
            conn.close(ErrorCode::Internal as u64, Vec::new());
        }
    }

    fn close(&mut self, handle: ConnHandle) {
        self.sessions.remove(&handle);
    }
}

#[cfg(test)]
mod tests {
    use super::{BodyError, Pending, PendingBody};
    use crate::Payload;
    use sark::dispatch::{BodyPlan, BodySource, PreparedRequest};
    use sark::service::BodyPolicy;

    #[test]
    fn pending_body_keeps_a_single_data_allocation() {
        let bytes = b"single-frame-body".to_vec();
        let allocation = bytes.as_ptr();
        let mut body = PendingBody::new(BodyPolicy::Buffered);

        body.push(Payload::from(bytes), usize::MAX, usize::MAX)
            .unwrap();

        let body = body.contiguous();
        assert_eq!(body, b"single-frame-body");
        assert_eq!(body.as_ptr(), allocation);
    }

    #[test]
    fn segmented_body_defers_join_and_reuses_first_unique_allocation() {
        let mut first = Vec::with_capacity(16);
        first.extend_from_slice(b"abc");
        let second = b"def".to_vec();
        let first_allocation = first.as_ptr();
        let second_allocation = second.as_ptr();
        let mut body = PendingBody::new(BodyPolicy::Buffered);

        body.push(Payload::from(first), usize::MAX, usize::MAX)
            .unwrap();
        body.push(Payload::from(second), usize::MAX, usize::MAX)
            .unwrap();

        let PendingBody::Segmented { first, rest, len } = &body else {
            panic!("two non-empty chunks must remain segmented");
        };
        assert_eq!(*len, 6);
        assert_eq!(first.as_slice().as_ptr(), first_allocation);
        assert_eq!(rest[0].as_slice().as_ptr(), second_allocation);

        let contiguous = body.contiguous();
        assert_eq!(contiguous, b"abcdef");
        assert_eq!(contiguous.as_ptr(), first_allocation);
        assert!(matches!(body, PendingBody::Single(_)));
    }

    #[test]
    fn discarded_body_tracks_length_without_retaining_chunks() {
        let mut body = PendingBody::new(BodyPolicy::Discarded);

        body.push(Payload::from(b"abc".to_vec()), usize::MAX, 0)
            .unwrap();
        body.push(Payload::from(b"def".to_vec()), usize::MAX, 0)
            .unwrap();

        assert!(matches!(body, PendingBody::Discarded { len: 6 }));
        assert_eq!(body.contiguous(), b"");
    }

    #[test]
    fn body_limits_are_enforced_before_storage_grows() {
        let mut too_large = PendingBody::new(BodyPolicy::Buffered);
        assert!(matches!(
            too_large.push(Payload::from(b"abcd".to_vec()), 3, usize::MAX),
            Err(BodyError::TooLarge)
        ));
        assert!(matches!(too_large, PendingBody::Empty));

        let mut too_fragmented = PendingBody::new(BodyPolicy::Buffered);
        too_fragmented
            .push(Payload::from(b"a".to_vec()), usize::MAX, 1)
            .unwrap();
        assert!(matches!(
            too_fragmented.push(Payload::from(b"b".to_vec()), usize::MAX, 1),
            Err(BodyError::TooManyChunks)
        ));
        assert!(matches!(too_fragmented, PendingBody::Single(_)));
    }

    struct Prepared(BodyPlan);

    impl PreparedRequest for Prepared {
        fn body_plan(&self) -> BodyPlan {
            self.0
        }
    }

    #[test]
    fn declared_length_is_an_early_storage_bound() {
        let oversized = Prepared(BodyPlan {
            policy: BodyPolicy::Buffered,
            max_body: 3,
            content_length: Some(4),
        });
        assert!(matches!(Pending::new(oversized), Err(BodyError::TooLarge)));

        let prepared = Prepared(BodyPlan {
            policy: BodyPolicy::Buffered,
            max_body: 4,
            content_length: Some(3),
        });
        let mut pending = Pending::new(prepared).expect("valid declared length");
        assert!(matches!(
            pending.push(Payload::from(b"abcd".to_vec()), usize::MAX),
            Err(BodyError::LengthMismatch)
        ));
        assert!(matches!(pending.body, PendingBody::Empty));
    }
}
