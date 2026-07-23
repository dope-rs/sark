use dope::runtime::profile::Throughput;
use o3::collections::{FixedHashTable, FixedQueue};

use super::{Body, Config};
use crate::conn::{self, Conn, ConnError};
use crate::frame::ErrorCode;
use crate::hpack::{Header, HeaderBlock, OwnedHeader};
use crate::role::ServerRole;
use crate::stream::StreamId;

type ServerProfile = Throughput;

#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub(super) max_request_body_bytes: usize,
    pub(super) max_connection_body_bytes: usize,
    pub(super) max_outbound_bytes: usize,
}

impl From<Config> for Limits {
    fn from(config: Config) -> Self {
        Self {
            max_request_body_bytes: config.max_request_body_bytes,
            max_connection_body_bytes: config.max_connection_body_bytes,
            max_outbound_bytes: config.max_outbound_bytes,
        }
    }
}

pub struct Request {
    pub headers: HeaderBlock,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &[u8]) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|header| header.name == name)
            .map(|header| header.value)
    }

    pub fn path(&self) -> Option<&[u8]> {
        self.header(b":path")
    }
}

enum ResponseHeaders {
    Owned(Vec<OwnedHeader>),
    Static(&'static [Header<'static>]),
}

pub struct Response {
    headers: ResponseHeaders,
    body: Body,
    pub trailers: Vec<OwnedHeader>,
}

impl Response {
    pub fn new(headers: Vec<OwnedHeader>, body: impl Into<Body>) -> Self {
        Self {
            headers: ResponseHeaders::Owned(headers),
            body: body.into(),
            trailers: Vec::new(),
        }
    }

    pub fn from_static(headers: &'static [Header<'static>], body: impl Into<Body>) -> Self {
        Self {
            headers: ResponseHeaders::Static(headers),
            body: body.into(),
            trailers: Vec::new(),
        }
    }

    pub fn text(body: impl Into<Body>) -> Self {
        const HEADERS: &[Header<'static>] = &[
            Header::new(b":status", b"200"),
            Header::new(b"content-type", b"text/plain; charset=utf-8"),
        ];
        Self::from_static(HEADERS, body)
    }

    pub fn body(&self) -> &Body {
        &self.body
    }
}

struct Incoming {
    stream_id: StreamId,
    headers: HeaderBlock,
    body: Vec<u8>,
}

impl From<Incoming> for Request {
    fn from(incoming: Incoming) -> Self {
        Self {
            headers: incoming.headers,
            body: incoming.body,
        }
    }
}

struct PendingBody {
    stream_id: StreamId,
    body: Body,
    offset: usize,
    stalled: bool,
    trailers: Vec<OwnedHeader>,
    trailers_sent: bool,
}

impl PendingBody {
    fn emit_trailers(&mut self, connection: &mut Conn<ServerRole>) -> Result<(), ConnError> {
        if self.trailers.is_empty() || self.trailers_sent {
            return Ok(());
        }
        connection.send_trailers_fields(
            self.stream_id,
            self.trailers.iter().map(OwnedHeader::as_ref),
        )?;
        self.trailers_sent = true;
        Ok(())
    }

    fn pump(
        &mut self,
        connection: &mut Conn<ServerRole>,
        max_outbound_bytes: usize,
    ) -> Result<bool, ConnError> {
        loop {
            if self.offset >= self.body.len() {
                self.emit_trailers(connection)?;
                return Ok(true);
            }
            if connection.outbound().len() >= max_outbound_bytes {
                self.stalled = false;
                return Ok(false);
            }
            let remaining = &self.body.as_slice()[self.offset..];
            let end_stream = self.trailers.is_empty();
            let written = connection.send_data(self.stream_id, remaining, end_stream)?;
            if written == 0 {
                self.stalled = true;
                return Ok(false);
            }
            self.offset += written;
        }
    }
}

pub(super) enum Dispatch {
    Response(Response),
    Pending,
    Reset(ErrorCode),
}

pub(super) trait EventSink {
    fn request(&mut self, stream_id: StreamId, request: Request) -> Dispatch;
    fn reset(&mut self, stream_id: StreamId);
}

pub(super) struct ConnectionState {
    pub(super) connection: Conn<ServerRole>,
    incoming: FixedHashTable<Incoming>,
    pending: FixedQueue<PendingBody>,
    buffered_body_bytes: usize,
}

impl Default for ConnectionState {
    fn default() -> Self {
        let connection = Conn::<ServerRole>::with_tuning::<ServerProfile>();
        let capacity = connection
            .local_settings()
            .max_concurrent_streams
            .map_or(1, |capacity| capacity.max(1)) as usize;
        Self {
            connection,
            incoming: FixedHashTable::with_capacity(capacity),
            pending: FixedQueue::with_capacity(capacity),
            buffered_body_bytes: 0,
        }
    }
}

impl ConnectionState {
    fn incoming(&self, stream_id: StreamId) -> Option<&Incoming> {
        self.incoming
            .get(u64::from(stream_id.0), |entry| entry.stream_id == stream_id)
    }

    fn incoming_mut(&mut self, stream_id: StreamId) -> Option<&mut Incoming> {
        self.incoming
            .get_mut(u64::from(stream_id.0), |entry| entry.stream_id == stream_id)
    }

    fn insert_incoming(&mut self, incoming: Incoming) -> bool {
        let stream_id = incoming.stream_id;
        self.incoming
            .try_insert(u64::from(stream_id.0), incoming, |entry| {
                entry.stream_id == stream_id
            })
            .is_ok()
    }

    fn take_incoming(&mut self, stream_id: StreamId) -> Option<Incoming> {
        let incoming = self
            .incoming
            .remove(u64::from(stream_id.0), |entry| entry.stream_id == stream_id)?;
        self.buffered_body_bytes = self.buffered_body_bytes.saturating_sub(incoming.body.len());
        Some(incoming)
    }

    fn receive_data(
        &mut self,
        stream_id: StreamId,
        data: &[u8],
        end_stream: bool,
        limits: Limits,
    ) -> Option<Request> {
        let body_bytes = self
            .incoming(stream_id)?
            .body
            .len()
            .saturating_add(data.len());
        let connection_body_bytes = self.buffered_body_bytes.saturating_add(data.len());
        if body_bytes > limits.max_request_body_bytes
            || connection_body_bytes > limits.max_connection_body_bytes
        {
            self.take_incoming(stream_id);
            let _ = self
                .connection
                .reset_stream(stream_id, ErrorCode::EnhanceYourCalm);
            return None;
        }
        self.incoming_mut(stream_id)?.body.extend_from_slice(data);
        self.buffered_body_bytes = connection_body_bytes;
        end_stream
            .then(|| self.take_incoming(stream_id).map(Request::from))
            .flatten()
    }

    pub(super) fn begin_response(
        &mut self,
        stream_id: StreamId,
        response: Response,
        limits: Limits,
    ) {
        if !self.connection.has_stream(stream_id) {
            return;
        }
        let has_trailers = !response.trailers.is_empty();
        let end_stream = response.body.is_empty() && !has_trailers;
        let sent = match &response.headers {
            ResponseHeaders::Owned(headers) => self.connection.send_response(
                stream_id,
                headers.iter().map(OwnedHeader::as_ref),
                end_stream,
            ),
            ResponseHeaders::Static(headers) => {
                self.connection
                    .send_response(stream_id, headers.iter().copied(), end_stream)
            }
        };
        if sent.is_err() || end_stream {
            return;
        }
        let mut body = PendingBody {
            stream_id,
            body: response.body,
            offset: 0,
            stalled: false,
            trailers: response.trailers,
            trailers_sent: false,
        };
        if !matches!(
            body.pump(&mut self.connection, limits.max_outbound_bytes),
            Ok(true)
        ) && self.pending.push_back(body).is_err()
        {
            let _ = self
                .connection
                .reset_stream(stream_id, ErrorCode::EnhanceYourCalm);
        }
    }

    fn deliver<S: EventSink>(
        &mut self,
        sink: &mut S,
        stream_id: StreamId,
        request: Request,
        limits: Limits,
    ) {
        match sink.request(stream_id, request) {
            Dispatch::Response(response) => self.begin_response(stream_id, response, limits),
            Dispatch::Pending => {}
            Dispatch::Reset(error) => {
                let _ = self.connection.reset_stream(stream_id, error);
            }
        }
    }

    pub(super) fn drain_events<S: EventSink>(&mut self, limits: Limits, sink: &mut S) -> usize {
        let mut drained = 0;
        while let Some(event) = self.connection.poll_event() {
            drained += 1;
            match event {
                conn::Event::Headers {
                    stream_id,
                    headers,
                    end_stream,
                    trailing,
                } => {
                    if trailing {
                        if let Some(mut incoming) = self.take_incoming(stream_id) {
                            if incoming.headers.append(headers).is_err() {
                                let _ = self
                                    .connection
                                    .reset_stream(stream_id, ErrorCode::EnhanceYourCalm);
                                continue;
                            }
                            self.deliver(sink, stream_id, incoming.into(), limits);
                        }
                    } else if end_stream {
                        self.deliver(
                            sink,
                            stream_id,
                            Request {
                                headers,
                                body: Vec::new(),
                            },
                            limits,
                        );
                    } else if !self.insert_incoming(Incoming {
                        stream_id,
                        headers,
                        body: Vec::new(),
                    }) {
                        let _ = self
                            .connection
                            .reset_stream(stream_id, ErrorCode::EnhanceYourCalm);
                    }
                }
                conn::Event::Data {
                    stream_id,
                    data,
                    end_stream,
                } => {
                    if let Some(request) = self.receive_data(stream_id, &data, end_stream, limits) {
                        self.deliver(sink, stream_id, request, limits);
                    }
                }
                conn::Event::StreamReset { stream_id, .. } => {
                    self.reset_stream(stream_id);
                    sink.reset(stream_id);
                }
                _ => {}
            }
        }
        self.resume_pending(limits);
        drained
    }

    fn resume_pending(&mut self, limits: Limits) {
        if self.connection.take_window_opened() {
            self.pump_pending(limits, true);
        }
    }

    pub(super) fn pump_pending(&mut self, limits: Limits, resume: bool) {
        let len = self.pending.len();
        for _ in 0..len {
            let Some(mut body) = self.pending.pop_front() else {
                break;
            };
            if resume {
                body.stalled = false;
            }
            let pending = body.stalled
                || self.connection.outbound().len() >= limits.max_outbound_bytes
                || matches!(
                    body.pump(&mut self.connection, limits.max_outbound_bytes),
                    Ok(false)
                );
            if pending && self.pending.push_back(body).is_err() {
                debug_assert!(false, "pending response queue lost capacity");
                break;
            }
        }
    }

    fn reset_stream(&mut self, stream_id: StreamId) {
        self.take_incoming(stream_id);
        self.pending.retain(|body| body.stream_id != stream_id);
    }

    pub(super) fn close(&mut self) {
        self.pending.clear();
        self.incoming.clear();
        self.buffered_body_bytes = 0;
    }
}
