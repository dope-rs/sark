use o3::buffer::{Bytes, Pooled, Retained, SharedPool};
use o3::collections::{FixedHashTable, FixedQueue, Slab, SlabKey};
use sark_h2::{ClientRole, Conn, ConnError, ErrorCode, StreamId, conn};

use crate::Codec;
use crate::frame::{DataChunk, Deframer, FrameError, MessageFrame};
use crate::headers::{HeaderBlock, ResponseHead};
use crate::metadata::Metadata;
use crate::status::{Code, Status};

#[derive(Clone, Debug)]
pub struct Config {
    pub max_in_flight: usize,
    pub max_completed: usize,
    pub max_events: usize,
    pub max_pending_msgs: usize,
    pub max_pending_len: usize,
    pub max_message_len: usize,
    pub max_buffered_len: usize,
    pub max_buffered_msgs: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_in_flight: 256,
            max_completed: 256,
            max_events: 8192,
            max_pending_msgs: 4,
            max_pending_len: 4 * 1024 * 1024 + 5,
            max_message_len: 4 * 1024 * 1024,
            max_buffered_len: 16 * 1024 * 1024,
            max_buffered_msgs: 8192,
        }
    }
}

#[derive(Debug)]
pub struct UnaryResult {
    pub stream_id: StreamId,
    pub metadata: Metadata,
    pub message: Option<MessageFrame>,
    pub trailers: Metadata,
    pub status: Status,
}

impl UnaryResult {
    pub fn into_single_payload(self) -> Result<Bytes<Retained>, Status> {
        if self.status.code() != Code::Ok {
            return Err(self.status);
        }
        self.message
            .map(|message| message.payload)
            .ok_or_else(|| Status::new(Code::Internal, "unary response needs one message"))
    }

    pub fn decode_single<C: Codec>(self, codec: &mut C) -> Result<C::Decode, Status> {
        let payload = self.into_single_payload()?;
        codec.decode(payload.as_slice())
    }
}

#[derive(Debug)]
pub enum StreamEvent {
    Headers {
        stream_id: StreamId,
        metadata: Metadata,
    },
    Message {
        stream_id: StreamId,
        message: MessageFrame,
    },
    Trailers {
        stream_id: StreamId,
        metadata: Metadata,
        status: Status,
    },
}

#[derive(Clone, Debug)]
pub enum TypedStreamEvent<T> {
    Headers {
        stream_id: StreamId,
        metadata: Metadata,
    },
    Message {
        stream_id: StreamId,
        message: T,
    },
    Trailers {
        stream_id: StreamId,
        metadata: Metadata,
        status: Status,
    },
}

impl StreamEvent {
    pub fn stream_id(&self) -> StreamId {
        match self {
            Self::Headers { stream_id, .. }
            | Self::Message { stream_id, .. }
            | Self::Trailers { stream_id, .. } => *stream_id,
        }
    }

    pub fn decode_message<C: Codec>(&self, codec: &mut C) -> Option<Result<C::Decode, Status>> {
        let Self::Message { message, .. } = self else {
            return None;
        };
        if message.compressed {
            return Some(Err(Status::new(
                Code::Unimplemented,
                "compressed messages are not supported",
            )));
        }
        Some(codec.decode(message.payload.as_slice()))
    }

    pub fn decode<C: Codec>(self, codec: &mut C) -> Result<TypedStreamEvent<C::Decode>, Status> {
        Ok(match self {
            Self::Headers {
                stream_id,
                metadata,
            } => TypedStreamEvent::Headers {
                stream_id,
                metadata,
            },
            Self::Message { stream_id, message } => {
                if message.compressed {
                    return Err(Status::new(
                        Code::Unimplemented,
                        "compressed messages are not supported",
                    ));
                }
                TypedStreamEvent::Message {
                    stream_id,
                    message: codec.decode(message.payload.as_slice())?,
                }
            }
            Self::Trailers {
                stream_id,
                metadata,
                status,
            } => TypedStreamEvent::Trailers {
                stream_id,
                metadata,
                status,
            },
        })
    }
}

struct ResponseState {
    mode: ResponseMode,
    metadata: Metadata,
    deframer: Deframer,
    message: Option<MessageFrame>,
    buffered_len: usize,
    message_count: usize,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ResponseMode {
    Unary,
    Streaming,
}

enum PendingRequestTag {}

type PendingRequestKey = SlabKey<PendingRequestTag>;

struct PendingRequest {
    bytes: Pooled,
    pos: usize,
    end_stream: bool,
    next: Option<PendingRequestKey>,
}

struct StreamRecord {
    stream_id: StreamId,
    response: ResponseState,
    request_head: Option<PendingRequestKey>,
    request_tail: Option<PendingRequestKey>,
    request_queued: bool,
    send_closed: bool,
}

pub struct Session {
    h2: Conn<ClientRole>,
    config: Config,
    streams: FixedHashTable<StreamRecord>,
    pending: FixedQueue<StreamId>,
    requests: Slab<PendingRequest, PendingRequestTag>,
    request_pool: SharedPool,
    message_pool: SharedPool,
    complete: FixedQueue<UnaryResult>,
    events: FixedQueue<StreamEvent>,
    encode_buf: Vec<u8>,
}

impl Session {
    pub fn new() -> Self {
        Self::with_config(Config::default())
    }

    pub fn with_config(config: Config) -> Self {
        assert!(config.max_in_flight > 0, "max_in_flight must be positive");
        assert!(
            config.max_pending_msgs > 0,
            "max_pending_msgs must be positive"
        );
        assert!(
            config.max_pending_len > 0,
            "max_pending_len must be positive"
        );
        let h2 = Conn::<ClientRole>::with_config(conn::Config {
            stream_capacity: config.max_in_flight,
            ..conn::Config::default()
        });
        Self {
            h2,
            streams: FixedHashTable::with_capacity(config.max_in_flight),
            pending: FixedQueue::with_capacity(config.max_in_flight),
            requests: Slab::with_capacity(config.max_pending_msgs),
            request_pool: SharedPool::new(config.max_pending_msgs, config.max_pending_len),
            message_pool: SharedPool::new(config.max_pending_msgs, config.max_message_len.max(1)),
            complete: FixedQueue::with_capacity(config.max_completed),
            events: FixedQueue::with_capacity(config.max_events),
            encode_buf: Vec::with_capacity(config.max_message_len),
            config,
        }
    }

    pub fn outbound(&self) -> &[u8] {
        self.h2.outbound()
    }

    pub fn drain_outbound(&mut self, n: usize) {
        self.h2.drain_outbound(n);
    }

    pub fn ingest(&mut self, bytes: &[u8]) -> Result<(), Status> {
        let result = self.h2.ingest(bytes);
        self.drive_inbound(result)
    }

    pub fn resume(&mut self) -> Result<(), Status> {
        let result = self.h2.resume();
        self.drive_inbound(result)
    }

    pub fn start_unary_raw(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        payload: &[u8],
    ) -> Result<StreamId, Status> {
        let stream_id =
            self.start_response_stream(path, authority, metadata, ResponseMode::Unary)?;
        if let Err(status) = self.send_message_bytes(stream_id, payload, true) {
            self.abort_stream(stream_id);
            return Err(status);
        }
        Ok(stream_id)
    }

    pub fn start_stream_raw(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
    ) -> Result<StreamId, Status> {
        self.start_response_stream(path, authority, metadata, ResponseMode::Streaming)
    }

    pub fn start_client_stream_raw(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
    ) -> Result<StreamId, Status> {
        self.start_response_stream(path, authority, metadata, ResponseMode::Unary)
    }

    fn start_response_stream(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        mode: ResponseMode,
    ) -> Result<StreamId, Status> {
        let headers = HeaderBlock::for_request(path, authority, metadata)?;
        let h2_headers = headers.as_h2();
        let stream_id = self
            .h2
            .start_request(&h2_headers, false)
            .map_err(Status::from_conn_err)?;
        let record = StreamRecord {
            stream_id,
            response: ResponseState {
                mode,
                metadata: Metadata::new(),
                deframer: Deframer::new(self.config.max_message_len),
                message: None,
                buffered_len: 0,
                message_count: 0,
            },
            request_head: None,
            request_tail: None,
            request_queued: false,
            send_closed: false,
        };
        if self
            .streams
            .try_insert(Self::stream_hash(stream_id), record, |record| {
                record.stream_id == stream_id
            })
            .is_err()
        {
            let _ = self.h2.reset_stream(stream_id, ErrorCode::RefusedStream);
            return Err(Self::resource_exhausted("too many in-flight streams"));
        }
        Ok(stream_id)
    }

    pub fn send_message_raw(&mut self, stream_id: StreamId, payload: &[u8]) -> Result<(), Status> {
        self.send_message_bytes(stream_id, payload, false)
    }

    pub fn finish_send(&mut self, stream_id: StreamId) -> Result<(), Status> {
        let tail = {
            let record = self
                .record_mut(stream_id)
                .ok_or_else(|| Status::new(Code::Internal, "request for unknown gRPC stream"))?;
            if record.send_closed {
                return Ok(());
            }
            record.send_closed = true;
            record.request_tail
        };
        if let Some(tail) = tail {
            self.requests.get_mut(tail).unwrap().end_stream = true;
            self.drive_pending_requests();
            return Ok(());
        }
        self.drive_pending_requests();
        self.h2
            .send_data(stream_id, &[], true)
            .map_err(Status::from_conn_err)
            .map(|_| ())
    }

    pub fn start_streaming_raw<I, B>(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        payloads: I,
    ) -> Result<StreamId, Status>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let stream_id = self.start_stream_raw(path, authority, metadata)?;
        let mut payloads = payloads.into_iter().peekable();
        if payloads.peek().is_none() {
            if let Err(status) = self.finish_send(stream_id) {
                self.abort_stream(stream_id);
                return Err(status);
            }
            return Ok(stream_id);
        }
        while let Some(payload) = payloads.next() {
            let end_stream = payloads.peek().is_none();
            if let Err(status) = self.send_message_bytes(stream_id, payload.as_ref(), end_stream) {
                self.abort_stream(stream_id);
                return Err(status);
            }
        }
        Ok(stream_id)
    }

    pub fn start_unary<C: Codec>(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<StreamId, Status> {
        let mut payload = core::mem::take(&mut self.encode_buf);
        payload.clear();
        let result = codec
            .encode(message, &mut payload)
            .and_then(|()| self.start_unary_raw(path, authority, metadata, &payload));
        self.encode_buf = payload;
        result
    }

    pub fn start_streaming<C: Codec>(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        codec: &mut C,
        messages: &[C::Encode],
    ) -> Result<StreamId, Status> {
        let stream_id = self.start_stream_raw(path, authority, metadata)?;
        let mut payload = core::mem::take(&mut self.encode_buf);
        for (index, message) in messages.iter().enumerate() {
            payload.clear();
            if let Err(status) = codec.encode(message, &mut payload) {
                self.encode_buf = payload;
                self.abort_stream(stream_id);
                return Err(status);
            }
            let end_stream = index + 1 == messages.len();
            if let Err(status) = self.send_message_bytes(stream_id, &payload, end_stream) {
                self.encode_buf = payload;
                self.abort_stream(stream_id);
                return Err(status);
            }
        }
        self.encode_buf = payload;
        if messages.is_empty()
            && let Err(status) = self.finish_send(stream_id)
        {
            self.abort_stream(stream_id);
            return Err(status);
        }
        Ok(stream_id)
    }

    pub fn send_message<C: Codec>(
        &mut self,
        stream_id: StreamId,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<(), Status> {
        let mut payload = core::mem::take(&mut self.encode_buf);
        payload.clear();
        let result = codec
            .encode(message, &mut payload)
            .and_then(|()| self.send_message_raw(stream_id, &payload));
        self.encode_buf = payload;
        result
    }

    pub fn poll_unary(&mut self) -> Option<UnaryResult> {
        self.complete.pop_front()
    }

    pub fn poll_event(&mut self) -> Option<StreamEvent> {
        self.events.pop_front()
    }

    fn stream_hash(stream_id: StreamId) -> u64 {
        u64::from(stream_id.0)
    }

    fn record(&self, stream_id: StreamId) -> Option<&StreamRecord> {
        self.streams.get(Self::stream_hash(stream_id), |record| {
            record.stream_id == stream_id
        })
    }

    fn record_mut(&mut self, stream_id: StreamId) -> Option<&mut StreamRecord> {
        self.streams
            .get_mut(Self::stream_hash(stream_id), |record| {
                record.stream_id == stream_id
            })
    }

    fn remove_stream(&mut self, stream_id: StreamId) -> Option<StreamRecord> {
        let record = self
            .streams
            .remove(Self::stream_hash(stream_id), |record| {
                record.stream_id == stream_id
            })?;
        if record.request_queued {
            self.pending.retain(|pending| *pending != stream_id);
        }
        self.clear_requests(record.request_head);
        Some(record)
    }

    fn resource_exhausted(message: &'static str) -> Status {
        Status::new(Code::ResourceExhausted, message)
    }

    fn drive_inbound(&mut self, mut result: Result<(), ConnError>) -> Result<(), Status> {
        loop {
            let drained = self.drain_events()?;
            self.drive_pending_requests();
            match result {
                Ok(()) => return Ok(()),
                Err(ConnError::Overload) if drained != 0 => result = self.h2.resume(),
                Err(error) => return Err(Status::from_conn_err(error)),
            }
        }
    }

    fn drain_events(&mut self) -> Result<usize, Status> {
        let mut drained = 0;
        while let Some(event) = self.h2.poll_event() {
            drained += 1;
            match event {
                conn::Event::Headers {
                    stream_id,
                    headers,
                    end_stream,
                    trailing,
                } if trailing => {
                    let fields = HeaderBlock::from_h2(&headers);
                    let (status, trailers) = Status::parse_h2_trailers(&fields)?;
                    self.finish_stream(stream_id, status, trailers)?;
                    if !end_stream {
                        return Err(Status::new(Code::Internal, "trailers without END_STREAM"));
                    }
                }
                conn::Event::Headers {
                    stream_id,
                    headers,
                    end_stream,
                    ..
                } => {
                    let fields = HeaderBlock::from_h2(&headers);
                    let head = ResponseHead::parse_h2(&fields)?;
                    let metadata = head.metadata;
                    let mode = self.response_mut(stream_id)?.mode;
                    match mode {
                        ResponseMode::Unary => {
                            self.response_mut(stream_id)?.metadata = metadata;
                        }
                        ResponseMode::Streaming => self
                            .events
                            .vacant_entry()
                            .ok_or_else(|| Self::resource_exhausted("event queue is full"))?
                            .push_back(StreamEvent::Headers {
                                stream_id,
                                metadata,
                            }),
                    }
                    if end_stream {
                        let (status, trailers) = Status::parse_h2_trailers(&fields)?;
                        self.finish_stream(stream_id, status, trailers)?;
                    }
                }
                conn::Event::Data {
                    stream_id,
                    data,
                    end_stream,
                } => {
                    let max_buffered_len = self.config.max_buffered_len;
                    let max_buffered_msgs = self.config.max_buffered_msgs;
                    let mut data = DataChunk::new(data);
                    while !data.is_empty() {
                        let message = {
                            let response = self
                                .streams
                                .get_mut(Self::stream_hash(stream_id), |record| {
                                    record.stream_id == stream_id
                                })
                                .map(|record| &mut record.response)
                                .ok_or_else(|| {
                                    Status::new(Code::Internal, "event for unknown gRPC stream")
                                })?;
                            response
                                .deframer
                                .next(&mut data, &self.message_pool)
                                .map_err(Status::from_frame_err)?
                        };
                        let Some(message) = message else {
                            continue;
                        };
                        let mode = {
                            let stream = self.response_mut(stream_id)?;
                            stream.buffered_len =
                                stream.buffered_len.saturating_add(message.payload.len());
                            stream.message_count += 1;
                            if stream.buffered_len > max_buffered_len
                                || stream.message_count > max_buffered_msgs
                            {
                                return Err(Status::new(
                                    Code::ResourceExhausted,
                                    "stream buffer limit exceeded",
                                ));
                            }
                            stream.mode
                        };
                        match mode {
                            ResponseMode::Unary => {
                                let response = self.response_mut(stream_id)?;
                                if response.message.replace(message).is_some() {
                                    return Err(Status::new(
                                        Code::Internal,
                                        "unary response has multiple messages",
                                    ));
                                }
                            }
                            ResponseMode::Streaming => self
                                .events
                                .vacant_entry()
                                .ok_or_else(|| Self::resource_exhausted("event queue is full"))?
                                .push_back(StreamEvent::Message { stream_id, message }),
                        }
                    }
                    if end_stream {
                        self.abort_stream(stream_id);
                        return Err(Status::new(Code::Internal, "missing grpc-status"));
                    }
                }
                conn::Event::StreamReset { stream_id, error } => {
                    let mode = self
                        .record(stream_id)
                        .map(|record| record.response.mode)
                        .ok_or_else(|| Status::new(Code::Internal, "reset for unknown stream"))?;
                    match mode {
                        ResponseMode::Unary if self.complete.is_full() => {
                            return Err(Self::resource_exhausted("result queue is full"));
                        }
                        ResponseMode::Streaming if self.events.is_full() => {
                            return Err(Self::resource_exhausted("event queue is full"));
                        }
                        _ => {}
                    }
                    let stream = self.remove_stream(stream_id).unwrap();
                    let status = Status::from_reset_err(error);
                    match mode {
                        ResponseMode::Unary => {
                            self.complete
                                .vacant_entry()
                                .unwrap()
                                .push_back(UnaryResult {
                                    stream_id,
                                    metadata: stream.response.metadata,
                                    message: stream.response.message,
                                    trailers: Metadata::new(),
                                    status,
                                })
                        }
                        ResponseMode::Streaming => {
                            self.events
                                .vacant_entry()
                                .unwrap()
                                .push_back(StreamEvent::Trailers {
                                    stream_id,
                                    metadata: Metadata::new(),
                                    status,
                                })
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(drained)
    }

    fn response_mut(&mut self, stream_id: StreamId) -> Result<&mut ResponseState, Status> {
        self.record_mut(stream_id)
            .map(|record| &mut record.response)
            .ok_or_else(|| Status::new(Code::Internal, "event for unknown gRPC stream"))
    }

    fn finish_stream(
        &mut self,
        stream_id: StreamId,
        status: Status,
        trailers: Metadata,
    ) -> Result<(), Status> {
        let mode = self
            .record(stream_id)
            .map(|record| record.response.mode)
            .ok_or_else(|| Status::new(Code::Internal, "trailers for unknown gRPC stream"))?;
        match mode {
            ResponseMode::Unary if self.complete.is_full() => {
                return Err(Self::resource_exhausted("result queue is full"));
            }
            ResponseMode::Streaming if self.events.is_full() => {
                return Err(Self::resource_exhausted("event queue is full"));
            }
            _ => {}
        }
        let stream = self
            .remove_stream(stream_id)
            .ok_or_else(|| Status::new(Code::Internal, "trailers for unknown gRPC stream"))?;
        match mode {
            ResponseMode::Unary => self
                .complete
                .vacant_entry()
                .unwrap()
                .push_back(UnaryResult {
                    stream_id,
                    metadata: stream.response.metadata,
                    message: stream.response.message,
                    trailers,
                    status,
                }),
            ResponseMode::Streaming => {
                self.events
                    .vacant_entry()
                    .unwrap()
                    .push_back(StreamEvent::Trailers {
                        stream_id,
                        metadata: trailers,
                        status,
                    })
            }
        }
        Ok(())
    }

    fn send_message_bytes(
        &mut self,
        stream_id: StreamId,
        payload: &[u8],
        end_stream: bool,
    ) -> Result<(), Status> {
        if payload.len() > self.config.max_message_len {
            return Err(Self::resource_exhausted("gRPC message is too large"));
        }
        let header = MessageFrame::header(false, payload.len()).map_err(Status::from_frame_err)?;
        let record = self
            .record(stream_id)
            .ok_or_else(|| Status::new(Code::Internal, "request for unknown gRPC stream"))?;
        if record.send_closed {
            return Err(Status::new(Code::Internal, "request stream is closed"));
        }
        if record.request_head.is_some() {
            self.enqueue_request(stream_id, &header, payload, 0, end_stream)?;
            if end_stream {
                self.record_mut(stream_id).unwrap().send_closed = true;
            }
            self.drive_pending_requests();
            return Ok(());
        }

        let mut consumed = match self
            .h2
            .send_data_parts(stream_id, &header, payload, end_stream)
            .map_err(Status::from_conn_err)
        {
            Ok(n) => n,
            Err(status) => {
                self.remove_stream(stream_id);
                return Err(status);
            }
        };
        let total = header.len() + payload.len();
        while consumed < total && consumed >= header.len() {
            let payload_pos = consumed - header.len();
            let n = match self
                .h2
                .send_data(stream_id, &payload[payload_pos..], end_stream)
                .map_err(Status::from_conn_err)
            {
                Ok(n) => n,
                Err(status) => {
                    self.remove_stream(stream_id);
                    return Err(status);
                }
            };
            if n == 0 {
                break;
            }
            consumed += n;
        }
        if consumed != total
            && let Err(status) =
                self.enqueue_request(stream_id, &header, payload, consumed, end_stream)
        {
            let _ = self.h2.reset_stream(stream_id, ErrorCode::Cancel);
            self.remove_stream(stream_id);
            return Err(status);
        }
        if end_stream {
            self.record_mut(stream_id).unwrap().send_closed = true;
        }
        Ok(())
    }

    fn enqueue_request(
        &mut self,
        stream_id: StreamId,
        header: &[u8; 5],
        payload: &[u8],
        consumed: usize,
        end_stream: bool,
    ) -> Result<(), Status> {
        let total = header.len() + payload.len();
        let additional = total - consumed;
        if additional > self.request_pool.capacity() {
            return Err(Self::resource_exhausted("pending request is too large"));
        }
        let (tail, queued) = self
            .record(stream_id)
            .map(|record| (record.request_tail, record.request_queued))
            .ok_or_else(|| Status::new(Code::Internal, "request for unknown gRPC stream"))?;
        let mut lease = self
            .request_pool
            .try_acquire()
            .ok_or_else(|| Self::resource_exhausted("pending request messages are full"))?;
        {
            let mut writer = lease.spare_writer();
            if consumed < header.len() {
                writer
                    .try_extend_from_slice(&header[consumed..])
                    .map_err(|_| Self::resource_exhausted("pending request is too large"))?;
                writer
                    .try_extend_from_slice(payload)
                    .map_err(|_| Self::resource_exhausted("pending request is too large"))?;
            } else {
                writer
                    .try_extend_from_slice(&payload[consumed - header.len()..])
                    .map_err(|_| Self::resource_exhausted("pending request is too large"))?;
            }
        }
        let request = PendingRequest {
            bytes: lease.freeze(),
            pos: 0,
            end_stream,
            next: None,
        };
        let key = self
            .requests
            .insert(request)
            .map_err(|_| Self::resource_exhausted("pending request messages are full"))?;
        if let Some(tail) = tail {
            self.requests.get_mut(tail).unwrap().next = Some(key);
        }
        let record = self.record_mut(stream_id).unwrap();
        if record.request_head.is_none() {
            record.request_head = Some(key);
        }
        record.request_tail = Some(key);
        if !queued {
            self.record_mut(stream_id).unwrap().request_queued = true;
            self.pending.vacant_entry().unwrap().push_back(stream_id);
        }
        Ok(())
    }

    fn drive_pending_requests(&mut self) {
        let len = self.pending.len();
        for _ in 0..len {
            let stream_id = self.pending.pop_front().unwrap();
            let Some(mut key) = self.record_mut(stream_id).and_then(|record| {
                record.request_queued = false;
                record.request_head
            }) else {
                continue;
            };
            loop {
                let state = {
                    let request = self.requests.get_mut(key).unwrap();
                    request.drive(&mut self.h2, stream_id)
                };
                match state {
                    Ok(RequestDrive::Complete) => {
                        let request = self.requests.remove(key).unwrap();
                        let next = request.next;
                        let Some(record) = self.record_mut(stream_id) else {
                            self.clear_requests(next);
                            break;
                        };
                        record.request_head = next;
                        if next.is_none() {
                            record.request_tail = None;
                            break;
                        }
                        key = next.unwrap();
                    }
                    Ok(RequestDrive::Blocked) => {
                        if let Some(record) = self.record_mut(stream_id) {
                            record.request_queued = true;
                            self.pending.vacant_entry().unwrap().push_back(stream_id);
                        }
                        break;
                    }
                    Err(_) => {
                        self.remove_stream(stream_id);
                        break;
                    }
                }
            }
        }
    }

    fn clear_requests(&mut self, mut key: Option<PendingRequestKey>) {
        while let Some(current) = key {
            let request = self.requests.remove(current).unwrap();
            key = request.next;
        }
    }

    fn abort_stream(&mut self, stream_id: StreamId) {
        if self.record(stream_id).is_some() {
            let _ = self.h2.reset_stream(stream_id, ErrorCode::Cancel);
            self.remove_stream(stream_id);
        }
    }
}

enum RequestDrive {
    Complete,
    Blocked,
}

impl PendingRequest {
    fn drive(
        &mut self,
        conn: &mut Conn<ClientRole>,
        stream_id: StreamId,
    ) -> Result<RequestDrive, ConnError> {
        while self.pos < self.bytes.len() {
            let n = conn.send_data(
                stream_id,
                &self.bytes.as_slice()[self.pos..],
                self.end_stream,
            )?;
            if n == 0 {
                return Ok(RequestDrive::Blocked);
            }
            self.pos += n;
        }
        Ok(RequestDrive::Complete)
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl Status {
    pub fn from_conn_err(err: ConnError) -> Status {
        let code = match err {
            ConnError::StreamGoneAway | ConnError::GoAwayReceived(_) => Code::Unavailable,
            ConnError::StreamLimit => Code::ResourceExhausted,
            ConnError::FlowControl => Code::ResourceExhausted,
            ConnError::Overload => Code::ResourceExhausted,
            ConnError::StreamClosed => Code::Unavailable,
            _ => Code::Internal,
        };
        Status::new(code, format!("HTTP/2 error: {err:?}"))
    }

    pub fn from_frame_err(err: FrameError) -> Status {
        match err {
            FrameError::BadCompressionFlag(_) => {
                Status::new(Code::InvalidArgument, "bad gRPC compression flag")
            }
            FrameError::MessageTooLarge { .. } => {
                Status::new(Code::ResourceExhausted, "gRPC message too large")
            }
            FrameError::LengthOverflow => {
                Status::new(Code::Internal, "gRPC message length overflow")
            }
            FrameError::Capacity => {
                Status::new(Code::ResourceExhausted, "gRPC message pool is full")
            }
        }
    }

    pub fn from_reset_err(error: ErrorCode) -> Status {
        match error {
            ErrorCode::Cancel => Status::new(Code::Cancelled, "HTTP/2 stream cancelled"),
            ErrorCode::RefusedStream => Status::new(Code::Unavailable, "HTTP/2 stream refused"),
            _ => Status::new(Code::Internal, format!("HTTP/2 stream reset: {error:?}")),
        }
    }
}
