use std::collections::{BTreeMap, VecDeque};

use sark_h2::{ClientRole, Conn, ConnError, ErrorCode, StreamId, conn};

use crate::Codec;
use crate::frame::{Deframer, FrameError, MessageFrame};
use crate::headers::{HeaderBlock, ResponseHead};
use crate::metadata::Metadata;
use crate::status::{Code, Status};

#[derive(Clone, Debug)]
pub struct Config {
    pub max_message_len: usize,
    pub max_buffered_len: usize,
    pub max_buffered_msgs: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_message_len: 4 * 1024 * 1024,
            max_buffered_len: 16 * 1024 * 1024,
            max_buffered_msgs: 8192,
        }
    }
}

#[derive(Clone, Debug)]
pub struct UnaryResult {
    pub stream_id: StreamId,
    pub metadata: Metadata,
    pub messages: Vec<MessageFrame>,
    pub trailers: Metadata,
    pub status: Status,
}

impl UnaryResult {
    pub fn into_single_payload(self) -> Result<Vec<u8>, Status> {
        if self.status.code() != Code::Ok {
            return Err(self.status);
        }
        let mut messages = self.messages;
        if messages.len() != 1 {
            return Err(Status::new(
                Code::Internal,
                "unary response needs one message",
            ));
        }
        Ok(messages.pop().unwrap().payload)
    }

    pub fn decode_single<C: Codec>(self, codec: &mut C) -> Result<C::Decode, Status> {
        let payload = self.into_single_payload()?;
        codec.decode(&payload)
    }

    pub fn decode_messages<C: Codec>(self, codec: &mut C) -> Result<Vec<C::Decode>, Status> {
        if self.status.code() != Code::Ok {
            return Err(self.status);
        }
        let mut decoded = Vec::with_capacity(self.messages.len());
        for message in self.messages {
            decoded.push(codec.decode(&message.payload)?);
        }
        Ok(decoded)
    }
}

#[derive(Clone, Debug)]
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
        Some(codec.decode(&message.payload))
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
                    message: codec.decode(&message.payload)?,
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

#[derive(Clone, Debug)]
struct ResponseState {
    metadata: Metadata,
    deframer: Deframer,
    messages: Vec<MessageFrame>,
    buffered_len: usize,
}

#[derive(Clone, Debug)]
struct PendingRequest {
    bytes: Vec<u8>,
    pos: usize,
    end_stream: bool,
}

pub struct Session {
    h2: Conn<ClientRole>,
    config: Config,
    streams: BTreeMap<StreamId, ResponseState>,
    pending: BTreeMap<StreamId, PendingRequest>,
    complete: VecDeque<UnaryResult>,
    events: VecDeque<StreamEvent>,
}

impl Session {
    pub fn new() -> Self {
        Self::with_config(Config::default())
    }

    pub fn with_config(config: Config) -> Self {
        Self {
            h2: Conn::<ClientRole>::new(),
            config,
            streams: BTreeMap::new(),
            pending: BTreeMap::new(),
            complete: VecDeque::new(),
            events: VecDeque::new(),
        }
    }

    pub fn outbound(&self) -> &[u8] {
        self.h2.outbound()
    }

    pub fn drain_outbound(&mut self, n: usize) {
        self.h2.drain_outbound(n);
    }

    pub fn ingest(&mut self, bytes: &[u8]) -> Result<(), Status> {
        self.h2.ingest(bytes).map_err(Status::from_conn_err)?;
        self.drain_events()?;
        self.drive_pending_requests();
        Ok(())
    }

    pub fn start_unary_raw(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        payload: &[u8],
    ) -> Result<StreamId, Status> {
        self.start_streaming_raw(path, authority, metadata, [payload])
    }

    pub fn start_stream_raw(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
    ) -> Result<StreamId, Status> {
        let headers = HeaderBlock::for_request(path, authority, metadata)?;
        let h2_headers = headers.as_h2();
        let stream_id = self
            .h2
            .start_request(&h2_headers, false)
            .map_err(Status::from_conn_err)?;
        self.streams.insert(
            stream_id,
            ResponseState {
                metadata: Metadata::new(),
                deframer: Deframer::new(self.config.max_message_len),
                messages: Vec::new(),
                buffered_len: 0,
            },
        );
        Ok(stream_id)
    }

    pub fn send_message_raw(&mut self, stream_id: StreamId, payload: &[u8]) -> Result<(), Status> {
        let mut framed = Vec::new();
        MessageFrame::encode(false, payload, &mut framed).map_err(Status::from_frame_err)?;
        self.queue_request_bytes(stream_id, framed, false);
        self.drive_pending_requests();
        Ok(())
    }

    pub fn finish_send(&mut self, stream_id: StreamId) -> Result<(), Status> {
        if let Some(pending) = self.pending.get_mut(&stream_id) {
            pending.end_stream = true;
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
        let mut framed = Vec::new();
        for payload in payloads {
            MessageFrame::encode(false, payload.as_ref(), &mut framed)
                .map_err(Status::from_frame_err)?;
        }
        let empty = framed.is_empty();
        self.queue_request_bytes(stream_id, framed, true);
        self.drive_pending_requests();
        if empty && !self.pending.contains_key(&stream_id) {
            self.finish_send(stream_id)?;
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
        let mut payload = Vec::new();
        codec.encode(message, &mut payload)?;
        self.start_unary_raw(path, authority, metadata, &payload)
    }

    pub fn start_streaming<C: Codec>(
        &mut self,
        path: &[u8],
        authority: Option<&[u8]>,
        metadata: &Metadata,
        codec: &mut C,
        messages: &[C::Encode],
    ) -> Result<StreamId, Status> {
        let mut payloads = Vec::with_capacity(messages.len());
        for message in messages {
            let mut payload = Vec::new();
            codec.encode(message, &mut payload)?;
            payloads.push(payload);
        }
        self.start_streaming_raw(path, authority, metadata, &payloads)
    }

    pub fn send_message<C: Codec>(
        &mut self,
        stream_id: StreamId,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<(), Status> {
        let mut payload = Vec::new();
        codec.encode(message, &mut payload)?;
        self.send_message_raw(stream_id, &payload)
    }

    pub fn poll_unary(&mut self) -> Option<UnaryResult> {
        self.complete.pop_front()
    }

    pub fn poll_event(&mut self) -> Option<StreamEvent> {
        self.events.pop_front()
    }

    fn drain_events(&mut self) -> Result<(), Status> {
        while let Some(event) = self.h2.poll_event() {
            match event {
                conn::Event::Headers {
                    stream_id,
                    headers,
                    end_stream,
                    trailing,
                } if trailing => {
                    let fields = HeaderBlock::from_h2_owned(headers);
                    let (status, trailers) = Status::parse_h2_trailers(&fields)?;
                    self.finish_stream(stream_id, status, trailers, true)?;
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
                    let fields = HeaderBlock::from_h2_owned(headers);
                    let head = ResponseHead::parse_h2(&fields)?;
                    let metadata = head.metadata;
                    let stream = self.stream_mut(stream_id)?;
                    stream.metadata = metadata.clone();
                    self.events.push_back(StreamEvent::Headers {
                        stream_id,
                        metadata,
                    });
                    if end_stream {
                        let (status, trailers) = Status::parse_h2_trailers(&fields)?;
                        self.finish_stream(stream_id, status, trailers, true)?;
                    }
                }
                conn::Event::Data {
                    stream_id, data, ..
                } => {
                    let max_buffered_len = self.config.max_buffered_len;
                    let max_buffered_msgs = self.config.max_buffered_msgs;
                    let stream = self.stream_mut(stream_id)?;
                    let mut messages = Vec::new();
                    stream
                        .deframer
                        .push(&data, &mut messages)
                        .map_err(Status::from_frame_err)?;
                    let added: usize = messages.iter().map(|message| message.payload.len()).sum();
                    stream.buffered_len = stream.buffered_len.saturating_add(added);
                    if stream.buffered_len > max_buffered_len
                        || stream.messages.len() + messages.len() > max_buffered_msgs
                    {
                        return Err(Status::new(
                            Code::ResourceExhausted,
                            "stream buffer limit exceeded",
                        ));
                    }
                    stream.messages.extend(messages.iter().cloned());
                    self.events.extend(
                        messages
                            .into_iter()
                            .map(|message| StreamEvent::Message { stream_id, message }),
                    );
                }
                conn::Event::StreamReset { stream_id, error } => {
                    self.streams.remove(&stream_id);
                    let status = Status::from_reset_err(error);
                    self.events.push_back(StreamEvent::Trailers {
                        stream_id,
                        metadata: Metadata::new(),
                        status: status.clone(),
                    });
                    self.complete.push_back(UnaryResult {
                        stream_id,
                        metadata: Metadata::new(),
                        messages: Vec::new(),
                        trailers: Metadata::new(),
                        status,
                    });
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn stream_mut(&mut self, stream_id: StreamId) -> Result<&mut ResponseState, Status> {
        self.streams
            .get_mut(&stream_id)
            .ok_or_else(|| Status::new(Code::Internal, "event for unknown gRPC stream"))
    }

    fn finish_stream(
        &mut self,
        stream_id: StreamId,
        status: Status,
        trailers: Metadata,
        emit_event: bool,
    ) -> Result<(), Status> {
        let stream = self
            .streams
            .remove(&stream_id)
            .ok_or_else(|| Status::new(Code::Internal, "trailers for unknown gRPC stream"))?;
        if emit_event {
            self.events.push_back(StreamEvent::Trailers {
                stream_id,
                metadata: trailers.clone(),
                status: status.clone(),
            });
        }
        self.complete.push_back(UnaryResult {
            stream_id,
            metadata: stream.metadata,
            messages: stream.messages,
            trailers,
            status,
        });
        Ok(())
    }

    fn drive_pending_requests(&mut self) {
        let ids: Vec<StreamId> = self.pending.keys().copied().collect();
        for stream_id in ids {
            let Some(mut pending) = self.pending.remove(&stream_id) else {
                continue;
            };
            loop {
                if pending.pos == pending.bytes.len() {
                    break;
                }
                let remaining = &pending.bytes[pending.pos..];
                let end_stream = pending.end_stream
                    && remaining.len() <= self.h2.peer_settings().max_frame_size as usize;
                let Ok(n) = self.h2.send_data(stream_id, remaining, end_stream) else {
                    self.streams.remove(&stream_id);
                    break;
                };
                if n == 0 {
                    self.pending.insert(stream_id, pending);
                    break;
                }
                pending.pos += n;
            }
        }
    }

    fn queue_request_bytes(&mut self, stream_id: StreamId, bytes: Vec<u8>, end_stream: bool) {
        if bytes.is_empty() {
            return;
        }
        match self.pending.get_mut(&stream_id) {
            Some(pending) => {
                pending.bytes.extend(bytes);
                pending.end_stream |= end_stream;
            }
            None => {
                self.pending.insert(
                    stream_id,
                    PendingRequest {
                        bytes,
                        pos: 0,
                        end_stream,
                    },
                );
            }
        }
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
