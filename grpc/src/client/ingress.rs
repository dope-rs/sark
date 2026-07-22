use o3::buffer::{Bytes, Retained, SharedPool};
use o3::collections::FixedQueue;
use sark_h2::{ClientRole, Conn, ConnError, StreamId, conn};

use crate::Codec;
use crate::frame::{DataChunk, MessageFrame};
use crate::headers::{HeaderBlock, ResponseHead};
use crate::metadata::Metadata;
use crate::status::{Code, Status};

use super::Config;
use super::call::{CallStore, ResponseMode, ResponseState};
use super::egress::Egress;

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

pub(super) struct Ingress {
    message_pool: SharedPool,
    complete: FixedQueue<UnaryResult>,
    events: FixedQueue<StreamEvent>,
    max_buffered_len: usize,
    max_buffered_msgs: usize,
}

impl Ingress {
    pub(super) fn with_config(config: &Config) -> Self {
        Self {
            message_pool: SharedPool::new(config.max_pending_msgs, config.max_message_len.max(1)),
            complete: FixedQueue::with_capacity(config.max_completed),
            events: FixedQueue::with_capacity(config.max_events),
            max_buffered_len: config.max_buffered_len,
            max_buffered_msgs: config.max_buffered_msgs,
        }
    }

    pub(super) fn poll_unary(&mut self) -> Option<UnaryResult> {
        self.complete.pop_front()
    }

    pub(super) fn poll_event(&mut self) -> Option<StreamEvent> {
        self.events.pop_front()
    }

    pub(super) fn drive(
        &mut self,
        h2: &mut Conn<ClientRole>,
        calls: &mut CallStore,
        egress: &mut Egress,
        mut result: Result<(), ConnError>,
    ) -> Result<(), Status> {
        loop {
            let drained = self.drain_events(h2, calls, egress)?;
            egress.drive(calls, h2);
            match result {
                Ok(()) => return Ok(()),
                Err(ConnError::Overload) if drained != 0 => result = h2.resume(),
                Err(error) => return Err(Status::from_conn_err(error)),
            }
        }
    }

    fn drain_events(
        &mut self,
        h2: &mut Conn<ClientRole>,
        calls: &mut CallStore,
        egress: &mut Egress,
    ) -> Result<usize, Status> {
        let mut drained = 0;
        while let Some(event) = h2.poll_event() {
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
                    self.finish_stream(calls, egress, stream_id, status, trailers)?;
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
                    let mode = self.response_mut(calls, stream_id)?.mode;
                    match mode {
                        ResponseMode::Unary => {
                            self.response_mut(calls, stream_id)?.metadata = metadata;
                        }
                        ResponseMode::Streaming => self.push_event(StreamEvent::Headers {
                            stream_id,
                            metadata,
                        })?,
                    }
                    if end_stream {
                        let (status, trailers) = Status::parse_h2_trailers(&fields)?;
                        self.finish_stream(calls, egress, stream_id, status, trailers)?;
                    }
                }
                conn::Event::Data {
                    stream_id,
                    data,
                    end_stream,
                } => {
                    let mut data = DataChunk::new(data);
                    while !data.is_empty() {
                        let message = self
                            .response_mut(calls, stream_id)?
                            .deframer
                            .next(&mut data, &self.message_pool)
                            .map_err(Status::from_frame_err)?;
                        let Some(message) = message else {
                            continue;
                        };
                        let mode = {
                            let response = self.response_mut(calls, stream_id)?;
                            response.buffered_len =
                                response.buffered_len.saturating_add(message.payload.len());
                            response.message_count += 1;
                            if response.buffered_len > self.max_buffered_len
                                || response.message_count > self.max_buffered_msgs
                            {
                                return Err(Status::new(
                                    Code::ResourceExhausted,
                                    "stream buffer limit exceeded",
                                ));
                            }
                            response.mode
                        };
                        match mode {
                            ResponseMode::Unary => {
                                let response = self.response_mut(calls, stream_id)?;
                                if response.message.replace(message).is_some() {
                                    return Err(Status::new(
                                        Code::Internal,
                                        "unary response has multiple messages",
                                    ));
                                }
                            }
                            ResponseMode::Streaming => {
                                self.push_event(StreamEvent::Message { stream_id, message })?
                            }
                        }
                    }
                    if end_stream {
                        egress.abort(calls, h2, stream_id);
                        return Err(Status::new(Code::Internal, "missing grpc-status"));
                    }
                }
                conn::Event::StreamReset { stream_id, error } => {
                    let mode = calls
                        .get(stream_id)
                        .map(|record| record.response.mode)
                        .ok_or_else(|| Status::new(Code::Internal, "reset for unknown stream"))?;
                    self.ensure_delivery_capacity(mode)?;
                    let stream = egress
                        .remove(calls, stream_id)
                        .ok_or_else(|| Status::new(Code::Internal, "reset for unknown stream"))?;
                    let status = Status::from_reset_err(error);
                    match mode {
                        ResponseMode::Unary => self.push_unary(UnaryResult {
                            stream_id,
                            metadata: stream.response.metadata,
                            message: stream.response.message,
                            trailers: Metadata::new(),
                            status,
                        })?,
                        ResponseMode::Streaming => self.push_event(StreamEvent::Trailers {
                            stream_id,
                            metadata: Metadata::new(),
                            status,
                        })?,
                    }
                }
                _ => {}
            }
        }
        Ok(drained)
    }

    fn response_mut<'a>(
        &self,
        calls: &'a mut CallStore,
        stream_id: StreamId,
    ) -> Result<&'a mut ResponseState, Status> {
        calls
            .response_mut(stream_id)
            .ok_or_else(|| Status::new(Code::Internal, "event for unknown gRPC stream"))
    }

    fn finish_stream(
        &mut self,
        calls: &mut CallStore,
        egress: &mut Egress,
        stream_id: StreamId,
        status: Status,
        trailers: Metadata,
    ) -> Result<(), Status> {
        let mode = calls
            .get(stream_id)
            .map(|record| record.response.mode)
            .ok_or_else(|| Status::new(Code::Internal, "trailers for unknown gRPC stream"))?;
        self.ensure_delivery_capacity(mode)?;
        let stream = egress
            .remove(calls, stream_id)
            .ok_or_else(|| Status::new(Code::Internal, "trailers for unknown gRPC stream"))?;
        match mode {
            ResponseMode::Unary => self.push_unary(UnaryResult {
                stream_id,
                metadata: stream.response.metadata,
                message: stream.response.message,
                trailers,
                status,
            })?,
            ResponseMode::Streaming => self.push_event(StreamEvent::Trailers {
                stream_id,
                metadata: trailers,
                status,
            })?,
        }
        Ok(())
    }

    fn ensure_delivery_capacity(&self, mode: ResponseMode) -> Result<(), Status> {
        let full = match mode {
            ResponseMode::Unary => self.complete.is_full(),
            ResponseMode::Streaming => self.events.is_full(),
        };
        if full {
            let message = match mode {
                ResponseMode::Unary => "result queue is full",
                ResponseMode::Streaming => "event queue is full",
            };
            Err(Self::resource_exhausted(message))
        } else {
            Ok(())
        }
    }

    fn push_unary(&mut self, result: UnaryResult) -> Result<(), Status> {
        self.complete
            .push_back(result)
            .map_err(|_| Self::resource_exhausted("result queue is full"))
    }

    fn push_event(&mut self, event: StreamEvent) -> Result<(), Status> {
        self.events
            .push_back(event)
            .map_err(|_| Self::resource_exhausted("event queue is full"))
    }

    fn resource_exhausted(message: &'static str) -> Status {
        Status::new(Code::ResourceExhausted, message)
    }
}
