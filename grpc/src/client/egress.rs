use o3::buffer::{Pooled, SharedPool};
use o3::collections::{FixedQueue, Slab, SlabKey};
use sark_h2::{ClientRole, Conn, ConnError, ErrorCode, StreamId};

use crate::frame::MessageFrame;
use crate::status::{Code, Status};

use super::Config;
use super::call::{CallRecord, CallStore};

pub(super) enum PendingRequestTag {}

#[derive(Copy, Clone)]
pub(super) struct PendingRequestKey(SlabKey<PendingRequestTag>);

struct PendingRequest {
    bytes: Pooled,
    pos: usize,
    end_stream: bool,
    next: Option<PendingRequestKey>,
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

pub(super) struct Egress {
    pending: FixedQueue<StreamId>,
    requests: Slab<PendingRequest, PendingRequestTag>,
    request_pool: SharedPool,
    max_message_len: usize,
}

impl Egress {
    pub(super) fn with_config(config: &Config) -> Self {
        Self {
            pending: FixedQueue::with_capacity(config.max_in_flight),
            requests: Slab::with_capacity(config.max_pending_msgs),
            request_pool: SharedPool::new(config.max_pending_msgs, config.max_pending_len),
            max_message_len: config.max_message_len,
        }
    }

    pub(super) fn finish_send(
        &mut self,
        calls: &mut CallStore,
        h2: &mut Conn<ClientRole>,
        stream_id: StreamId,
    ) -> Result<(), Status> {
        let tail = {
            let record = calls
                .get_mut(stream_id)
                .ok_or_else(|| Status::new(Code::Internal, "request for unknown gRPC stream"))?;
            if record.send_closed {
                return Ok(());
            }
            record.send_closed = true;
            record.request_tail
        };
        if let Some(tail) = tail {
            let Some(request) = self.requests.get_mut(tail.0) else {
                self.remove(calls, stream_id);
                return Err(Status::new(
                    Code::Internal,
                    "pending request chain is corrupt",
                ));
            };
            request.end_stream = true;
            self.drive(calls, h2);
            return Ok(());
        }
        self.drive(calls, h2);
        h2.send_data(stream_id, &[], true)
            .map_err(Status::from_conn_err)
            .map(|_| ())
    }

    pub(super) fn send_message(
        &mut self,
        calls: &mut CallStore,
        h2: &mut Conn<ClientRole>,
        stream_id: StreamId,
        payload: &[u8],
        end_stream: bool,
    ) -> Result<(), Status> {
        if payload.len() > self.max_message_len {
            return Err(Self::resource_exhausted("gRPC message is too large"));
        }
        let header = MessageFrame::header(false, payload.len()).map_err(Status::from_frame_err)?;
        let record = calls
            .get(stream_id)
            .ok_or_else(|| Status::new(Code::Internal, "request for unknown gRPC stream"))?;
        if record.send_closed {
            return Err(Status::new(Code::Internal, "request stream is closed"));
        }
        if record.request_head.is_some() {
            self.enqueue(calls, stream_id, &header, payload, 0, end_stream)?;
            if end_stream {
                self.mark_closed(calls, stream_id)?;
            }
            self.drive(calls, h2);
            return Ok(());
        }

        let mut consumed = match h2
            .send_data_parts(stream_id, &header, payload, end_stream)
            .map_err(Status::from_conn_err)
        {
            Ok(n) => n,
            Err(status) => {
                self.remove(calls, stream_id);
                return Err(status);
            }
        };
        let total = header.len() + payload.len();
        while consumed < total && consumed >= header.len() {
            let payload_pos = consumed - header.len();
            let n = match h2
                .send_data(stream_id, &payload[payload_pos..], end_stream)
                .map_err(Status::from_conn_err)
            {
                Ok(n) => n,
                Err(status) => {
                    self.remove(calls, stream_id);
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
                self.enqueue(calls, stream_id, &header, payload, consumed, end_stream)
        {
            let _ = h2.reset_stream(stream_id, ErrorCode::Cancel);
            self.remove(calls, stream_id);
            return Err(status);
        }
        if end_stream {
            self.mark_closed(calls, stream_id)?;
        }
        Ok(())
    }

    fn mark_closed(&self, calls: &mut CallStore, stream_id: StreamId) -> Result<(), Status> {
        let record = calls
            .get_mut(stream_id)
            .ok_or_else(|| Status::new(Code::Internal, "request for unknown gRPC stream"))?;
        record.send_closed = true;
        Ok(())
    }

    fn enqueue(
        &mut self,
        calls: &mut CallStore,
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
        let (tail, queued) = calls
            .get(stream_id)
            .map(|record| (record.request_tail, record.request_queued))
            .ok_or_else(|| Status::new(Code::Internal, "request for unknown gRPC stream"))?;
        if !queued && self.pending.is_full() {
            return Err(Self::resource_exhausted("pending request queue is full"));
        }
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
        let key = self
            .requests
            .insert(PendingRequest {
                bytes: lease.freeze(),
                pos: 0,
                end_stream,
                next: None,
            })
            .map_err(|_| Self::resource_exhausted("pending request messages are full"))?;

        if let Some(tail) = tail {
            let Some(request) = self.requests.get_mut(tail.0) else {
                let _ = self.requests.remove(key);
                return Err(Status::new(
                    Code::Internal,
                    "pending request chain is corrupt",
                ));
            };
            request.next = Some(PendingRequestKey(key));
        }
        let Some(record) = calls.get_mut(stream_id) else {
            let _ = self.requests.remove(key);
            return Err(Status::new(
                Code::Internal,
                "request for unknown gRPC stream",
            ));
        };
        if record.request_head.is_none() {
            record.request_head = Some(PendingRequestKey(key));
        }
        record.request_tail = Some(PendingRequestKey(key));
        if !queued {
            if self.pending.push_back(stream_id).is_err() {
                self.remove(calls, stream_id);
                return Err(Self::resource_exhausted("pending request queue is full"));
            }
            if let Some(record) = calls.get_mut(stream_id) {
                record.request_queued = true;
            }
        }
        Ok(())
    }

    pub(super) fn drive(&mut self, calls: &mut CallStore, h2: &mut Conn<ClientRole>) {
        let len = self.pending.len();
        for _ in 0..len {
            let Some(stream_id) = self.pending.pop_front() else {
                break;
            };
            let Some(mut key) = calls.get_mut(stream_id).and_then(|record| {
                record.request_queued = false;
                record.request_head
            }) else {
                continue;
            };
            loop {
                let state = match self.requests.get_mut(key.0) {
                    Some(request) => request.drive(h2, stream_id),
                    None => Err(ConnError::BadStream),
                };
                match state {
                    Ok(RequestDrive::Complete) => {
                        let Some(request) = self.requests.remove(key.0) else {
                            self.remove(calls, stream_id);
                            break;
                        };
                        let next = request.next;
                        let Some(record) = calls.get_mut(stream_id) else {
                            self.clear_requests(next);
                            break;
                        };
                        record.request_head = next;
                        if let Some(next) = next {
                            key = next;
                        } else {
                            record.request_tail = None;
                            break;
                        }
                    }
                    Ok(RequestDrive::Blocked) => {
                        if self.pending.push_back(stream_id).is_ok() {
                            if let Some(record) = calls.get_mut(stream_id) {
                                record.request_queued = true;
                            }
                        } else {
                            self.remove(calls, stream_id);
                        }
                        break;
                    }
                    Err(_) => {
                        self.remove(calls, stream_id);
                        break;
                    }
                }
            }
        }
    }

    pub(super) fn remove(
        &mut self,
        calls: &mut CallStore,
        stream_id: StreamId,
    ) -> Option<CallRecord> {
        let mut record = calls.remove(stream_id)?;
        if record.request_queued {
            self.pending.retain(|pending| *pending != stream_id);
            record.request_queued = false;
        }
        self.clear_requests(record.request_head.take());
        record.request_tail = None;
        Some(record)
    }

    fn clear_requests(&mut self, mut key: Option<PendingRequestKey>) {
        while let Some(current) = key {
            let Some(request) = self.requests.remove(current.0) else {
                debug_assert!(false, "pending request chain is corrupt");
                break;
            };
            key = request.next;
        }
    }

    pub(super) fn abort(
        &mut self,
        calls: &mut CallStore,
        h2: &mut Conn<ClientRole>,
        stream_id: StreamId,
    ) {
        if calls.get(stream_id).is_some() {
            let _ = h2.reset_stream(stream_id, ErrorCode::Cancel);
            self.remove(calls, stream_id);
        }
    }

    fn resource_exhausted(message: &'static str) -> Status {
        Status::new(Code::ResourceExhausted, message)
    }
}
