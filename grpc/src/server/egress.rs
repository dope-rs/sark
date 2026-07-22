use o3::collections::{FixedQueue, Slab, SlabKey};
use sark_h2::ServerRole;
use sark_h2::conn::Conn;
use sark_h2::stream::StreamId;

use crate::frame::MessageFrame;
use crate::headers::HeaderBlock;
use crate::status::{Code, Status};

use super::StreamReply;
use super::call::{CallRecord, CallStore};

enum ReplyBatchTag {}
type ReplyBatchKey = SlabKey<ReplyBatchTag>;

struct ReplyBatch {
    messages: Vec<Vec<u8>>,
    next: Option<ReplyBatchKey>,
}

#[derive(Clone, Debug)]
pub(super) struct PendingResponse {
    headers: HeaderBlock,
    head: Option<ReplyBatchKey>,
    tail: Option<ReplyBatchKey>,
    trailers: Option<HeaderBlock>,
    headers_sent: bool,
    message_pos: usize,
    frame_pos: usize,
    remaining_bytes: usize,
}

enum ResponseDrive {
    Complete,
    Blocked,
    Idle,
}

pub(super) struct Egress {
    pending: FixedQueue<StreamId>,
    replies: Slab<ReplyBatch, ReplyBatchTag>,
    pending_len: usize,
    pending_capacity: usize,
}

impl Egress {
    pub(super) fn with_capacity(streams: usize, batches: usize, bytes: usize) -> Self {
        Self {
            pending: FixedQueue::with_capacity(streams),
            replies: Slab::with_capacity(batches),
            pending_len: 0,
            pending_capacity: bytes,
        }
    }

    pub(super) fn enqueue(
        &mut self,
        calls: &mut CallStore,
        stream_id: StreamId,
        reply: StreamReply,
    ) -> Result<(), Status> {
        if reply.messages.is_empty()
            && reply.status.is_none()
            && reply.metadata.entries().is_empty()
        {
            return Ok(());
        }

        let added = reply.messages.iter().try_fold(0usize, |total, message| {
            MessageFrame::header(false, message.len())
                .map_err(|_| Status::new(Code::Internal, "response message too large"))?;
            let framed_len = message.len().checked_add(5).ok_or_else(|| {
                Status::new(Code::ResourceExhausted, "pending response bytes are full")
            })?;
            total.checked_add(framed_len).ok_or_else(|| {
                Status::new(Code::ResourceExhausted, "pending response bytes are full")
            })
        })?;
        let Some(next_pending_len) = self
            .pending_len
            .checked_add(added)
            .filter(|total| *total <= self.pending_capacity)
        else {
            return Err(Status::new(
                Code::ResourceExhausted,
                "pending response bytes are full",
            ));
        };

        let headers = HeaderBlock::for_response(&reply.metadata)?;
        let trailers = reply
            .status
            .map(|status| HeaderBlock::for_trailers(&status, &reply.trailers))
            .transpose()?;
        let Some(call) = calls.get_mut(stream_id) else {
            return Err(Status::new(Code::Internal, "reply for unknown gRPC stream"));
        };
        let needs_queue = !call.queued;
        if needs_queue && self.pending.is_full() {
            return Err(Status::new(
                Code::ResourceExhausted,
                "pending response queue is full",
            ));
        }
        if needs_queue && self.pending.push_back(stream_id).is_err() {
            return Err(Status::new(
                Code::ResourceExhausted,
                "pending response queue is full",
            ));
        }

        let batch = if reply.messages.is_empty() {
            None
        } else {
            match self.replies.insert(ReplyBatch {
                messages: reply.messages,
                next: None,
            }) {
                Ok(batch) => Some(batch),
                Err(_) => {
                    if needs_queue {
                        self.pending.retain(|pending| *pending != stream_id);
                    }
                    return Err(Status::new(
                        Code::ResourceExhausted,
                        "pending response messages are full",
                    ));
                }
            }
        };
        let tail = call.pending.as_ref().and_then(|pending| pending.tail);
        if let (Some(tail), Some(batch)) = (tail, batch) {
            let Some(previous) = self.replies.get_mut(tail) else {
                let _ = self.replies.remove(batch);
                if needs_queue {
                    self.pending.retain(|pending| *pending != stream_id);
                }
                return Err(Status::new(
                    Code::Internal,
                    "pending response chain is inconsistent",
                ));
            };
            previous.next = Some(batch);
        }

        match call.pending.as_mut() {
            Some(pending) => {
                if pending.head.is_none() {
                    pending.head = batch;
                }
                if batch.is_some() {
                    pending.tail = batch;
                }
                if trailers.is_some() {
                    pending.trailers = trailers;
                }
                pending.remaining_bytes += added;
            }
            None => {
                call.pending = Some(PendingResponse {
                    headers,
                    head: batch,
                    tail: batch,
                    trailers,
                    headers_sent: false,
                    message_pos: 0,
                    frame_pos: 0,
                    remaining_bytes: added,
                });
            }
        }
        self.pending_len = next_pending_len;
        if needs_queue {
            call.queued = true;
        }
        Ok(())
    }

    pub(super) fn drive(&mut self, calls: &mut CallStore, conn: &mut Conn<ServerRole>) {
        let len = self.pending.len();
        for _ in 0..len {
            let Some(stream_id) = self.pending.pop_front() else {
                break;
            };
            let Some(mut pending) = calls.get_mut(stream_id).and_then(|call| {
                call.queued = false;
                call.pending.take()
            }) else {
                continue;
            };
            match pending.drive(conn, &mut self.replies, &mut self.pending_len, stream_id) {
                Ok(ResponseDrive::Complete) => calls.remove_empty(stream_id),
                Ok(ResponseDrive::Blocked) => {
                    if let Some(call) = calls.get_mut(stream_id) {
                        if self.pending.push_back(stream_id).is_ok() {
                            call.pending = Some(pending);
                            call.queued = true;
                        } else {
                            self.release(pending);
                            calls.remove_empty(stream_id);
                        }
                    } else {
                        self.release(pending);
                    }
                }
                Ok(ResponseDrive::Idle) => {
                    if let Some(call) = calls.get_mut(stream_id) {
                        call.pending = Some(pending);
                    }
                }
                Err(()) => {
                    self.release(pending);
                    calls.remove_empty(stream_id);
                }
            }
        }
    }

    pub(super) fn detach(&mut self, stream_id: StreamId, call: &mut CallRecord) {
        if call.queued {
            self.pending.retain(|pending| *pending != stream_id);
            call.queued = false;
        }
        if let Some(pending) = call.pending.take() {
            self.release(pending);
        }
    }

    fn release(&mut self, pending: PendingResponse) {
        self.pending_len = self.pending_len.saturating_sub(pending.remaining_bytes);
        let mut key = pending.head;
        while let Some(current) = key {
            let Some(batch) = self.replies.remove(current) else {
                break;
            };
            key = batch.next;
        }
    }
}

impl PendingResponse {
    fn drive(
        &mut self,
        conn: &mut Conn<ServerRole>,
        replies: &mut Slab<ReplyBatch, ReplyBatchTag>,
        pending_len: &mut usize,
        stream_id: StreamId,
    ) -> Result<ResponseDrive, ()> {
        if !self.headers_sent {
            let h2_headers = self.headers.as_h2();
            conn.send_response(stream_id, h2_headers.iter().copied(), false)
                .map_err(|_| ())?;
            self.headers_sent = true;
        }

        while let Some(key) = self.head {
            let message_len = {
                let Some(batch) = replies.get(key) else {
                    return Err(());
                };
                if self.message_pos > batch.messages.len() {
                    return Err(());
                }
                if self.message_pos == batch.messages.len() {
                    let Some(batch) = replies.remove(key) else {
                        return Err(());
                    };
                    self.head = batch.next;
                    if self.head.is_none() {
                        self.tail = None;
                    }
                    self.message_pos = 0;
                    continue;
                }
                let Some(message) = batch.messages.get(self.message_pos) else {
                    return Err(());
                };
                message.len()
            };
            let header = MessageFrame::header(false, message_len).map_err(|_| ())?;
            let n = {
                let Some(payload) = replies
                    .get(key)
                    .and_then(|batch| batch.messages.get(self.message_pos))
                else {
                    return Err(());
                };
                if self.frame_pos < header.len() {
                    conn.send_data_parts(stream_id, &header[self.frame_pos..], payload, false)
                } else {
                    let Some(remaining) = payload.get(self.frame_pos - header.len()..) else {
                        return Err(());
                    };
                    conn.send_data(stream_id, remaining, false)
                }
            }
            .map_err(|_| ())?;
            if n == 0 {
                return Ok(ResponseDrive::Blocked);
            }
            self.frame_pos += n;
            *pending_len = pending_len.saturating_sub(n);
            self.remaining_bytes = self.remaining_bytes.saturating_sub(n);
            if self.frame_pos == header.len() + message_len {
                self.message_pos += 1;
                self.frame_pos = 0;
            }
        }

        if let Some(trailers) = &self.trailers {
            let h2_trailers = trailers.as_h2();
            conn.send_trailers(stream_id, &h2_trailers)
                .map_err(|_| ())?;
            Ok(ResponseDrive::Complete)
        } else {
            Ok(ResponseDrive::Idle)
        }
    }
}
