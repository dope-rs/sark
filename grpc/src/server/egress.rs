use o3::collections::{Slab, SlabKey};
use sark_h2::ServerRole;
use sark_h2::conn::Conn;
use sark_h2::stream::StreamId;

use crate::frame::MessageFrame;
use crate::headers::HeaderBlock;

pub(super) enum ReplyBatchTag {}
pub(super) type ReplyBatchKey = SlabKey<ReplyBatchTag>;

pub(super) struct ReplyBatch {
    pub(super) messages: Vec<Vec<u8>>,
    pub(super) next: Option<ReplyBatchKey>,
}

#[derive(Clone, Debug)]
pub(super) struct PendingResponse {
    pub(super) headers: HeaderBlock,
    pub(super) head: Option<ReplyBatchKey>,
    pub(super) tail: Option<ReplyBatchKey>,
    pub(super) trailers: Option<HeaderBlock>,
    pub(super) headers_sent: bool,
    pub(super) message_pos: usize,
    pub(super) frame_pos: usize,
}

pub(super) enum ResponseDrive {
    Complete,
    Blocked,
    Idle,
}

impl PendingResponse {
    pub(super) fn drive(
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
                let batch = replies.get(key).unwrap();
                if self.message_pos == batch.messages.len() {
                    let batch = replies.remove(key).unwrap();
                    self.head = batch.next;
                    if self.head.is_none() {
                        self.tail = None;
                    }
                    self.message_pos = 0;
                    continue;
                }
                batch.messages[self.message_pos].len()
            };
            let header = MessageFrame::header(false, message_len).map_err(|_| ())?;
            let n = {
                let payload = &replies.get(key).unwrap().messages[self.message_pos];
                if self.frame_pos < header.len() {
                    conn.send_data_parts(stream_id, &header[self.frame_pos..], payload, false)
                } else {
                    conn.send_data(stream_id, &payload[self.frame_pos - header.len()..], false)
                }
            }
            .map_err(|_| ())?;
            if n == 0 {
                return Ok(ResponseDrive::Blocked);
            }
            self.frame_pos += n;
            *pending_len -= n;
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

    pub(super) fn remaining_len(&self, replies: &Slab<ReplyBatch, ReplyBatchTag>) -> usize {
        let mut total = 0usize;
        let mut key = self.head;
        let mut first = true;
        while let Some(current) = key {
            let batch = replies.get(current).unwrap();
            let start = if first { self.message_pos } else { 0 };
            for (index, message) in batch.messages[start..].iter().enumerate() {
                let len = message.len() + 5;
                total += if first && index == 0 {
                    len - self.frame_pos
                } else {
                    len
                };
            }
            first = false;
            key = batch.next;
        }
        total
    }
}
