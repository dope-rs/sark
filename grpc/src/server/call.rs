use o3::collections::{FixedHashTable, Slab, SlabKey};
use sark_h2::StreamId;

use super::egress::PendingResponse;
use crate::frame::{Deframer, MessageFrame};
use crate::headers::RequestHead;
use crate::metadata::Metadata;
use crate::server::StreamMode;

pub(super) enum MessageNodeTag {}
pub(super) type MessageNodeKey = SlabKey<MessageNodeTag>;

pub(super) struct MessageNode {
    pub(super) message: MessageFrame,
    pub(super) next: Option<MessageNodeKey>,
}

pub(super) struct StreamState {
    pub(super) head: RequestHead,
    pub(super) deframer: Deframer,
    pub(super) message_head: Option<MessageNodeKey>,
    pub(super) message_tail: Option<MessageNodeKey>,
    pub(super) message_count: usize,
    pub(super) trailers: Metadata,
    pub(super) mode: StreamMode,
    pub(super) buffered_len: usize,
}

pub(super) struct CallRecord {
    pub(super) stream_id: StreamId,
    pub(super) stream: Option<StreamState>,
    pub(super) pending: Option<PendingResponse>,
    pub(super) queued: bool,
}

pub(super) struct CallStore {
    records: FixedHashTable<CallRecord>,
    messages: Slab<MessageNode, MessageNodeTag>,
    buffered_total: usize,
}

impl CallStore {
    pub(super) fn with_capacity(streams: usize, messages: usize) -> Self {
        Self {
            records: FixedHashTable::with_capacity(streams),
            messages: Slab::with_capacity(messages),
            buffered_total: 0,
        }
    }

    fn hash(stream_id: StreamId) -> u64 {
        u64::from(stream_id.0)
    }

    pub(super) fn get_mut(&mut self, stream_id: StreamId) -> Option<&mut CallRecord> {
        self.records
            .get_mut(Self::hash(stream_id), |call| call.stream_id == stream_id)
    }

    pub(super) fn stream_mut(&mut self, stream_id: StreamId) -> Option<&mut StreamState> {
        self.get_mut(stream_id)?.stream.as_mut()
    }

    pub(super) fn insert(&mut self, stream_id: StreamId, stream: StreamState) -> bool {
        self.records
            .try_insert(
                Self::hash(stream_id),
                CallRecord {
                    stream_id,
                    stream: Some(stream),
                    pending: None,
                    queued: false,
                },
                |call| call.stream_id == stream_id,
            )
            .is_ok()
    }

    pub(super) fn push_message(
        &mut self,
        stream_id: StreamId,
        message: MessageFrame,
    ) -> Result<(), MessageFrame> {
        let Some((tail, count)) = self
            .stream_mut(stream_id)
            .map(|stream| (stream.message_tail, stream.message_count))
        else {
            return Err(message);
        };
        let key = match self.messages.insert(MessageNode {
            message,
            next: None,
        }) {
            Ok(key) => key,
            Err(node) => return Err(node.message),
        };
        if let Some(tail) = tail {
            self.messages.get_mut(tail).unwrap().next = Some(key);
        }
        let stream = self.stream_mut(stream_id).unwrap();
        if stream.message_head.is_none() {
            stream.message_head = Some(key);
        }
        stream.message_tail = Some(key);
        stream.message_count = count + 1;
        Ok(())
    }

    pub(super) fn clear_messages(&mut self, mut next: Option<MessageNodeKey>) {
        while let Some(key) = next {
            let node = self.messages.remove(key).unwrap();
            next = node.next;
        }
    }

    pub(super) fn remove(&mut self, stream_id: StreamId) -> Option<CallRecord> {
        self.records
            .remove(Self::hash(stream_id), |call| call.stream_id == stream_id)
    }

    pub(super) fn release_stream(&mut self, stream: &StreamState) {
        self.buffered_total = self.buffered_total.saturating_sub(stream.buffered_len);
        self.clear_messages(stream.message_head);
    }

    pub(super) fn take_stream(&mut self, stream_id: StreamId) -> Option<StreamState> {
        let stream = self.get_mut(stream_id)?.stream.take()?;
        self.buffered_total = self.buffered_total.saturating_sub(stream.buffered_len);
        Some(stream)
    }

    pub(super) fn remove_empty(&mut self, stream_id: StreamId) {
        let empty = self
            .records
            .get(Self::hash(stream_id), |call| call.stream_id == stream_id)
            .is_some_and(|call| call.stream.is_none() && call.pending.is_none());
        if empty {
            let _ = self
                .records
                .remove(Self::hash(stream_id), |call| call.stream_id == stream_id);
        }
    }

    pub(super) fn messages(&self) -> &Slab<MessageNode, MessageNodeTag> {
        &self.messages
    }

    pub(super) fn buffered_total(&self) -> usize {
        self.buffered_total
    }

    pub(super) fn add_buffered(&mut self, bytes: usize) {
        self.buffered_total += bytes;
    }
}
