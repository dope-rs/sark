use o3::collections::{FixedHashTable, FixedQueue, Slab, SlabKey};
use sark_h2::StreamId;

use super::egress::PendingResponse;
use crate::frame::{Deframer, MessageFrame};
use crate::headers::RequestHead;
use crate::metadata::Metadata;
use crate::server::StreamMode;

enum MessageNodeTag {}
type MessageNodeKey = SlabKey<MessageNodeTag>;

struct MessageNode {
    message: MessageFrame,
    next: Option<MessageNodeKey>,
}

pub(super) struct StreamState {
    pub(super) head: RequestHead,
    pub(super) deframer: Deframer,
    message_head: Option<MessageNodeKey>,
    message_tail: Option<MessageNodeKey>,
    pub(super) message_count: usize,
    pub(super) trailers: Metadata,
    pub(super) mode: StreamMode,
    pub(super) buffered_len: usize,
}

#[derive(Clone, Copy)]
pub(super) struct MessageChain {
    head: Option<MessageNodeKey>,
    len: usize,
}

impl StreamState {
    pub(super) fn new(head: RequestHead, max_message_len: usize, mode: StreamMode) -> Self {
        Self {
            head,
            deframer: Deframer::new(max_message_len),
            message_head: None,
            message_tail: None,
            message_count: 0,
            trailers: Metadata::new(),
            mode,
            buffered_len: 0,
        }
    }

    pub(super) fn message_chain(&self) -> MessageChain {
        MessageChain {
            head: self.message_head,
            len: self.message_count,
        }
    }
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

pub struct MessageList<'a> {
    repr: MessageListRepr<'a>,
    len: usize,
}

enum MessageListRepr<'a> {
    Chain {
        nodes: &'a Slab<MessageNode, MessageNodeTag>,
        next: Option<MessageNodeKey>,
    },
    Queue(&'a FixedQueue<MessageFrame>),
}

impl<'a> MessageList<'a> {
    pub(super) fn from_queue(queue: &'a FixedQueue<MessageFrame>) -> Self {
        Self {
            len: queue.len(),
            repr: MessageListRepr::Queue(queue),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> MessageIter<'_> {
        let repr = match &self.repr {
            MessageListRepr::Chain { nodes, next } => MessageIterRepr::Chain { nodes, next: *next },
            MessageListRepr::Queue(queue) => MessageIterRepr::Queue { queue, index: 0 },
        };
        MessageIter { repr }
    }
}

impl<'a> IntoIterator for &'a MessageList<'_> {
    type Item = &'a MessageFrame;
    type IntoIter = MessageIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct MessageIter<'a> {
    repr: MessageIterRepr<'a>,
}

enum MessageIterRepr<'a> {
    Chain {
        nodes: &'a Slab<MessageNode, MessageNodeTag>,
        next: Option<MessageNodeKey>,
    },
    Queue {
        queue: &'a FixedQueue<MessageFrame>,
        index: usize,
    },
}

impl<'a> Iterator for MessageIter<'a> {
    type Item = &'a MessageFrame;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.repr {
            MessageIterRepr::Chain { nodes, next } => {
                let node = nodes.get((*next)?)?;
                *next = node.next;
                Some(&node.message)
            }
            MessageIterRepr::Queue { queue, index } => {
                let message = queue.get(*index)?;
                *index += 1;
                Some(message)
            }
        }
    }
}

impl CallStore {
    pub(super) fn with_capacity(streams: usize, messages: usize) -> Self {
        Self {
            records: FixedHashTable::with_capacity(streams),
            messages: Slab::with_capacity(messages),
            buffered_total: 0,
        }
    }

    pub(super) fn get_mut(&mut self, stream_id: StreamId) -> Option<&mut CallRecord> {
        self.records
            .get_mut(u64::from(stream_id.0), |call| call.stream_id == stream_id)
    }

    pub(super) fn stream_mut(&mut self, stream_id: StreamId) -> Option<&mut StreamState> {
        self.get_mut(stream_id)?.stream.as_mut()
    }

    pub(super) fn insert(&mut self, stream_id: StreamId, stream: StreamState) -> bool {
        self.records
            .try_insert(
                u64::from(stream_id.0),
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
        max_stream_len: usize,
        max_messages: usize,
        max_total_len: usize,
    ) -> Result<(), ()> {
        let added = message.payload.len();
        let Self {
            records,
            messages,
            buffered_total,
        } = self;
        let Some(stream) = records
            .get_mut(u64::from(stream_id.0), |call| call.stream_id == stream_id)
            .and_then(|call| call.stream.as_mut())
        else {
            return Err(());
        };
        if stream.buffered_len.saturating_add(added) > max_stream_len
            || stream.message_count == max_messages
            || buffered_total.saturating_add(added) > max_total_len
        {
            return Err(());
        }
        let key = match messages.insert(MessageNode {
            message,
            next: None,
        }) {
            Ok(key) => key,
            Err(_) => return Err(()),
        };
        if let Some(tail) = stream.message_tail {
            let Some(node) = messages.get_mut(tail) else {
                let _ = messages.remove(key);
                return Err(());
            };
            node.next = Some(key);
        }
        if stream.message_head.is_none() {
            stream.message_head = Some(key);
        }
        stream.message_tail = Some(key);
        stream.message_count += 1;
        stream.buffered_len += added;
        *buffered_total += added;
        Ok(())
    }

    fn clear_messages(&mut self, mut next: Option<MessageNodeKey>) {
        while let Some(key) = next {
            let Some(node) = self.messages.remove(key) else {
                break;
            };
            next = node.next;
        }
    }

    pub(super) fn remove(&mut self, stream_id: StreamId) -> Option<CallRecord> {
        self.records
            .remove(u64::from(stream_id.0), |call| call.stream_id == stream_id)
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
            .get(u64::from(stream_id.0), |call| call.stream_id == stream_id)
            .is_some_and(|call| call.stream.is_none() && call.pending.is_none());
        if empty {
            let _ = self
                .records
                .remove(u64::from(stream_id.0), |call| call.stream_id == stream_id);
        }
    }

    pub(super) fn message_list(&self, chain: MessageChain) -> MessageList<'_> {
        MessageList {
            repr: MessageListRepr::Chain {
                nodes: &self.messages,
                next: chain.head,
            },
            len: chain.len,
        }
    }

    pub(super) fn release_messages(&mut self, chain: MessageChain) {
        self.clear_messages(chain.head);
    }
}
