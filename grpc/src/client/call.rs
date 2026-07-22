use o3::collections::FixedHashTable;
use sark_h2::StreamId;

use crate::frame::{Deframer, MessageFrame};
use crate::metadata::Metadata;

use super::egress::PendingRequestKey;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum ResponseMode {
    Unary,
    Streaming,
}

pub(super) struct ResponseState {
    pub(super) mode: ResponseMode,
    pub(super) metadata: Metadata,
    pub(super) deframer: Deframer,
    pub(super) message: Option<MessageFrame>,
    pub(super) buffered_len: usize,
    pub(super) message_count: usize,
}

pub(super) struct CallRecord {
    pub(super) stream_id: StreamId,
    pub(super) response: ResponseState,
    pub(super) request_head: Option<PendingRequestKey>,
    pub(super) request_tail: Option<PendingRequestKey>,
    pub(super) request_queued: bool,
    pub(super) send_closed: bool,
}

pub(super) struct CallStore {
    records: FixedHashTable<CallRecord>,
    max_message_len: usize,
}

impl CallStore {
    pub(super) fn with_capacity(streams: usize, max_message_len: usize) -> Self {
        Self {
            records: FixedHashTable::with_capacity(streams),
            max_message_len,
        }
    }

    pub(super) fn insert(&mut self, stream_id: StreamId, mode: ResponseMode) -> bool {
        self.records
            .try_insert(
                u64::from(stream_id.0),
                CallRecord {
                    stream_id,
                    response: ResponseState {
                        mode,
                        metadata: Metadata::new(),
                        deframer: Deframer::new(self.max_message_len),
                        message: None,
                        buffered_len: 0,
                        message_count: 0,
                    },
                    request_head: None,
                    request_tail: None,
                    request_queued: false,
                    send_closed: false,
                },
                |record| record.stream_id == stream_id,
            )
            .is_ok()
    }

    pub(super) fn get(&self, stream_id: StreamId) -> Option<&CallRecord> {
        self.records.get(u64::from(stream_id.0), |record| {
            record.stream_id == stream_id
        })
    }

    pub(super) fn get_mut(&mut self, stream_id: StreamId) -> Option<&mut CallRecord> {
        self.records.get_mut(u64::from(stream_id.0), |record| {
            record.stream_id == stream_id
        })
    }

    pub(super) fn response_mut(&mut self, stream_id: StreamId) -> Option<&mut ResponseState> {
        self.get_mut(stream_id).map(|record| &mut record.response)
    }

    pub(super) fn remove(&mut self, stream_id: StreamId) -> Option<CallRecord> {
        self.records.remove(u64::from(stream_id.0), |record| {
            record.stream_id == stream_id
        })
    }
}
