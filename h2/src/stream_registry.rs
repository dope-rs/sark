use core::marker::PhantomData;

use o3::collections::{FixedHashTable, FixedQueue};

use crate::flow;
use crate::role::Role;
use crate::stream::{self, Stream, StreamId};

const RESET_RING_CAP: usize = 256;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum StreamClass {
    Connection,
    Active,
    ClosedRst,
    ClosedEnd,
    Idle,
}

pub(crate) struct StreamRecord {
    pub(crate) stream: Stream,
    pub(crate) send_window: flow::Window,
    pub(crate) recv_window: flow::Window,
    pub(crate) pending_release: u32,
}

pub(crate) struct StreamRegistry<R: Role> {
    role: PhantomData<R>,
    streams: FixedHashTable<StreamRecord>,
    local_count: usize,
    peer_count: usize,
    next_local_id: stream::IdGen,
    last_peer_id: StreamId,
    reset: FixedQueue<StreamId>,
}

impl<R: Role> StreamRegistry<R> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            role: PhantomData,
            streams: FixedHashTable::with_capacity(capacity),
            local_count: 0,
            peer_count: 0,
            next_local_id: stream::IdGen::new(R::FIRST_LOCAL_STREAM_ID),
            last_peer_id: StreamId::CONNECTION,
            reset: FixedQueue::with_capacity(RESET_RING_CAP),
        }
    }

    pub(crate) fn is_peer_initiated(id: StreamId) -> bool {
        if R::IS_SERVER {
            id.is_client()
        } else {
            id.is_server()
        }
    }

    pub(crate) fn is_local_initiated(id: StreamId) -> bool {
        !Self::is_peer_initiated(id)
    }

    pub(crate) fn get(&self, id: StreamId) -> Option<&StreamRecord> {
        self.streams
            .get(u64::from(id.as_u32()), |record| record.stream.id == id)
    }

    pub(crate) fn get_mut(&mut self, id: StreamId) -> Option<&mut StreamRecord> {
        self.streams
            .get_mut(u64::from(id.as_u32()), |record| record.stream.id == id)
    }

    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut StreamRecord> {
        self.streams.values_mut()
    }

    pub(crate) fn insert(
        &mut self,
        stream: Stream,
        send_window: u32,
        recv_window: u32,
    ) -> Result<(), Stream> {
        let id = stream.id;
        let record = StreamRecord {
            stream,
            send_window: flow::Window::with(send_window as i32),
            recv_window: flow::Window::with(recv_window as i32),
            pending_release: 0,
        };
        match self
            .streams
            .try_insert(u64::from(id.as_u32()), record, |record| {
                record.stream.id == id
            }) {
            Ok(()) => {
                if Self::is_local_initiated(id) {
                    self.local_count += 1;
                } else {
                    self.peer_count += 1;
                }
                Ok(())
            }
            Err(record) => Err(record.stream),
        }
    }

    pub(crate) fn remove(&mut self, id: StreamId) {
        if self
            .streams
            .remove(u64::from(id.as_u32()), |record| record.stream.id == id)
            .is_some()
        {
            if Self::is_local_initiated(id) {
                self.local_count -= 1;
            } else {
                self.peer_count -= 1;
            }
        }
    }

    pub(crate) fn active_count(&self) -> usize {
        self.streams.len()
    }

    pub(crate) fn reset_count(&self) -> usize {
        self.reset.len()
    }

    pub(crate) fn can_accept_peer(&self, limit: usize) -> bool {
        self.peer_count < limit && self.active_count() < self.streams.capacity()
    }

    pub(crate) fn can_open_local(&self, limit: usize) -> bool {
        self.local_count < limit && self.active_count() < self.streams.capacity()
    }

    pub(crate) fn mark_reset(&mut self, id: StreamId) {
        if self.reset.contains(&id) {
            return;
        }
        if self.reset.is_full() {
            self.reset.pop_front();
        }
        let Some(entry) = self.reset.vacant_entry() else {
            debug_assert!(false, "reset queue must have space after eviction");
            return;
        };
        entry.push_back(id);
    }

    pub(crate) fn classify(&self, id: StreamId) -> StreamClass {
        if id.is_zero() {
            return StreamClass::Connection;
        }
        if self.get(id).is_some() {
            return StreamClass::Active;
        }
        let previously_opened = if Self::is_peer_initiated(id) {
            id <= self.last_peer_id
        } else {
            self.next_local_id.peek().is_none_or(|next| id < next)
        };
        if previously_opened {
            return if self.reset.contains(&id) {
                StreamClass::ClosedRst
            } else {
                StreamClass::ClosedEnd
            };
        }
        StreamClass::Idle
    }

    pub(crate) fn last_peer_id(&self) -> StreamId {
        self.last_peer_id
    }

    pub(crate) fn observe_peer(&mut self, id: StreamId) {
        self.last_peer_id = id;
    }

    pub(crate) fn next_local_id(&mut self) -> Option<StreamId> {
        self.next_local_id.next_id()
    }
}
