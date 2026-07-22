use dope::driver::token::Token;
use dope_fiber::{ErasedTaskId, TaskId};
use o3::collections::FixedHashTable;

use crate::stream::StreamId;

#[derive(Clone, Copy)]
pub(crate) struct TaskTarget(Option<ErasedTaskId>);

impl TaskTarget {
    pub(crate) const fn idle() -> Self {
        Self(None)
    }

    pub(crate) const fn task(key: ErasedTaskId) -> Self {
        Self(Some(key))
    }

    pub(crate) const fn key(self) -> Option<ErasedTaskId> {
        self.0
    }
}

pub(crate) struct RunningTask {
    pub(crate) connection_id: Token,
    pub(crate) stream_id: StreamId,
    pub(crate) task: TaskId,
    pub(crate) key: ErasedTaskId,
    pub(crate) previous: Option<u32>,
    pub(crate) next: Option<u32>,
}

#[derive(Clone, Copy)]
struct TaskMapEntry {
    connection_id: Token,
    stream_id: StreamId,
    index: u32,
}

pub(crate) struct TaskMap {
    entries: FixedHashTable<TaskMapEntry>,
}

impl TaskMap {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: FixedHashTable::with_capacity(capacity),
        }
    }

    fn hash(connection_id: Token, stream_id: StreamId) -> u64 {
        let value = connection_id.raw() ^ u64::from(stream_id.0).wrapping_mul(0x9E37_79B9);
        value.wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }

    pub(crate) fn insert(
        &mut self,
        connection_id: Token,
        stream_id: StreamId,
        index: usize,
    ) -> bool {
        self.entries
            .try_insert(
                Self::hash(connection_id, stream_id),
                TaskMapEntry {
                    connection_id,
                    stream_id,
                    index: index as u32,
                },
                |entry| entry.connection_id == connection_id && entry.stream_id == stream_id,
            )
            .is_ok()
    }

    pub(crate) fn remove(&mut self, connection_id: Token, stream_id: StreamId) -> Option<usize> {
        self.entries
            .remove(Self::hash(connection_id, stream_id), |entry| {
                entry.connection_id == connection_id && entry.stream_id == stream_id
            })
            .map(|entry| entry.index as usize)
    }

    pub(crate) fn get(&self, connection_id: Token, stream_id: StreamId) -> Option<usize> {
        self.entries
            .get(Self::hash(connection_id, stream_id), |entry| {
                entry.connection_id == connection_id && entry.stream_id == stream_id
            })
            .map(|entry| entry.index as usize)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
