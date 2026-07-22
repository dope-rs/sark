use core::marker::PhantomData;
use core::pin::Pin;

use dope::driver::token::Token;
use dope_fiber::{ErasedTaskId, TaskContext, TaskId, TaskQueue, Waker};
use o3::collections::FixedHashTable;

use crate::stream::StreamId;

pub(crate) type TaskTarget = Option<ErasedTaskId>;

/// The listener owns the ready target for the lifetime of every child task.
pub(crate) unsafe fn listener_waker<'from, 'to>(waker: Waker<'from>) -> Waker<'to> {
    unsafe { core::mem::transmute(waker) }
}

pub(crate) struct TaskWake<'d> {
    task: TaskContext<TaskTarget>,
    pub(crate) bound: bool,
    driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'d> TaskWake<'d> {
    pub(crate) fn new() -> Self {
        Self {
            task: TaskContext::with_target(None),
            bound: false,
            driver: PhantomData,
        }
    }

    pub(crate) unsafe fn bind(
        mut self: Pin<&mut Self>,
        key: ErasedTaskId,
        ready: Pin<&TaskQueue<TaskTarget>>,
        parent: Waker<'_>,
    ) {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        let task = unsafe { Pin::new_unchecked(&this.task) };
        let _ = unsafe { task.bind_child(ready, Some(key), parent) };
        this.bound = true;
    }

    pub(crate) fn waker(self: Pin<&Self>) -> Waker<'d> {
        unsafe { self.map_unchecked(|this| &this.task).context_unchecked() }
    }

    unsafe fn unbind(mut self: Pin<&mut Self>) {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        if !this.bound {
            return;
        }
        unsafe { Pin::new_unchecked(&this.task).unbind() };
        this.bound = false;
    }

    pub(crate) fn at_mut(wakes: Pin<&mut [Self]>, index: usize) -> Pin<&mut Self> {
        unsafe { wakes.map_unchecked_mut(|wakes| &mut wakes[index]) }
    }
}

pub(crate) struct BoundTask<'a, 'd>(Option<Pin<&'a mut TaskWake<'d>>>);

impl<'a, 'd> BoundTask<'a, 'd> {
    pub(crate) fn new(wake: Pin<&'a mut TaskWake<'d>>) -> Self {
        Self(Some(wake))
    }
}

impl Drop for BoundTask<'_, '_> {
    fn drop(&mut self) {
        unsafe { self.0.take().unwrap().unbind() };
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
