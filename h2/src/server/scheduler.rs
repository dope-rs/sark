use std::task::Poll;

use dope::DriverContext;
use dope::driver::token::Token;
use dope_fiber::abi::Fiber;
use dope_fiber::raw::slab::TaskSlab;
use dope_fiber::raw::task::RootWaker;
use dope_fiber::slab::{ErasedTaskId, TaskId};
use o3::collections::FixedHashTable;

use super::task::{RunningTask, TaskMap, TaskTarget};
use crate::stream::StreamId;

pub(super) enum Started<T> {
    Ready(T),
    Pending,
    Refused,
    Failed,
}

pub(super) enum Resumed<T> {
    Ready(Option<StreamId>, T),
    Pending,
    Failed(Option<StreamId>),
    Stale,
}

pub(super) struct StartContext<'a, 'turn, 'd> {
    pub(super) connection_id: Token,
    pub(super) stream_id: StreamId,
    pub(super) task_head: &'a mut Option<u32>,
    pub(super) parent: RootWaker<'d>,
    pub(super) driver: &'a mut DriverContext<'turn, 'd>,
}

pub(super) struct Scheduler<'d, F>
where
    F: Fiber<'d> + 'd,
{
    slab: TaskSlab<'d, F, TaskTarget>,
    tasks: Box<[Option<RunningTask>]>,
    task_map: TaskMap,
    ready: ReadyMap,
}

#[derive(Clone, Copy)]
struct ReadyConnection {
    connection_id: Token,
    head: u32,
}

struct ReadyMap {
    entries: FixedHashTable<ReadyConnection>,
}

impl ReadyMap {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: FixedHashTable::with_capacity(capacity),
        }
    }

    fn hash(connection_id: Token) -> u64 {
        connection_id.raw().wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }

    fn head(&self, connection_id: Token) -> Option<usize> {
        self.entries
            .get(Self::hash(connection_id), |entry| {
                entry.connection_id == connection_id
            })
            .map(|entry| entry.head as usize)
    }

    fn replace_head(&mut self, connection_id: Token, head: usize) -> bool {
        if let Some(entry) = self.entries.get_mut(Self::hash(connection_id), |entry| {
            entry.connection_id == connection_id
        }) {
            entry.head = head as u32;
            return true;
        }
        self.entries
            .try_insert(
                Self::hash(connection_id),
                ReadyConnection {
                    connection_id,
                    head: head as u32,
                },
                |entry| entry.connection_id == connection_id,
            )
            .is_ok()
    }

    fn remove(&mut self, connection_id: Token) {
        self.entries.remove(Self::hash(connection_id), |entry| {
            entry.connection_id == connection_id
        });
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl<'d, F> Scheduler<'d, F>
where
    F: Fiber<'d> + 'd,
{
    pub(super) fn try_with_capacity(capacity: usize) -> Option<Self> {
        if capacity == 0
            || u32::try_from(capacity).is_err()
            || !FixedHashTable::<ReadyConnection>::capacity_fits(capacity)
        {
            return None;
        }
        Some(Self {
            slab: TaskSlab::with_capacity(capacity),
            tasks: (0..capacity).map(|_| None).collect(),
            task_map: TaskMap::with_capacity(capacity),
            ready: ReadyMap::with_capacity(capacity),
        })
    }

    pub(super) fn start(
        &mut self,
        fiber: F,
        context: StartContext<'_, '_, 'd>,
    ) -> Started<F::Output> {
        let StartContext {
            connection_id,
            stream_id,
            task_head,
            parent,
            driver,
        } = context;
        let Some(task) = self.slab.insert(fiber) else {
            return Started::Refused;
        };
        let key = task.erase();
        let task = TaskId::from_erased(key);
        let mut task = TaskPoll::new(self, task_head, NewTask::new(task));
        if !task.bind(key, parent) {
            return Started::Failed;
        }
        match task.poll(driver) {
            Some(Poll::Ready(output)) => {
                let _ = task.complete();
                Started::Ready(output)
            }
            Some(Poll::Pending) => {
                if task.register(connection_id, stream_id, key) {
                    Started::Pending
                } else {
                    Started::Refused
                }
            }
            None => {
                debug_assert!(false, "live task must exist in fiber slab");
                let _ = task.complete();
                Started::Failed
            }
        }
    }

    pub(super) fn route_ready(&mut self, parent: RootWaker<'d>) {
        let Self {
            slab, tasks, ready, ..
        } = self;
        let Some(snapshot) = slab.snapshot_root(parent) else {
            return;
        };
        for target in snapshot {
            Self::queue_ready(tasks, ready, target);
        }
    }

    fn queue_ready(tasks: &mut [Option<RunningTask>], ready: &mut ReadyMap, target: TaskTarget) {
        let Some(key) = target.key() else {
            return;
        };
        let index = key.index();
        let Some(task) = tasks.get(index).and_then(Option::as_ref) else {
            return;
        };
        if task.key != key || task.ready {
            return;
        }
        let connection_id = task.connection_id;
        let previous_head = ready.head(connection_id);
        if !ready.replace_head(connection_id, index) {
            debug_assert!(
                false,
                "ready connection map cannot exceed the task capacity"
            );
            return;
        }
        if let Some(previous_head) = previous_head
            && let Some(previous) = tasks.get_mut(previous_head).and_then(Option::as_mut)
        {
            previous.ready_previous = Some(index as u32);
        }
        let Some(task) = tasks.get_mut(index).and_then(Option::as_mut) else {
            return;
        };
        task.ready = true;
        task.ready_previous = None;
        task.ready_next = previous_head.map(|head| head as u32);
    }

    pub(super) fn pop_ready(&mut self, connection_id: Token) -> Option<ErasedTaskId> {
        let index = self.ready.head(connection_id)?;
        let task = self.tasks.get(index).and_then(Option::as_ref)?;
        if !task.ready || task.connection_id != connection_id {
            debug_assert!(false, "ready connection head must name its live task");
            self.ready.remove(connection_id);
            return None;
        }
        let key = task.key;
        let next = task.ready_next.map(|next| next as usize);
        if let Some(next) = next {
            if !self.ready.replace_head(connection_id, next) {
                debug_assert!(false, "existing ready connection must remain indexed");
                return None;
            }
            if let Some(next) = self.tasks.get_mut(next).and_then(Option::as_mut) {
                next.ready_previous = None;
            }
        } else {
            self.ready.remove(connection_id);
        }
        if let Some(task) = self.tasks.get_mut(index).and_then(Option::as_mut) {
            task.ready = false;
            task.ready_previous = None;
            task.ready_next = None;
        }
        Some(key)
    }

    pub(super) fn resume(
        &mut self,
        key: ErasedTaskId,
        connection_id: Token,
        task_head: &mut Option<u32>,
        driver: &mut DriverContext<'_, 'd>,
    ) -> Resumed<F::Output> {
        let index = key.index();
        let Some(running) = self.tasks.get(index).and_then(Option::as_ref) else {
            return Resumed::Stale;
        };
        if running.key != key || running.connection_id != connection_id {
            return Resumed::Stale;
        }
        let mut task = TaskPoll::new(self, task_head, RegisteredTask { index });
        match task.poll(driver) {
            Some(Poll::Ready(output)) => Resumed::Ready(task.complete(), output),
            Some(Poll::Pending) => {
                std::mem::forget(task);
                Resumed::Pending
            }
            None => {
                debug_assert!(false, "live task must exist in fiber slab");
                Resumed::Failed(task.complete())
            }
        }
    }

    pub(super) fn cancel(
        &mut self,
        task_head: &mut Option<u32>,
        connection_id: Token,
        stream_id: StreamId,
    ) {
        let Some(index) = self.task_map.get(connection_id, stream_id) else {
            return;
        };
        let _ = self.release_task(task_head, index);
    }

    pub(super) fn close(&mut self, task_head: &mut Option<u32>) {
        while let Some(index) = *task_head {
            if self.release_task(task_head, index as usize).is_none() {
                *task_head = None;
            }
        }
    }

    fn register_task(
        &mut self,
        task_head: &mut Option<u32>,
        index: usize,
        task: &mut Option<TaskId>,
        connection_id: Token,
        stream_id: StreamId,
        key: ErasedTaskId,
    ) -> bool {
        let Some(slot) = self.tasks.get(index) else {
            return false;
        };
        if slot.is_some() {
            return false;
        }
        if let Some(next) = *task_head {
            let Some(next_task) = self.tasks.get(next as usize).and_then(Option::as_ref) else {
                return false;
            };
            if next_task.connection_id != connection_id {
                return false;
            }
        }
        if !self.task_map.insert(connection_id, stream_id, index) {
            return false;
        }
        let Some(task) = task.take() else {
            self.task_map.remove(connection_id, stream_id);
            return false;
        };
        self.tasks[index] = Some(RunningTask {
            connection_id,
            stream_id,
            task,
            key,
            previous: None,
            next: *task_head,
            ready: false,
            ready_previous: None,
            ready_next: None,
        });
        let next = *task_head;
        *task_head = Some(index as u32);
        if let Some(next) = next
            && let Some(next) = self.tasks[next as usize].as_mut()
        {
            next.previous = Some(index as u32);
        }
        true
    }

    fn release_task(&mut self, task_head: &mut Option<u32>, index: usize) -> Option<StreamId> {
        self.unlink_ready(index);
        let running = self.tasks.get_mut(index)?.take()?;
        let RunningTask {
            connection_id,
            stream_id,
            task,
            previous,
            next,
            ..
        } = running;
        self.task_map.remove(connection_id, stream_id);
        if let Some(previous) = previous {
            if let Some(previous) = self.tasks[previous as usize].as_mut() {
                previous.next = next;
            }
        } else if *task_head == Some(index as u32) {
            *task_head = next;
        }
        if let Some(next) = next
            && let Some(next) = self.tasks[next as usize].as_mut()
        {
            next.previous = previous;
        }
        self.release_bound_task(index, task);
        Some(stream_id)
    }

    fn unlink_ready(&mut self, index: usize) {
        let Some(task) = self.tasks.get(index).and_then(Option::as_ref) else {
            return;
        };
        if !task.ready {
            return;
        }
        let connection_id = task.connection_id;
        let previous = task.ready_previous.map(|previous| previous as usize);
        let next = task.ready_next.map(|next| next as usize);
        if let Some(previous) = previous {
            if let Some(previous) = self.tasks.get_mut(previous).and_then(Option::as_mut) {
                previous.ready_next = next.map(|next| next as u32);
            }
        } else if let Some(next) = next {
            let replaced = self.ready.replace_head(connection_id, next);
            debug_assert!(replaced, "ready head must remain indexed");
        } else {
            self.ready.remove(connection_id);
        }
        if let Some(next) = next
            && let Some(next) = self.tasks.get_mut(next).and_then(Option::as_mut)
        {
            next.ready_previous = previous.map(|previous| previous as u32);
        }
        if let Some(task) = self.tasks.get_mut(index).and_then(Option::as_mut) {
            task.ready = false;
            task.ready_previous = None;
            task.ready_next = None;
        }
    }

    fn release_bound_task(&mut self, index: usize, task: TaskId) {
        debug_assert_eq!(index, task.index());
        let removed = self.slab.remove(task);
        debug_assert!(removed, "live task must be removable");
    }
}

impl<'d, F> Drop for Scheduler<'d, F>
where
    F: Fiber<'d> + 'd,
{
    fn drop(&mut self) {
        assert!(self.tasks.iter().all(Option::is_none));
        assert!(self.task_map.is_empty());
        assert!(self.ready.is_empty());
    }
}

trait TaskPollState<'d, F>
where
    F: Fiber<'d> + 'd,
{
    fn task<'a>(&'a self, tasks: &'a [Option<RunningTask>]) -> Option<&'a TaskId>;
    fn release(
        &mut self,
        scheduler: &mut Scheduler<'d, F>,
        task_head: &mut Option<u32>,
    ) -> Option<StreamId>;
}

struct NewTask {
    task: Option<TaskId>,
    index: usize,
}

impl NewTask {
    fn new(task: TaskId) -> Self {
        Self {
            index: task.index(),
            task: Some(task),
        }
    }
}

impl<'d, F> TaskPollState<'d, F> for NewTask
where
    F: Fiber<'d> + 'd,
{
    fn task<'a>(&'a self, _tasks: &'a [Option<RunningTask>]) -> Option<&'a TaskId> {
        self.task.as_ref()
    }

    fn release(
        &mut self,
        scheduler: &mut Scheduler<'d, F>,
        _task_head: &mut Option<u32>,
    ) -> Option<StreamId> {
        let task = self.task.take()?;
        scheduler.release_bound_task(self.index, task);
        None
    }
}

struct RegisteredTask {
    index: usize,
}

impl<'d, F> TaskPollState<'d, F> for RegisteredTask
where
    F: Fiber<'d> + 'd,
{
    fn task<'a>(&'a self, tasks: &'a [Option<RunningTask>]) -> Option<&'a TaskId> {
        tasks
            .get(self.index)
            .and_then(Option::as_ref)
            .map(|task| &task.task)
    }

    fn release(
        &mut self,
        scheduler: &mut Scheduler<'d, F>,
        task_head: &mut Option<u32>,
    ) -> Option<StreamId> {
        scheduler.release_task(task_head, self.index)
    }
}

struct TaskPoll<'a, 'd, F, S>
where
    F: Fiber<'d> + 'd,
    S: TaskPollState<'d, F>,
{
    scheduler: &'a mut Scheduler<'d, F>,
    task_head: &'a mut Option<u32>,
    state: S,
}

impl<'a, 'd, F, S> TaskPoll<'a, 'd, F, S>
where
    F: Fiber<'d> + 'd,
    S: TaskPollState<'d, F>,
{
    fn new(scheduler: &'a mut Scheduler<'d, F>, task_head: &'a mut Option<u32>, state: S) -> Self {
        Self {
            scheduler,
            task_head,
            state,
        }
    }

    fn poll(&mut self, driver: &mut DriverContext<'_, 'd>) -> Option<Poll<F::Output>> {
        let task = self.state.task(&self.scheduler.tasks)?;
        self.scheduler.slab.poll(task, driver)
    }

    fn complete(mut self) -> Option<StreamId> {
        let stream_id = self.state.release(self.scheduler, self.task_head);
        std::mem::forget(self);
        stream_id
    }
}

impl<'a, 'd, F> TaskPoll<'a, 'd, F, NewTask>
where
    F: Fiber<'d> + 'd,
{
    fn bind(&mut self, key: ErasedTaskId, parent: RootWaker<'d>) -> bool {
        let Some(task) = self.state.task.as_ref() else {
            return false;
        };
        self.scheduler
            .slab
            .bind(task, TaskTarget::task(key), parent)
    }

    fn register(mut self, connection_id: Token, stream_id: StreamId, key: ErasedTaskId) -> bool {
        let registered = self.scheduler.register_task(
            self.task_head,
            self.state.index,
            &mut self.state.task,
            connection_id,
            stream_id,
            key,
        );
        if registered {
            std::mem::forget(self);
        }
        registered
    }
}

impl<'d, F, S> Drop for TaskPoll<'_, 'd, F, S>
where
    F: Fiber<'d> + 'd,
    S: TaskPollState<'d, F>,
{
    fn drop(&mut self) {
        let _ = self.state.release(self.scheduler, self.task_head);
    }
}
