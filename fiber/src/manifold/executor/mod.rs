//! Fixed-capacity, driver-local execution for multiplexed tasks.

use core::{num::NonZero, pin};

use dope::{
    core::driver::{
        retained,
        route::KeyTag,
        schedule::{app::Work, ready::task::owner},
    },
    manifold::network::listener::conn::Id,
};
use dope_fiber::{abi, context::wake::RootWaker, task::storage};
use manifold::task::{schedule, schedule::start};
use sark_core::{manifold, manifold::task::schedule::wake};
use set::group;
use storage::set;

pub(in crate::manifold) mod task;

pub(in crate::manifold) fn with<'d, D, F: abi::Fiber<'d>, T: Copy, O>(
    capacity: NonZero<u32>,
    owners: owner::Capacity,
    run: impl for<'s> FnOnce(group::Tasks<'s, 'd, F, task::Task<'d, T>, task::Group, D>) -> O,
) -> Result<O, schedule::Error> {
    group::Tasks::with(capacity, owners, task::Group, run).map_err(|error| match error {
        group::Error::Capacity(error) => schedule::Error::Layout(error),
        group::Error::Tasks(error) => schedule::Error::Tasks(error),
        group::Error::Orders(error) => schedule::Error::Orders(error),
        group::Error::Ready(error) => schedule::Error::Ready(error),
    })
}

/// Fixed-capacity task scheduler for one driver domain.
pub struct Scheduler<'s, 'd, D, F, T>
where
    F: abi::Fiber<'d>,
    T: Copy,
{
    pub(in crate::manifold) tasks: group::Tasks<'s, 'd, F, task::Task<'d, T>, task::Group, D>,
}

impl<'s, 'd, D, F, T> Scheduler<'s, 'd, D, F, T>
where
    F: abi::Fiber<'d>,
    T: Copy,
{
    fn insert(
        &mut self,
        fiber: F,
        conn: Id<'d, KeyTag<0>>,
        target: T,
        parent: RootWaker<'d>,
    ) -> Result<group::Id<'s, 'd, D>, start::Error> {
        use set::insert::Error;

        let task = task::Task {
            owner: conn,
            target,
            parent,
        };
        self.tasks
            .try_insert(fiber, task, parent)
            .map_err(|error| match error {
                Error::Full => start::Error::Full,
                Error::Admission(error) => start::Error::Admission(error),
                Error::Topology(error) => start::Error::Topology(error),
                Error::Owner(error) => start::Error::Owner(error),
            })
    }

    fn started(
        &mut self,
        id: group::Id<'s, 'd, D>,
        conn: Id<'d, KeyTag<0>>,
        parent: RootWaker<'d>,
    ) -> start::Start<group::Id<'s, 'd, D>, F::Output> {
        let started = match self.tasks.take(id) {
            Ok((_, output)) => start::Start::Ready(output),
            Err(id) => start::Start::Pending(id),
        };
        if self.tasks.contains(conn) {
            parent.wake();
        }
        started
    }

    fn collect(&mut self, active: Id<'d, KeyTag<0>>) {
        while let Some((arrival, (conn, parent))) =
            self.tasks.release_done(|task| (task.owner, task.parent))
        {
            if matches!(arrival, group::Arrival::First) && conn != active {
                parent.wake();
            }
        }
    }
}

// SAFETY: group::Id retains the invariant allocation scope of `tasks`.
// Completion and cancellation remove each pinned fiber; scheduler ownership
// destroys any remaining resident fibers before its storage is released.
unsafe impl<'s, 'd, D, F, T, O> schedule::raw::Scheduler<'d, D, Id<'d, KeyTag<0>>, T, F, O>
    for Scheduler<'s, 'd, D, F, T>
where
    F: abi::Fiber<'d, Output = O>,
    T: Copy,
{
    type Pending = group::Id<'s, 'd, D>;

    fn start_with<Observe>(
        &mut self,
        task: F,
        owner: Id<'d, KeyTag<0>>,
        target: T,
        ctx: schedule::Context<'_, '_, '_, '_, 'd>,
        observe: Observe,
    ) -> Result<start::Start<Self::Pending, O>, start::Error>
    where
        Observe: FnMut(T, pin::Pin<&mut F>),
    {
        let parent = RootWaker::from(ctx.parent);
        let id = self.insert(task, owner, target, parent)?;
        self.drive_with(owner, ctx.work, ctx.driver, observe);
        Ok(self.started(id, owner, parent))
    }

    fn access<R>(
        &mut self,
        pending: &Self::Pending,
        access: impl FnOnce(pin::Pin<&mut F>) -> R,
    ) -> Option<R> {
        self.tasks.access(pending, access)
    }

    fn drive_with<Observe>(
        &mut self,
        active: Id<'d, KeyTag<0>>,
        work: Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
        mut observe: Observe,
    ) where
        Observe: FnMut(T, pin::Pin<&mut F>),
    {
        self.tasks.poll_with(active, work, driver, |task, fiber| {
            observe(task.target, fiber)
        });
        self.collect(active);
    }

    fn has_done(&self, owner: Id<'d, KeyTag<0>>) -> bool {
        self.tasks.contains(owner)
    }

    fn pop_done(&mut self, owner: Id<'d, KeyTag<0>>) -> Option<(T, O)> {
        self.tasks
            .pop(owner)
            .map(|(task, output)| (task.target, output))
    }

    fn cancel(&mut self, pending: Self::Pending) {
        self.tasks.discard(pending);
    }

    fn try_wake(&self, pending: &Self::Pending) -> Result<(), wake::Error> {
        match self.tasks.try_wake(pending) {
            Ok(()) => Ok(()),
            Err(set::WakeError::Missing) => Err(wake::Error::Missing),
            Err(set::WakeError::Inactive) => Err(wake::Error::Inactive),
        }
    }

    fn has_work_for(&self, owner: Id<'d, KeyTag<0>>) -> bool {
        matches!(self.tasks.work_for(owner), set::Work::Pending)
    }

    fn wake_next(&mut self) {
        self.tasks.wake_next();
    }
}
