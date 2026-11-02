//! Fixed-capacity, driver-local execution for multiplexed tasks.

use core::{num::NonZero, pin};

use crate::server::runtime::schedule::{self, start, wake};
use dope::{
    core::driver::{
        retained,
        route::KeyTag,
        schedule::{app::Work, ready::task::owner},
    },
    manifold::network::listener::conn::Id,
};
use dope_fiber::{abi, context::wake::RootWaker, task::storage};
use set::group;
use storage::set;

mod task;

pub(in crate::server) fn with<'d, D, F: abi::Fiber<'d>, T: Copy, O>(
    capacity: NonZero<u32>,
    owners: owner::Capacity,
    run: impl for<'s> FnOnce(Scheduler<'s, 'd, D, F, T>) -> O,
) -> Result<O, schedule::Error> {
    group::Tasks::with(capacity, owners, task::Group, |tasks| {
        run(Scheduler { tasks })
    })
    .map_err(|error| match error {
        group::Error::Capacity(error) => schedule::Error::Layout(error),
        group::Error::Tasks(error) => schedule::Error::Tasks(error),
        group::Error::Orders(error) => schedule::Error::Orders(error),
        group::Error::Ready(error) => schedule::Error::Ready(error),
    })
}

/// Fixed-capacity task scheduler for one driver domain.
///
/// `group::Id` retains the invariant allocation scope of `tasks`. Completion
/// and cancellation remove each pinned fiber; ownership destroys remaining
/// resident fibers before their storage is released.
pub(in crate::server) struct Scheduler<'s, 'd, D, F, T>
where
    F: abi::Fiber<'d>,
    T: Copy,
{
    tasks: group::Tasks<'s, 'd, F, task::Task<'d, T>, task::Group, D>,
}

impl<'s, 'd, D, F, T> Scheduler<'s, 'd, D, F, T>
where
    F: abi::Fiber<'d>,
    T: Copy,
{
    fn collect(&mut self, active: Id<'d, KeyTag<0>>) {
        while let Some((arrival, (conn, parent))) =
            self.tasks.release_done(|task| (task.owner, task.parent))
        {
            if matches!(arrival, group::Arrival::First) && conn != active {
                parent.wake();
            }
        }
    }

    pub(in crate::server) fn start_with<Observe>(
        &mut self,
        task: F,
        owner: Id<'d, KeyTag<0>>,
        target: T,
        ctx: schedule::Context<'_, '_, '_, '_, 'd>,
        observe: Observe,
    ) -> Result<start::Start<group::Id<'s, 'd, D>, F::Output>, start::Error>
    where
        Observe: FnMut(T, pin::Pin<&mut F>),
    {
        let parent = RootWaker::from(ctx.parent);
        let id = self.tasks.try_insert(
            task,
            task::Task {
                owner,
                target,
                parent,
            },
            parent,
        )?;
        self.drive_with(owner, ctx.work, ctx.driver, observe);
        let started = match self.tasks.take(id) {
            Ok((_, output)) => start::Start::Ready(output),
            Err(id) => start::Start::Pending(id),
        };
        if self.tasks.contains(owner) {
            parent.wake();
        }
        Ok(started)
    }

    pub(in crate::server) fn access<R>(
        &mut self,
        pending: &group::Id<'s, 'd, D>,
        access: impl FnOnce(pin::Pin<&mut F>) -> R,
    ) -> Option<R> {
        self.tasks.access(pending, access)
    }

    pub(in crate::server) fn drive_with<Observe>(
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

    pub(in crate::server) fn has_done(&self, owner: Id<'d, KeyTag<0>>) -> bool {
        self.tasks.contains(owner)
    }

    pub(in crate::server) fn pop_done(
        &mut self,
        owner: Id<'d, KeyTag<0>>,
    ) -> Option<(T, F::Output)> {
        self.tasks
            .pop(owner)
            .map(|(task, output)| (task.target, output))
    }

    pub(in crate::server) fn cancel(&mut self, pending: group::Id<'s, 'd, D>) {
        self.tasks.discard(pending);
    }

    pub(in crate::server) fn try_wake(
        &self,
        pending: &group::Id<'s, 'd, D>,
    ) -> Result<(), wake::Error> {
        self.tasks.try_wake(pending)
    }

    pub(in crate::server) fn has_work_for(&self, owner: Id<'d, KeyTag<0>>) -> bool {
        matches!(self.tasks.work_for(owner), set::Work::Pending)
    }

    pub(in crate::server) fn wake_next(&mut self) {
        self.tasks.wake_next();
    }
}
