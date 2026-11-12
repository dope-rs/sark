//! Fixed-capacity, driver-local execution for multiplexed tasks.

use core::{num::NonZero, pin};

use crate::server::runtime::schedule::{self, start};
use dope::{
    core::driver::{
        self,
        route::KeyTag,
        schedule::{app::Work, ready::task::owner},
    },
    manifold::network::listener::conn::Id,
};
use dope_fiber::{abi, context::wake::RootWaker, task::storage::set::group};
use sark_core::manifold::server::drain::Progress;

mod task;

pub(in crate::server) type Tasks<'s, 'd, D, F, T> =
    group::Tasks<'s, 'd, F, task::Task<'d, T>, task::Group, D>;

pub(in crate::server) fn with<'d, D, F: abi::Fiber<'d>, T: Copy, O>(
    capacity: NonZero<u32>,
    owners: owner::Capacity,
    run: impl for<'s> FnOnce(Tasks<'s, 'd, D, F, T>) -> O,
) -> Result<O, schedule::Error> {
    group::Tasks::with(capacity, owners, task::Group, run).map_err(|error| match error {
        group::Error::Capacity(error) => schedule::Error::Layout(error),
        group::Error::Tasks(error) => schedule::Error::Tasks(error),
        group::Error::Orders(error) => schedule::Error::Orders(error),
        group::Error::Ready(error) => schedule::Error::Ready(error),
    })
}

pub(in crate::server) fn start_with<'s, 'd, D, F, T, Observe>(
    tasks: &mut Tasks<'s, 'd, D, F, T>,
    task: F,
    owner: Id<'d, KeyTag<0>>,
    target: T,
    ctx: schedule::Context<'_, '_, '_, 'd>,
    observe: Observe,
) -> Result<start::Start<group::Id<'s, 'd, D>, F::Output>, start::Error>
where
    F: abi::Fiber<'d>,
    T: Copy,
    Observe: FnMut(T, pin::Pin<&mut F>),
{
    let parent = RootWaker::from(ctx.parent);
    let id = tasks.try_insert(task, task::Task { owner, target }, parent)?;
    let _ = drive_with(tasks, owner, ctx.work, ctx.driver, observe);
    let started = match tasks.take(id) {
        Ok((_, output)) => start::Start::Ready(output),
        Err(id) => start::Start::Pending(id),
    };
    if tasks.contains(owner) {
        parent.wake();
    }
    Ok(started)
}

pub(in crate::server) fn drive_with<'s, 'd, D, F, T, Observe>(
    tasks: &mut Tasks<'s, 'd, D, F, T>,
    active: Id<'d, KeyTag<0>>,
    work: Work<'_, 'd>,
    driver: &mut driver::Context<'_, 'd>,
    mut observe: Observe,
) -> Progress
where
    F: abi::Fiber<'d>,
    T: Copy,
    Observe: FnMut(T, pin::Pin<&mut F>),
{
    let mut progress = Progress::default();
    tasks.poll_with(active, work, driver, |task, fiber| {
        progress.record();
        observe(task.target, fiber);
    });
    while tasks.release_done(|_| ()).is_some() {}
    progress
}
