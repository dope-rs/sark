//! Fixed-capacity, driver-local execution for multiplexed tasks.

use core::{num::NonZero, pin, task::Poll};

use crate::server::runtime::schedule::{self, start};
use dope::{
    core::driver::{
        self,
        route::KeyTag,
        schedule::{app::Work, ready::task::owner},
    },
    manifold::network::listener::conn::Id,
};
use dope_fiber::{
    abi,
    context::wake::RootWaker,
    task::storage::{scope::Scope, set},
};
use sark_core::manifold::server::drain::Progress;

pub(in crate::server) type Tasks<'s, 'd, D, F, T> =
    Scope<'s, pin::Pin<&'s mut set::Tasks<'d, F, T, D>>>;

pub(in crate::server) fn with<'d, D, F: abi::Fiber<'d>, T: Copy, O>(
    capacity: NonZero<u32>,
    owners: owner::Capacity,
    run: impl for<'s> FnOnce(Tasks<'s, 'd, D, F, T>) -> O,
) -> Result<O, schedule::Error> {
    set::Tasks::with(capacity, owners, run)
}

pub(in crate::server) fn start_with<'s, 'd, D, F, T, Observe>(
    tasks: &mut Tasks<'s, 'd, D, F, T>,
    task: F,
    owner: Id<'d, KeyTag<0>>,
    target: T,
    ctx: schedule::Context<'_, '_, '_, 'd>,
    observe: Observe,
) -> Result<start::Start<set::Id<'s, 'd, D>, F::Output>, start::Error>
where
    F: abi::Fiber<'d>,
    T: Copy,
    Observe: FnMut(T, pin::Pin<&mut F>, Poll<()>),
{
    let parent = RootWaker::from(ctx.parent);
    let id = tasks.try_insert(task, target, parent, owner)?;
    let _ = drive_with(tasks, owner, ctx.work, ctx.driver, observe);
    Ok(match tasks.take(id) {
        Ok(output) => start::Start::Ready(output),
        Err(id) => start::Start::Pending(id),
    })
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
    Observe: FnMut(T, pin::Pin<&mut F>, Poll<()>),
{
    let mut progress = Progress::default();
    tasks.poll_with(active, work, driver, |target, fiber, status| {
        progress.record();
        observe(*target, fiber, status);
    });
    progress
}
