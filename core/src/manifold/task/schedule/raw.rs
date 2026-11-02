use core::pin;

use dope_core::driver::{retained, schedule::app};

use crate::manifold::task::schedule::{Context, start, wake};

/// Monomorphized task scheduler for `Domain`.
///
/// # Safety
/// Each pending identity names one resident task with matching coordinates; cancellation and completion destroy that task and its owned borrows before return, and no task outlives the scheduler. Observed polling selects only the requested owner's tasks and retains other owners' readiness.
pub unsafe trait Scheduler<'d, Domain, Owner, Target, Task, Output>
where
    Owner: Copy + Eq,
    Target: Copy,
{
    type Pending;

    fn start_with<Observe>(
        &mut self,
        task: Task,
        owner: Owner,
        target: Target,
        ctx: Context<'_, '_, '_, '_, 'd>,
        observe: Observe,
    ) -> Result<start::Start<Self::Pending, Output>, start::Error>
    where
        Observe: FnMut(Target, pin::Pin<&mut Task>);

    fn access<R>(
        &mut self,
        pending: &Self::Pending,
        access: impl FnOnce(pin::Pin<&mut Task>) -> R,
    ) -> Option<R>;

    fn drive_with<Observe>(
        &mut self,
        active: Owner,
        work: app::Work<'_, 'd>,
        driver: &mut retained::Context<'_, '_, 'd>,
        observe: Observe,
    ) where
        Observe: FnMut(Target, pin::Pin<&mut Task>);

    fn has_done(&self, owner: Owner) -> bool;

    fn pop_done(&mut self, owner: Owner) -> Option<(Target, Output)>;

    /// Consumes an identity branded to this scheduler and retires its task.
    fn cancel(&mut self, pending: Self::Pending);

    fn try_wake(&self, pending: &Self::Pending) -> Result<(), wake::Error>;

    /// Reports work for this owner, conservatively including unresolved wake ingress.
    fn has_work_for(&self, owner: Owner) -> bool;

    fn wake_next(&mut self);
}
