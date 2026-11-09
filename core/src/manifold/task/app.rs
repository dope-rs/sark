use std::{convert::Infallible, fmt, mem, pin};

use app::step;
use dope_core::driver;
use driver::schedule::{app, ready};
use ready::task::owner;
use request::admit;
use sark_http::{payload::owned, request::Id};

use crate::{
    decode,
    decode::{dispatch, exec, request},
    duplex,
    manifold::task::{self, Target, response},
    route,
};

/// Binds generated task storage to one driver domain.
pub trait Bind<'d>: Sized
where
    <Self::Runtime as decode::Decode>::Egress<'d>: 'd,
{
    type Target: Target + 'd;
    type Runtime: Runtime<'d, Target = Self::Target>;
    type Error;

    fn try_bind(self, bind: driver::Binding<'d>) -> Result<Self::Runtime, Self::Error>;
}

/// Result of polling one generated multiplexed task.
#[must_use]
#[repr(u8)]
pub enum Turn<'turn, 'd, C = ()> {
    Idle,
    Deferred,
    Pending(step::Receipt<'turn, 'd>),
    PendingReady(step::Receipt<'turn, 'd>),
    Ready(C, step::Receipt<'turn, 'd>),
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Wake {
    Ready,
    Idle,
}

/// Linear monomorphized handoff into one protocol response adapter.
pub trait Handoff<'d, T: Target, F: response::Completion<'d>>: Sized {
    type Ready;

    fn pending(self, target: T);

    /// Transfers the target's sole fixed response after releasing its runtime slot and quota.
    /// This is mutually exclusive with `ready(Event::Head)` for the same target.
    fn complete(self, target: T, output: F) -> Self::Ready;

    fn ready<E: fmt::Debug>(self, target: T, event: response::Event<'d, E>) -> Self::Ready;
}

/// Allocation-free task execution selected independently of the protocol.
pub trait Runtime<'d>:
    decode::Decode<Head: request::Head<Start: admit::Start<Self>>, Egress<'d>: 'd>
    + duplex::Application<'d>
    + Sized
{
    type Task;
    type Fixed: response::Completion<'d>;
    type Build: crate::body::Push<'d>;
    type Target: Target + 'd;
    type Admission;

    /// Exact unadmitted reason for one owner-lane registration.
    type OwnerCause: fmt::Debug;

    /// Exact reason one owner lane could not be retired.
    type RetireCause: fmt::Debug;

    fn reject_step(error: <Self::Build as crate::body::Build<'d>>::StepError) -> route::Reject;

    fn reject_push(error: <Self::Build as crate::body::Push<'d>>::Error) -> route::Reject;

    fn rejection(error: &task::Error<Self::Admission>) -> route::Reject;

    /// Binds the lane owner of one admitted session, classifying a lane this runtime
    /// does not own or a generation another registration already claimed.
    fn register(
        self: pin::Pin<&Self>,
        owner: owner::Owner<Self::Target>,
    ) -> Result<(), Self::OwnerCause>;

    /// Releases the lane owner of one closed session, classifying a foreign or stale
    /// lane and a generation whose tasks never retired.
    fn try_retire(
        self: pin::Pin<&Self>,
        owner: owner::Owner<Self::Target>,
    ) -> Result<(), Self::RetireCause>;

    fn try_dispatch(
        self: pin::Pin<&mut Self>,
        req: request::Ready<exec::Task, Self::Prepared, owned::Segments<'d>>,
        target: Self::Target,
        owner: owner::Owner<Self::Target>,
        parent: ready::Target<'d>,
    ) -> Result<Self::Task, task::Error<Self::Admission>>;

    fn try_start(
        &self,
        req: Self::Prepared,
    ) -> Result<Self::Build, dispatch::Error<Infallible, Infallible, route::Reject>>;

    fn try_built(
        self: pin::Pin<&mut Self>,
        output: <Self::Build as crate::body::Build<'d>>::Output,
        target: Self::Target,
        owner: owner::Owner<Self::Target>,
        parent: ready::Target<'d>,
    ) -> Result<Self::Task, task::Error<Self::Admission>>;

    fn drive<'turn, H: Handoff<'d, Self::Target, Self::Fixed>>(
        self: pin::Pin<&mut Self>,
        scope: owner::Scope<Self::Target>,
        work: app::Work<'turn, 'd>,
        driver: &mut driver::Context<'_, 'd>,
        sink: H,
    ) -> Turn<'turn, 'd, H::Ready>;

    fn emit<E: decode::Streaming<'d, 'd>>(
        self: pin::Pin<&mut Self>,
        task: &Self::Task,
        make: impl FnOnce(Id<'d>) -> E,
    ) -> response::Access<response::Prepared<'d, E>>;

    fn resume(self: pin::Pin<&mut Self>, task: &Self::Task) -> response::Access<()>;

    fn has_work(self: pin::Pin<&Self>, scope: owner::Scope<Self::Target>) -> bool;

    fn wake(self: pin::Pin<&mut Self>, scope: owner::Scope<Self::Target>) {
        let _ = Self::try_wake(self, scope);
    }

    fn try_wake(self: pin::Pin<&mut Self>, scope: owner::Scope<Self::Target>) -> Wake;

    fn cancel(self: pin::Pin<&mut Self>, task: Self::Task);
}

const _: () = assert!(mem::size_of::<Turn<'static, 'static>>() == 1);
