use std::{marker, num::NonZero};

use dope::core::driver::{
    route::KeyTag,
    schedule::{clock::timer, ready::task::owner},
};
use dope_fiber::{
    abi,
    task::storage::{fixed, scope},
};
use fixed::{
    direct, quota,
    schedule::{Plan, Plane, queue},
    tree,
};
use manifold::task::{self, request::Input, response::Output};
use sark_core::{manifold, route};
use sark_grpc::server::{
    mode::schedule::{Streaming, Unary},
    runtime::{state::active, stream::Domain, unary},
    service::task::{stream, unary::Fiber},
};
use task::schedule;

use crate::manifold::executor;

/// Marks routes executed by the fiber runtime.
pub struct NativeFiber;

/// Selects generated route storage without changing its task identity.
#[doc(hidden)]
pub trait Storage {
    type Target: Copy;
    type Clock<'d>;
    type Protocol<Cold, Bound>;
    type Resource<Bound>;

    type Tasks<'s, 'd, D, S>
    where
        D: tree::Tree<'d>,
        S: tree::Tree<'d>;

    type DirectOnly<'s, 'd, D>
    where
        D: tree::Tree<'d>;
}

/// Constructs an application's initial task storage.
#[doc(hidden)]
pub trait Initial: Storage {
    fn protocol<Cold, Bound>(cold: Cold) -> Self::Protocol<Cold, Bound>;

    fn resource<Bound>() -> Self::Resource<Bound>;

    fn clock<'d>() -> Self::Clock<'d>;

    fn tasks<'s, 'd, D, S>(
        limit: quota::Limit,
        brand: scope::Brand<'s>,
    ) -> Self::Tasks<'s, 'd, D, S>
    where
        D: tree::Tree<'d>,
        S: tree::Tree<'d>;

    fn direct_only<'s, 'd, D>(
        limit: quota::Limit,
        brand: scope::Brand<'s>,
    ) -> Self::DirectOnly<'s, 'd, D>
    where
        D: tree::Tree<'d>;
}

/// Marks storage whose scheduler identity is executable.
#[doc(hidden)]
pub trait Runnable: Storage {
    fn handle<'d>(clock: &Self::Clock<'d>) -> timer::Timer<'d>;
}

#[doc(hidden)]
pub struct Direct;

#[doc(hidden)]
pub struct Cold;

#[doc(hidden)]
pub struct Empty;

impl Storage for Direct {
    type Target = ();
    type Clock<'d> = timer::Timer<'d>;
    type Protocol<Cold, Bound> = Bound;
    type Resource<Bound> = Bound;

    type Tasks<'s, 'd, D, S>
        = scope::Owned<'s, direct::Plane<'d, D, quota::Bounded>>
    where
        D: tree::Tree<'d>,
        S: tree::Tree<'d>;

    type DirectOnly<'s, 'd, D>
        = scope::Owned<'s, direct::Plane<'d, D, quota::Bounded>>
    where
        D: tree::Tree<'d>;
}

impl Storage for Cold {
    type Target = ();
    type Clock<'d> = Empty;
    type Protocol<Cold, Bound> = Cold;
    type Resource<Bound> = Empty;

    type Tasks<'s, 'd, D, S>
        = (scope::Brand<'s>, direct::Plan<'d, D>)
    where
        D: tree::Tree<'d>,
        S: tree::Tree<'d>;

    type DirectOnly<'s, 'd, D>
        = (scope::Brand<'s>, direct::Plan<'d, D>)
    where
        D: tree::Tree<'d>;
}

impl Initial for Cold {
    fn protocol<Cold, Bound>(cold: Cold) -> Self::Protocol<Cold, Bound> {
        cold
    }

    fn resource<Bound>() -> Self::Resource<Bound> {
        Empty
    }

    fn clock<'d>() -> Self::Clock<'d> {
        Empty
    }

    fn tasks<'s, 'd, D, S>(
        limit: quota::Limit,
        brand: scope::Brand<'s>,
    ) -> Self::Tasks<'s, 'd, D, S>
    where
        D: tree::Tree<'d>,
        S: tree::Tree<'d>,
    {
        (brand, direct::Plan::new(limit))
    }

    fn direct_only<'s, 'd, D>(
        limit: quota::Limit,
        brand: scope::Brand<'s>,
    ) -> Self::DirectOnly<'s, 'd, D>
    where
        D: tree::Tree<'d>,
    {
        (brand, direct::Plan::new(limit))
    }
}

impl Runnable for Direct {
    fn handle<'d>(clock: &Self::Clock<'d>) -> timer::Timer<'d> {
        *clock
    }
}

#[doc(hidden)]
pub struct Multi<T: task::Target>(marker::PhantomData<fn() -> T>);

#[doc(hidden)]
pub struct Prepared<T: task::Target>(marker::PhantomData<fn() -> T>);

impl<T: task::Target> Storage for Multi<T> {
    type Target = T;
    type Clock<'d> = Empty;
    type Protocol<Cold, Bound> = Empty;
    type Resource<Bound> = Empty;

    type Tasks<'s, 'd, D, S>
        = (scope::Brand<'s>, Plan<'d, S>)
    where
        D: tree::Tree<'d>,
        S: tree::Tree<'d>;

    type DirectOnly<'s, 'd, D>
        = Empty
    where
        D: tree::Tree<'d>;
}

impl<T: task::Target> Initial for Multi<T> {
    fn protocol<Cold, Bound>(_: Cold) -> Self::Protocol<Cold, Bound> {
        Empty
    }

    fn resource<Bound>() -> Self::Resource<Bound> {
        Empty
    }

    fn clock<'d>() -> Self::Clock<'d> {
        Empty
    }

    fn tasks<'s, 'd, D, S>(
        limit: quota::Limit,
        brand: scope::Brand<'s>,
    ) -> Self::Tasks<'s, 'd, D, S>
    where
        D: tree::Tree<'d>,
        S: tree::Tree<'d>,
    {
        (brand, Plan::new(limit))
    }

    fn direct_only<'s, 'd, D>(_: quota::Limit, _: scope::Brand<'s>) -> Self::DirectOnly<'s, 'd, D>
    where
        D: tree::Tree<'d>,
    {
        Empty
    }
}

impl<T: task::Target> Storage for Prepared<T> {
    type Target = T;
    type Clock<'d> = timer::Timer<'d>;
    type Protocol<Cold, Bound> = ();
    type Resource<Bound> = Bound;

    type Tasks<'s, 'd, D, S>
        = scope::Owned<'s, Plane<'d, S, quota::Bounded, queue::Owned<'d, T>>>
    where
        D: tree::Tree<'d>,
        S: tree::Tree<'d>;

    type DirectOnly<'s, 'd, D>
        = Empty
    where
        D: tree::Tree<'d>;
}

impl<T: task::Target> Runnable for Prepared<T> {
    fn handle<'d>(clock: &Self::Clock<'d>) -> timer::Timer<'d> {
        *clock
    }
}

pub trait TaskRoute<'d, State>: route::Task<'d, State, Request: route::Own> + Sized {
    fn invoke_task<'req>(
        input: (Input<'d, Self>, &'req State, timer::Timer<'d>),
    ) -> impl abi::Fiber<'d, Output = Output<'d, Self, State>> + 'req
    where
        'd: 'req;
}

impl<H> Unary<H, KeyTag<0>> for NativeFiber
where
    H: Fiber,
{
    type Task<'life, 'd>
        = H::Fiber<'life, 'd>
    where
        'd: 'life,
        Self: 'life,
        H: 'life;
    type Scheduler<'s, 'r, 'life, 'd>
        = executor::Scheduler<'s, 'd, unary::Domain, H::Fiber<'life, 'd>, active::Entry<'r>>
    where
        'r: 'life,
        'd: 'life,
        Self: 'life,
        H: 'life;

    fn with<'r, 'life, 'd, O>(
        capacity: NonZero<u32>,
        owners: owner::Capacity,
        run: impl for<'s> FnOnce(Self::Scheduler<'s, 'r, 'life, 'd>) -> O,
    ) -> Result<O, schedule::Error>
    where
        'r: 'life,
        'd: 'life,
        Self: 'life,
        H: 'life,
    {
        executor::with(capacity, owners, |tasks| run(executor::Scheduler { tasks }))
    }
}

impl<H> Streaming<H, KeyTag<0>> for NativeFiber
where
    H: stream::Handler,
{
    type Scheduler<'s, 'r, 'life, 'd>
        = executor::Scheduler<
        's,
        'd,
        Domain,
        stream::Fiber<'d, H::Task<'life, 'd>, H::Request<'d>, H::Response, H::Codec>,
        active::Entry<'r>,
    >
    where
        'r: 'life,
        'd: 'life,
        Self: 'life,
        H: 'life;

    fn with<'r, 'life, 'd, O>(
        capacity: NonZero<u32>,
        owners: owner::Capacity,
        run: impl for<'s> FnOnce(Self::Scheduler<'s, 'r, 'life, 'd>) -> O,
    ) -> Result<O, schedule::Error>
    where
        'r: 'life,
        'd: 'life,
        Self: 'life,
        H: 'life,
    {
        executor::with(capacity, owners, |tasks| run(executor::Scheduler { tasks }))
    }
}

const _: () = assert!(std::mem::size_of::<Empty>() == 0);
