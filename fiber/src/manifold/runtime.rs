use std::marker;

use dope::core::driver::schedule::clock::timer;
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

/// Marks routes executed by the fiber runtime.
pub struct NativeFiber;

/// Selects generated route storage without changing its task identity.
#[doc(hidden)]
pub trait Storage {
    type Target: Copy;
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
    fn handle<'d>(clock: &Self::Resource<timer::Timer<'d>>) -> timer::Timer<'d>;
}

#[doc(hidden)]
pub struct Direct;

#[doc(hidden)]
pub struct Cold;

impl Storage for Direct {
    type Target = ();
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
    type Protocol<Cold, Bound> = Cold;
    type Resource<Bound> = ();

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

    fn resource<Bound>() -> Self::Resource<Bound> {}

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
    fn handle<'d>(clock: &Self::Resource<timer::Timer<'d>>) -> timer::Timer<'d> {
        *clock
    }
}

#[doc(hidden)]
pub struct Multi<T: task::Target>(marker::PhantomData<fn() -> T>);

#[doc(hidden)]
pub struct Prepared<T: task::Target>(marker::PhantomData<fn() -> T>);

impl<T: task::Target> Storage for Multi<T> {
    type Target = T;
    type Protocol<Cold, Bound> = ();
    type Resource<Bound> = ();

    type Tasks<'s, 'd, D, S>
        = (scope::Brand<'s>, Plan<'d, S>)
    where
        D: tree::Tree<'d>,
        S: tree::Tree<'d>;

    type DirectOnly<'s, 'd, D>
        = ()
    where
        D: tree::Tree<'d>;
}

impl<T: task::Target> Initial for Multi<T> {
    fn protocol<Cold, Bound>(_: Cold) -> Self::Protocol<Cold, Bound> {}

    fn resource<Bound>() -> Self::Resource<Bound> {}

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
    {}
}

impl<T: task::Target> Storage for Prepared<T> {
    type Target = T;
    type Protocol<Cold, Bound> = ();
    type Resource<Bound> = Bound;

    type Tasks<'s, 'd, D, S>
        = scope::Owned<'s, Plane<'d, S, quota::Bounded, queue::Owned<'d, T>>>
    where
        D: tree::Tree<'d>,
        S: tree::Tree<'d>;

    type DirectOnly<'s, 'd, D>
        = ()
    where
        D: tree::Tree<'d>;
}

impl<T: task::Target> Runnable for Prepared<T> {
    fn handle<'d>(clock: &Self::Resource<timer::Timer<'d>>) -> timer::Timer<'d> {
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
