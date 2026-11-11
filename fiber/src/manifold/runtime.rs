use std::marker;

use dope::core::driver::schedule::clock::timer;
use dope_fiber::{
    abi,
    task::storage::{fixed, scope},
};
use manifold::task::{self, request::Input, response::Output};
use o3::depot::{arena::slab::storage::Heap, capacity::Count};
use sark_core::{manifold, route};

/// Marks routes executed by the fiber runtime.
pub struct NativeFiber;

/// Selects generated route storage without changing its task identity.
#[doc(hidden)]
pub trait Storage {
    type Target: Copy;
    type Protocol<Cold, Bound>;
    type Resource<Bound>;
    type Quota<Direct, Queued>;

    type Tasks<'s, 'd, DF, SF, D, M>;
    type DirectOnly<'s, 'd, F, D>;
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
    type Quota<Direct, Queued> = Direct;

    type Tasks<'s, 'd, DF, SF, D, M> =
        scope::Owned<'s, fixed::Slab<'d, DF, { u32::MAX as usize }, D, Heap>>;
    type DirectOnly<'s, 'd, F, D> =
        scope::Owned<'s, fixed::Slab<'d, F, { u32::MAX as usize }, D, Heap>>;
}

impl Storage for Cold {
    type Target = ();
    type Protocol<Cold, Bound> = Cold;
    type Resource<Bound> = ();
    type Quota<Direct, Queued> = ();

    type Tasks<'s, 'd, DF, SF, D, M> = (scope::Brand<'s>, Count);
    type DirectOnly<'s, 'd, F, D> = (scope::Brand<'s>, Count);
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
    type Quota<Direct, Queued> = ();

    type Tasks<'s, 'd, DF, SF, D, M> = (scope::Brand<'s>, Count);
    type DirectOnly<'s, 'd, F, D> = ();
}

impl<T: task::Target> Storage for Prepared<T> {
    type Target = T;
    type Protocol<Cold, Bound> = ();
    type Resource<Bound> = Bound;
    type Quota<Direct, Queued> = Queued;

    type Tasks<'s, 'd, DF, SF, D, M> = scope::Owned<
        's,
        fixed::schedule::Storage<'d, SF, D, T, M, fixed::schedule::queue::Owned<'d, T>>,
    >;
    type DirectOnly<'s, 'd, F, D> = ();
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
