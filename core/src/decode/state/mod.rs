use core::{convert::Infallible, marker::PhantomData, ops::ControlFlow};

use crate::{
    body::admit::{Finished, Open, Track},
    decode::{exec, request},
};

pub mod admit;
pub mod body;
pub mod open;
mod uniform;

pub use uniform::Uniform;

pub struct Uniforms<P>(PhantomData<fn() -> P>);

/// Payload family carried by each route state.
pub trait Set {
    type SyncOpen;
    type SyncFinished;
    type TaskOpen;
    type TaskFinished;
    type DuplexOpen;
    type DuplexFinished;
    type UnsupportedOpen;
    type UnsupportedFinished;
}

/// Route selected before its guard runs.
pub enum Routed {}

impl Set for Routed {
    type SyncOpen = Track<Open>;
    type SyncFinished = Track<Finished>;
    type TaskOpen = Track<Open>;
    type TaskFinished = Track<Finished>;
    type DuplexOpen = ();
    type DuplexFinished = ();
    type UnsupportedOpen = Track<Open>;
    type UnsupportedFinished = Track<Finished>;
}

/// Route selected after its guard admits it.
pub enum Admitted {}

impl Set for Admitted {
    type SyncOpen = Track<Open>;
    type SyncFinished = Track<Finished>;
    type TaskOpen = Track<Open>;
    type TaskFinished = Track<Finished>;
    type DuplexOpen = ();
    type DuplexFinished = ();
    type UnsupportedOpen = Track<Open>;
    type UnsupportedFinished = Track<Finished>;
}

/// Payload conversion performed when transport input reaches its terminal edge.
///
/// The generated route family owns the only runtime discriminant. Implementors
/// move the payload without allocating or introducing a second phase tag.
pub trait Finish: Set {
    fn sync(body: Self::SyncOpen) -> Self::SyncFinished;

    fn task(body: Self::TaskOpen) -> Self::TaskFinished;

    fn duplex(body: Self::DuplexOpen) -> Self::DuplexFinished;

    fn reject(body: Self::UnsupportedOpen) -> Self::UnsupportedFinished;
}

/// One tagless route payload belonging to an aggregate prepared request.
pub trait Slot<P>: Sized {
    fn prepare(self) -> P;

    fn admit<K, B>(self, body: B) -> request::Admitted<K, P, B> {
        request::Admitted::new(self.prepare(), body)
    }
}

pub struct Whole<P>(P);

impl<P> Whole<P> {
    pub const fn new(prep: P) -> Self {
        Self(prep)
    }
}

impl<P> Slot<P> for Whole<P> {
    fn prepare(self) -> P {
        self.0
    }
}

/// Consuming route-state transformation.
pub trait Map<P, F: Set, G: Set>: Sized {
    type Break;
    type Retained;

    fn sync_open<S: Slot<P>>(
        self,
        slot: S,
        body: F::SyncOpen,
    ) -> ControlFlow<Self::Break, (Self::Retained, S, G::SyncOpen)>;

    fn sync_done<S: Slot<P>>(
        self,
        slot: S,
        body: F::SyncFinished,
    ) -> ControlFlow<Self::Break, (Self::Retained, S, G::SyncFinished)>;

    fn task_open<S: Slot<P>>(
        self,
        slot: S,
        body: F::TaskOpen,
    ) -> ControlFlow<Self::Break, (Self::Retained, S, G::TaskOpen)>;

    fn task_done<S: Slot<P>>(
        self,
        slot: S,
        body: F::TaskFinished,
    ) -> ControlFlow<Self::Break, (Self::Retained, S, G::TaskFinished)>;

    fn duplex_open<S: Slot<P>>(
        self,
        slot: S,
        body: F::DuplexOpen,
    ) -> ControlFlow<Self::Break, (Self::Retained, S, G::DuplexOpen)>;

    fn duplex_done<S: Slot<P>>(
        self,
        slot: S,
        body: F::DuplexFinished,
    ) -> ControlFlow<Self::Break, (Self::Retained, S, G::DuplexFinished)>;

    fn reject_open<S: Slot<P>>(
        self,
        slot: S,
        body: F::UnsupportedOpen,
    ) -> ControlFlow<Self::Break, (Self::Retained, S, G::UnsupportedOpen)>;

    fn reject_done<S: Slot<P>>(
        self,
        slot: S,
        body: F::UnsupportedFinished,
    ) -> ControlFlow<Self::Break, (Self::Retained, S, G::UnsupportedFinished)>;
}

/// Zero-state payload-preserving transition.
pub struct Pass;

impl<P, F, G> Map<P, F, G> for Pass
where
    F: Set,
    G: Set<
            SyncOpen = F::SyncOpen,
            SyncFinished = F::SyncFinished,
            TaskOpen = F::TaskOpen,
            TaskFinished = F::TaskFinished,
            DuplexOpen = F::DuplexOpen,
            DuplexFinished = F::DuplexFinished,
            UnsupportedOpen = F::UnsupportedOpen,
            UnsupportedFinished = F::UnsupportedFinished,
        >,
{
    type Break = Infallible;
    type Retained = ();

    fn sync_open<S: Slot<P>>(
        self,
        slot: S,
        body: F::SyncOpen,
    ) -> ControlFlow<Infallible, ((), S, F::SyncOpen)> {
        ControlFlow::Continue(((), slot, body))
    }

    fn sync_done<S: Slot<P>>(
        self,
        slot: S,
        body: F::SyncFinished,
    ) -> ControlFlow<Infallible, ((), S, F::SyncFinished)> {
        ControlFlow::Continue(((), slot, body))
    }

    fn task_open<S: Slot<P>>(
        self,
        slot: S,
        body: F::TaskOpen,
    ) -> ControlFlow<Infallible, ((), S, F::TaskOpen)> {
        ControlFlow::Continue(((), slot, body))
    }

    fn task_done<S: Slot<P>>(
        self,
        slot: S,
        body: F::TaskFinished,
    ) -> ControlFlow<Infallible, ((), S, F::TaskFinished)> {
        ControlFlow::Continue(((), slot, body))
    }

    fn duplex_open<S: Slot<P>>(
        self,
        slot: S,
        body: F::DuplexOpen,
    ) -> ControlFlow<Infallible, ((), S, F::DuplexOpen)> {
        ControlFlow::Continue(((), slot, body))
    }

    fn duplex_done<S: Slot<P>>(
        self,
        slot: S,
        body: F::DuplexFinished,
    ) -> ControlFlow<Infallible, ((), S, F::DuplexFinished)> {
        ControlFlow::Continue(((), slot, body))
    }

    fn reject_open<S: Slot<P>>(
        self,
        slot: S,
        body: F::UnsupportedOpen,
    ) -> ControlFlow<Infallible, ((), S, F::UnsupportedOpen)> {
        ControlFlow::Continue(((), slot, body))
    }

    fn reject_done<S: Slot<P>>(
        self,
        slot: S,
        body: F::UnsupportedFinished,
    ) -> ControlFlow<Infallible, ((), S, F::UnsupportedFinished)> {
        ControlFlow::Continue(((), slot, body))
    }
}

/// Borrowed operation over one selected route state.
pub trait Each<P, F: Set>: Sized {
    type Output<'a>;

    fn sync_open<'a, S: Slot<P>>(self, slot: &S, body: &'a mut F::SyncOpen) -> Self::Output<'a>;

    fn sync_done<'a, S: Slot<P>>(self, slot: &S, body: &'a mut F::SyncFinished)
    -> Self::Output<'a>;

    fn task_open<'a, S: Slot<P>>(self, slot: &S, body: &'a mut F::TaskOpen) -> Self::Output<'a>;

    fn task_done<'a, S: Slot<P>>(self, slot: &S, body: &'a mut F::TaskFinished)
    -> Self::Output<'a>;

    fn duplex_open<'a, S: Slot<P>>(self, slot: &S, body: &'a mut F::DuplexOpen)
    -> Self::Output<'a>;

    fn duplex_done<'a, S: Slot<P>>(
        self,
        slot: &S,
        body: &'a mut F::DuplexFinished,
    ) -> Self::Output<'a>;

    fn reject_open<'a, S: Slot<P>>(
        self,
        slot: &S,
        body: &'a mut F::UnsupportedOpen,
    ) -> Self::Output<'a>;

    fn reject_done<'a, S: Slot<P>>(
        self,
        slot: &S,
        body: &'a mut F::UnsupportedFinished,
    ) -> Self::Output<'a>;
}

/// Terminal consumption of one selected route state.
///
/// Unlike [`Map`], a fold cannot produce another route state and therefore
/// needs neither an uninhabited payload family nor a `ControlFlow` fallback.
pub trait Fold<P, F: Set>: Sized {
    type Output;

    fn sync_open<S: Slot<P>>(self, slot: S, body: F::SyncOpen) -> Self::Output;

    fn sync_done<S: Slot<P>>(self, slot: S, body: F::SyncFinished) -> Self::Output;

    fn task_open<S: Slot<P>>(self, slot: S, body: F::TaskOpen) -> Self::Output;

    fn task_done<S: Slot<P>>(self, slot: S, body: F::TaskFinished) -> Self::Output;

    fn duplex_open<S: Slot<P>>(self, slot: S, body: F::DuplexOpen) -> Self::Output;

    fn duplex_done<S: Slot<P>>(self, slot: S, body: F::DuplexFinished) -> Self::Output;

    fn reject_open<S: Slot<P>>(self, slot: S, body: F::UnsupportedOpen) -> Self::Output;

    fn reject_done<S: Slot<P>>(self, slot: S, body: F::UnsupportedFinished) -> Self::Output;
}

/// One type constructor for every state of the same generated route enum.
pub trait Family<P>: Sized {
    type State<F: Set>: Flow<P, F, Family = Self> + body::Flow<F>;
}

/// A route family whose every arm has the same execution kind.
///
/// Generated homogeneous routers implement this capability directly, so a
/// protocol restricted to one execution kind never needs mismatch arms.
pub trait Homogeneous<P, K: exec::Kind>: Family<P> {
    fn fold<F, T>(state: Self::State<F>, fold: T) -> T::Output
    where
        F: Set,
        T: exec::Fold<P, F, K>;
}

impl<P> Family<P> for Uniforms<P> {
    type State<F: Set> = Uniform<P, F>;
}

impl<P> Homogeneous<P, exec::Sync> for Uniforms<P> {
    fn fold<F, T>(state: Uniform<P, F>, fold: T) -> T::Output
    where
        F: Set,
        T: exec::Fold<P, F, exec::Sync>,
    {
        let (prep, body) = state.into_parts();
        match body {
            request::Body::Open(body) => fold.open(Whole::new(prep), body),
            request::Body::Finished(body) => fold.finished(Whole::new(prep), body),
        }
    }
}

type Mapped<B, R, S, P, G> = ControlFlow<B, (R, <S as Family<P>>::State<G>)>;

/// A generated route enum carrying exactly one route discriminant.
pub trait Flow<P, F: Set>: Sized {
    type Family: Family<P>;

    fn each<'a, M: Each<P, F>>(&'a mut self, each: M) -> M::Output<'a>;

    fn map_retain<G: Set, M: Map<P, F, G>>(
        self,
        map: M,
    ) -> Mapped<M::Break, M::Retained, Self::Family, P, G>;

    fn map<G: Set, M: Map<P, F, G, Retained = ()>>(
        self,
        map: M,
    ) -> ControlFlow<M::Break, <Self::Family as Family<P>>::State<G>> {
        self.map_retain(map).map_continue(|((), state)| state)
    }

    fn fold<T: Fold<P, F>>(self, fold: T) -> T::Output;

    /// Retags an open route arm as finished while retaining the same route,
    /// slot, payload owner, and generated-family discriminant storage.
    fn finish(self) -> Result<Self, Self>
    where
        F: Finish;

    fn each_open<'a, V: open::Each<F>>(&'a mut self, visit: V) -> V::Output<'a>
    where
        F: open::Set;

    fn fold_open<V: open::Fold<P, F>>(self, visit: V) -> V::Output
    where
        F: open::Set;

    fn consume<O, V: FnOnce(F::Body) -> O>(self, visit: V) -> O
    where
        F: open::Set;
}

/// Consuming unguarded transition into the application's admitted family.
pub trait Admission<P, F: Set, G: Set>: Sized {
    type Output;

    fn admit<V: admit::Visit<P, F, G>>(self, visit: &mut V) -> ControlFlow<V::Break, Self::Output>;
}
