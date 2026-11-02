use core::{convert::Infallible, ops::ControlFlow};

use crate::decode::{dispatch, state};

type Decision<T, C, E, S> =
    ControlFlow<Result<T, (dispatch::Error<E, Infallible, Infallible>, C)>, S>;

/// Protocol payload transition for a route proved not to have a guard.
pub trait Visit<P, F: state::Set, G: state::Set> {
    type Break;

    fn sync_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::SyncOpen,
    ) -> ControlFlow<Self::Break, (S, G::SyncOpen)>;

    fn sync_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::SyncFinished,
    ) -> ControlFlow<Self::Break, (S, G::SyncFinished)>;

    fn task_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::TaskOpen,
    ) -> ControlFlow<Self::Break, (S, G::TaskOpen)>;

    fn task_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::TaskFinished,
    ) -> ControlFlow<Self::Break, (S, G::TaskFinished)>;

    fn duplex_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::DuplexOpen,
    ) -> ControlFlow<Self::Break, (S, G::DuplexOpen)>;

    fn duplex_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::DuplexFinished,
    ) -> ControlFlow<Self::Break, (S, G::DuplexFinished)>;

    fn reject_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::UnsupportedOpen,
    ) -> ControlFlow<Self::Break, (S, G::UnsupportedOpen)>;

    fn reject_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::UnsupportedFinished,
    ) -> ControlFlow<Self::Break, (S, G::UnsupportedFinished)>;
}

/// Protocol payload transition driven by an exact generated guard decision.
pub trait TryVisit<P, F: state::Set, G: state::Set, E> {
    type Break<T, C>;

    fn sync_open<S: state::Slot<P>, C, T>(
        &mut self,
        flow: Decision<T, C, E, S>,
        body: F::SyncOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, G::SyncOpen)>;

    fn sync_done<S: state::Slot<P>, C, T>(
        &mut self,
        flow: Decision<T, C, E, S>,
        body: F::SyncFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, G::SyncFinished)>;

    fn task_open<S: state::Slot<P>, C, T>(
        &mut self,
        flow: Decision<T, C, E, S>,
        body: F::TaskOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, G::TaskOpen)>;

    fn task_done<S: state::Slot<P>, C, T>(
        &mut self,
        flow: Decision<T, C, E, S>,
        body: F::TaskFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, G::TaskFinished)>;

    fn duplex_open<S: state::Slot<P>, C, T>(
        &mut self,
        flow: Decision<T, C, E, S>,
        body: F::DuplexOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, G::DuplexOpen)>;

    fn duplex_done<S: state::Slot<P>, C, T>(
        &mut self,
        flow: Decision<T, C, E, S>,
        body: F::DuplexFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, G::DuplexFinished)>;

    fn reject_open<S: state::Slot<P>, C, T>(
        &mut self,
        flow: Decision<T, C, E, S>,
        body: F::UnsupportedOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, G::UnsupportedOpen)>;

    fn reject_done<S: state::Slot<P>, C, T>(
        &mut self,
        flow: Decision<T, C, E, S>,
        body: F::UnsupportedFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, G::UnsupportedFinished)>;
}

impl<P> Visit<P, state::Routed, state::Admitted> for state::Pass {
    type Break = Infallible;

    fn sync_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: <state::Routed as state::Set>::SyncOpen,
    ) -> ControlFlow<Self::Break, (S, <state::Admitted as state::Set>::SyncOpen)> {
        ControlFlow::Continue((slot, body))
    }

    fn sync_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: <state::Routed as state::Set>::SyncFinished,
    ) -> ControlFlow<Self::Break, (S, <state::Admitted as state::Set>::SyncFinished)> {
        ControlFlow::Continue((slot, body))
    }

    fn task_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: <state::Routed as state::Set>::TaskOpen,
    ) -> ControlFlow<Self::Break, (S, <state::Admitted as state::Set>::TaskOpen)> {
        ControlFlow::Continue((slot, body))
    }

    fn task_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: <state::Routed as state::Set>::TaskFinished,
    ) -> ControlFlow<Self::Break, (S, <state::Admitted as state::Set>::TaskFinished)> {
        ControlFlow::Continue((slot, body))
    }

    fn duplex_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: <state::Routed as state::Set>::DuplexOpen,
    ) -> ControlFlow<Self::Break, (S, <state::Admitted as state::Set>::DuplexOpen)> {
        ControlFlow::Continue((slot, body))
    }

    fn duplex_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: <state::Routed as state::Set>::DuplexFinished,
    ) -> ControlFlow<Self::Break, (S, <state::Admitted as state::Set>::DuplexFinished)> {
        ControlFlow::Continue((slot, body))
    }

    fn reject_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: <state::Routed as state::Set>::UnsupportedOpen,
    ) -> ControlFlow<Self::Break, (S, <state::Admitted as state::Set>::UnsupportedOpen)> {
        ControlFlow::Continue((slot, body))
    }

    fn reject_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: <state::Routed as state::Set>::UnsupportedFinished,
    ) -> ControlFlow<Self::Break, (S, <state::Admitted as state::Set>::UnsupportedFinished)> {
        ControlFlow::Continue((slot, body))
    }
}

impl<P, E> TryVisit<P, state::Routed, state::Admitted, E> for state::Pass {
    type Break<T, C> = Result<T, (dispatch::Error<E, Infallible, Infallible>, C)>;

    fn sync_open<S: state::Slot<P>, C, T>(
        &mut self,
        flow: ControlFlow<Self::Break<T, C>, S>,
        body: <state::Routed as state::Set>::SyncOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, <state::Admitted as state::Set>::SyncOpen)> {
        flow.map_continue(|slot| (slot, body))
    }

    fn sync_done<S: state::Slot<P>, C, T>(
        &mut self,
        flow: ControlFlow<Self::Break<T, C>, S>,
        body: <state::Routed as state::Set>::SyncFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, <state::Admitted as state::Set>::SyncFinished)> {
        flow.map_continue(|slot| (slot, body))
    }

    fn task_open<S: state::Slot<P>, C, T>(
        &mut self,
        flow: ControlFlow<Self::Break<T, C>, S>,
        body: <state::Routed as state::Set>::TaskOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, <state::Admitted as state::Set>::TaskOpen)> {
        flow.map_continue(|slot| (slot, body))
    }

    fn task_done<S: state::Slot<P>, C, T>(
        &mut self,
        flow: ControlFlow<Self::Break<T, C>, S>,
        body: <state::Routed as state::Set>::TaskFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, <state::Admitted as state::Set>::TaskFinished)> {
        flow.map_continue(|slot| (slot, body))
    }

    fn duplex_open<S: state::Slot<P>, C, T>(
        &mut self,
        flow: ControlFlow<Self::Break<T, C>, S>,
        body: <state::Routed as state::Set>::DuplexOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, <state::Admitted as state::Set>::DuplexOpen)> {
        flow.map_continue(|slot| (slot, body))
    }

    fn duplex_done<S: state::Slot<P>, C, T>(
        &mut self,
        flow: ControlFlow<Self::Break<T, C>, S>,
        body: <state::Routed as state::Set>::DuplexFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, <state::Admitted as state::Set>::DuplexFinished)> {
        flow.map_continue(|slot| (slot, body))
    }

    fn reject_open<S: state::Slot<P>, C, T>(
        &mut self,
        flow: ControlFlow<Self::Break<T, C>, S>,
        body: <state::Routed as state::Set>::UnsupportedOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, <state::Admitted as state::Set>::UnsupportedOpen)> {
        flow.map_continue(|slot| (slot, body))
    }

    fn reject_done<S: state::Slot<P>, C, T>(
        &mut self,
        flow: ControlFlow<Self::Break<T, C>, S>,
        body: <state::Routed as state::Set>::UnsupportedFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, <state::Admitted as state::Set>::UnsupportedFinished)>
    {
        flow.map_continue(|slot| (slot, body))
    }
}
