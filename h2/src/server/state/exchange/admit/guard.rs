use core::{convert::Infallible, marker::PhantomData, ops::ControlFlow};

use o3::buffer::{resident, segments};
use sark_core::decode::{dispatch, state, state::admit};

use crate::{
    conn,
    server::{
        payload::Buffer,
        state::exchange::{Accepted, Input, Rejected},
    },
};

type Duplex<'d> = Option<segments::Chain<resident::Shared<'d>>>;

type Dispatch<T, C> = Result<T, (dispatch::Error<conn::Error, Infallible, Infallible>, C)>;

pub(crate) struct Guard<'d>(PhantomData<&'d ()>);

impl<'d> Guard<'d> {
    pub(crate) const fn new() -> Self {
        Self(PhantomData)
    }

    fn body<S, T, C>(
        &mut self,
        outcome: ControlFlow<Dispatch<T, C>, S>,
        body: Buffer<'d>,
    ) -> ControlFlow<(Dispatch<T, C>, Rejected<'d>), (S, Buffer<'d>)> {
        match outcome {
            ControlFlow::Continue(slot) => ControlFlow::Continue((slot, body)),
            ControlFlow::Break(outcome) => ControlFlow::Break((outcome, Rejected::Body(body))),
        }
    }
}

impl<'d, P, F> admit::TryVisit<P, F, Accepted<'d>, conn::Error> for Guard<'d>
where
    F: Input<'d>,
{
    type Break<T, C> = (Dispatch<T, C>, Rejected<'d>);

    fn sync_open<S: state::Slot<P>, C, T>(
        &mut self,
        outcome: ControlFlow<Dispatch<T, C>, S>,
        body: F::SyncOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, Buffer<'d>)> {
        self.body(outcome, F::sync(body))
    }

    fn sync_done<S: state::Slot<P>, C, T>(
        &mut self,
        outcome: ControlFlow<Dispatch<T, C>, S>,
        body: F::SyncFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, Buffer<'d>)> {
        self.body(outcome, F::sync_done(body))
    }

    fn task_open<S: state::Slot<P>, C, T>(
        &mut self,
        outcome: ControlFlow<Dispatch<T, C>, S>,
        body: F::TaskOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, Buffer<'d>)> {
        self.body(outcome, F::task(body))
    }

    fn task_done<S: state::Slot<P>, C, T>(
        &mut self,
        outcome: ControlFlow<Dispatch<T, C>, S>,
        body: F::TaskFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, Buffer<'d>)> {
        self.body(outcome, F::task_done(body))
    }

    /// Releases a rejected duplex selection through the close hook.
    ///
    /// A rejected selection never delivers its body, which is the exact case
    /// the hook exists for, so no duplex storage reaches the failure value.
    fn duplex_open<S: state::Slot<P>, C, T>(
        &mut self,
        outcome: ControlFlow<Dispatch<T, C>, S>,
        body: F::DuplexOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, Duplex<'d>)> {
        match outcome {
            ControlFlow::Continue(slot) => ControlFlow::Continue((slot, F::duplex(body))),
            ControlFlow::Break(outcome) => {
                F::close_duplex(body);
                ControlFlow::Break((outcome, Rejected::Duplex))
            }
        }
    }

    /// Releases a rejected finished duplex selection through the end hook.
    ///
    /// Symmetric with `duplex_open`: a rejected selection never delivers the
    /// finished body either.
    fn duplex_done<S: state::Slot<P>, C, T>(
        &mut self,
        outcome: ControlFlow<Dispatch<T, C>, S>,
        body: F::DuplexFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, Duplex<'d>)> {
        match outcome {
            ControlFlow::Continue(slot) => ControlFlow::Continue((slot, F::duplex_done(body))),
            ControlFlow::Break(outcome) => {
                F::end_duplex(body);
                ControlFlow::Break((outcome, Rejected::Duplex))
            }
        }
    }

    fn reject_open<S: state::Slot<P>, C, T>(
        &mut self,
        outcome: ControlFlow<Dispatch<T, C>, S>,
        body: F::UnsupportedOpen,
    ) -> ControlFlow<Self::Break<T, C>, (S, Buffer<'d>)> {
        self.body(outcome, F::other(body))
    }

    fn reject_done<S: state::Slot<P>, C, T>(
        &mut self,
        outcome: ControlFlow<Dispatch<T, C>, S>,
        body: F::UnsupportedFinished,
    ) -> ControlFlow<Self::Break<T, C>, (S, Buffer<'d>)> {
        self.body(outcome, F::other_done(body))
    }
}

const _: () = assert!(core::mem::size_of::<Guard<'_>>() == 0);
