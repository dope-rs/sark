use core::{convert::Infallible, marker::PhantomData, ops::ControlFlow};

use o3::buffer::{resident, segments};
use sark_core::decode::{
    request,
    state::{self, admit},
};

use crate::server::{
    payload::Buffer,
    state::exchange::{Accepted, Input},
};

pub(crate) struct Plain<'d>(PhantomData<&'d ()>);

impl<'d> Plain<'d> {
    pub(crate) const fn new() -> Self {
        Self(PhantomData)
    }

    pub(crate) fn admit<P, F, T>(
        mut self,
        request: T,
    ) -> <P::Family as state::Family<P>>::State<Accepted<'d>>
    where
        P: request::Prepared,
        F: Input<'d>,
        T: state::Admission<
                P,
                F,
                Accepted<'d>,
                Output = <P::Family as state::Family<P>>::State<Accepted<'d>>,
            >,
    {
        match state::Admission::admit(request, &mut self) {
            ControlFlow::Continue(request) => request,
            ControlFlow::Break(never) => match never {},
        }
    }
}

impl<'d, P, F> admit::Visit<P, F, Accepted<'d>> for Plain<'d>
where
    F: Input<'d>,
{
    type Break = Infallible;

    fn sync_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::SyncOpen,
    ) -> ControlFlow<Self::Break, (S, Buffer<'d>)> {
        ControlFlow::Continue((slot, F::sync(body)))
    }

    fn sync_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::SyncFinished,
    ) -> ControlFlow<Self::Break, (S, Buffer<'d>)> {
        ControlFlow::Continue((slot, F::sync_done(body)))
    }

    fn task_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::TaskOpen,
    ) -> ControlFlow<Self::Break, (S, Buffer<'d>)> {
        ControlFlow::Continue((slot, F::task(body)))
    }

    fn task_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::TaskFinished,
    ) -> ControlFlow<Self::Break, (S, Buffer<'d>)> {
        ControlFlow::Continue((slot, F::task_done(body)))
    }

    fn duplex_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::DuplexOpen,
    ) -> ControlFlow<Self::Break, (S, Option<segments::Chain<resident::Shared<'d>>>)> {
        ControlFlow::Continue((slot, F::duplex(body)))
    }

    fn duplex_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::DuplexFinished,
    ) -> ControlFlow<Self::Break, (S, Option<segments::Chain<resident::Shared<'d>>>)> {
        ControlFlow::Continue((slot, F::duplex_done(body)))
    }

    fn reject_open<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::UnsupportedOpen,
    ) -> ControlFlow<Self::Break, (S, Buffer<'d>)> {
        ControlFlow::Continue((slot, F::other(body)))
    }

    fn reject_done<S: state::Slot<P>>(
        &mut self,
        slot: S,
        body: F::UnsupportedFinished,
    ) -> ControlFlow<Self::Break, (S, Buffer<'d>)> {
        ControlFlow::Continue((slot, F::other_done(body)))
    }
}

const _: () = assert!(core::mem::size_of::<Plain<'_>>() == 0);
