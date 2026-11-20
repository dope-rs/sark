use core::ops::ControlFlow;

use crate::{
    body::admit::{Finished, Open, Track},
    decode::state,
};

pub trait Payload:
    Bodies<SyncOpen: From<Track<Open>>, SyncFinished: From<Track<Finished>>, DuplexOpen: Default>
{
}

impl Payload for state::Admitted {}

pub trait Bodies:
    state::Set<
        TaskOpen = <Self as state::Set>::SyncOpen,
        TaskFinished = <Self as state::Set>::SyncFinished,
        UnsupportedOpen = <Self as state::Set>::SyncOpen,
        UnsupportedFinished = <Self as state::Set>::SyncFinished,
        DuplexFinished = <Self as state::Set>::DuplexOpen,
    >
{
}

impl Bodies for state::Routed {}
impl Bodies for state::Admitted {}

pub trait Resolve<F: Bodies>: Bodies {
    type Stopped<T>;

    fn open<T, S>(
        outcome: ControlFlow<T, S>,
        body: F::SyncOpen,
    ) -> ControlFlow<Self::Stopped<T>, (S, Self::SyncOpen)>;

    fn finished<T, S>(
        outcome: ControlFlow<T, S>,
        body: F::SyncFinished,
    ) -> ControlFlow<Self::Stopped<T>, (S, Self::SyncFinished)>;

    fn duplex<T, S>(
        outcome: ControlFlow<T, S>,
        body: F::DuplexOpen,
    ) -> ControlFlow<Self::Stopped<T>, (S, Self::DuplexOpen)>;
}

impl Resolve<state::Routed> for state::Admitted {
    type Stopped<T> = T;

    fn open<T, S>(
        outcome: ControlFlow<T, S>,
        body: Track<Open>,
    ) -> ControlFlow<T, (S, Track<Open>)> {
        ControlFlow::Continue((outcome?, body))
    }

    fn finished<T, S>(
        outcome: ControlFlow<T, S>,
        body: Track<Finished>,
    ) -> ControlFlow<T, (S, Track<Finished>)> {
        ControlFlow::Continue((outcome?, body))
    }

    fn duplex<T, S>(outcome: ControlFlow<T, S>, (): ()) -> ControlFlow<T, (S, ())> {
        ControlFlow::Continue((outcome?, ()))
    }
}
