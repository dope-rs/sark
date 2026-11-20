use core::convert::Infallible;

use crate::decode::state;

/// Route payload family whose only inhabited states are open sync and task bodies.
pub trait Set:
    state::Set<
        SyncOpen = Self::Body,
        SyncFinished = Infallible,
        TaskOpen = Self::Body,
        TaskFinished = Infallible,
        DuplexOpen = Infallible,
        DuplexFinished = Infallible,
        UnsupportedOpen = Infallible,
        UnsupportedFinished = Infallible,
    >
{
    type Body;
}

/// Terminal operation that needs the selected route identity.
pub trait Fold<P, F: Set>: Sized {
    type Output;

    fn sync<S: state::Slot<P>>(self, slot: S, body: F::SyncOpen) -> Self::Output;

    fn task<S: state::Slot<P>>(self, slot: S, body: F::TaskOpen) -> Self::Output;
}
