use crate::decode::state;

/// One route payload retaining its exact execution and completion state.
pub enum Value<F: state::Set> {
    SyncOpen(F::SyncOpen),
    SyncFinished(F::SyncFinished),
    TaskOpen(F::TaskOpen),
    TaskFinished(F::TaskFinished),
    DuplexOpen(F::DuplexOpen),
    DuplexFinished(F::DuplexFinished),
    UnsupportedOpen(F::UnsupportedOpen),
    UnsupportedFinished(F::UnsupportedFinished),
}

/// Terminal operation over route payloads that does not consume route identity.
pub trait Fold<F: state::Set>: Sized {
    type Output;

    fn apply(self, value: Value<F>) -> Self::Output;
}

/// Consuming route dispatch that explicitly discards route identity.
pub trait Flow<F: state::Set>: Sized {
    fn fold<V: Fold<F>>(self, visit: V) -> V::Output;
}
