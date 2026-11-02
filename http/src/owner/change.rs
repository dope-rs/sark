use crate::owner::cause;

/// The state change carried by one response-owner effect.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Change {
    StagingAcquired(cause::Acquire),
    Handoff,
    StagingReleased(cause::Staging),
    TransportReleased(cause::Transport),
}
