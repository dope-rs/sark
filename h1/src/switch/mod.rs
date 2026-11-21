//! Linear HTTP/1 protocol switching state.

use core::{convert::Infallible, mem};

use sark_http::h1::switch::Kind;

mod engine;

pub use engine::Engine;

/// Policy for a connection that cannot leave HTTP mode.
#[derive(Clone, Copy, Default)]
pub struct Disabled;

/// Observable protocol mode for a switching-enabled connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode<R = Kind, S = Kind> {
    Http,
    Switching(S),
    Raw(R),
}

enum Phase {
    Switching,
    Raw,
}

/// Inline switching policy retaining the exact raw owner.
pub struct Enabled<D>(Option<(Phase, Kind, D)>);

/// Affine raw owner proposed by one successful switching route.
pub struct Transition<D> {
    pub(super) kind: Kind,
    pub(super) duplex: D,
}

/// Exact state that rejected an affine raw transition.
pub struct Error<D> {
    mode: Mode,
    next: Transition<D>,
}

impl<D> Default for Enabled<D> {
    fn default() -> Self {
        Self(None)
    }
}

impl<D> Enabled<D> {
    pub fn raw(&self) -> Result<&D, Mode<Infallible>> {
        match &self.0 {
            Some((Phase::Raw, _, duplex)) => Ok(duplex),
            Some((Phase::Switching, kind, _)) => Err(Mode::Switching(*kind)),
            None => Err(Mode::Http),
        }
    }

    pub fn raw_mut(&mut self) -> Result<&mut D, Mode<Infallible>> {
        match &mut self.0 {
            Some((Phase::Raw, _, duplex)) => Ok(duplex),
            Some((Phase::Switching, kind, _)) => Err(Mode::Switching(*kind)),
            None => Err(Mode::Http),
        }
    }

    /// Accesses staged raw state only for exact ingress attachment.
    #[doc(hidden)]
    pub fn staged_mut(&mut self) -> Result<&mut D, Mode<Kind, Infallible>> {
        match &mut self.0 {
            Some((Phase::Switching, _, duplex)) => Ok(duplex),
            Some((Phase::Raw, kind, _)) => Err(Mode::Raw(*kind)),
            None => Err(Mode::Http),
        }
    }
}

impl<D> Transition<D> {
    pub const fn new(kind: Kind, duplex: D) -> Self {
        Self { kind, duplex }
    }
}

impl<D> Error<D> {
    pub(super) const fn new(mode: Mode, next: Transition<D>) -> Self {
        Self { mode, next }
    }

    pub(crate) const fn mode(&self) -> Mode {
        self.mode
    }

    pub(crate) const fn attempted(&self) -> Kind {
        self.next.kind
    }
}

const _: () = {
    assert!(mem::size_of::<Disabled>() == 0);
    assert!(mem::size_of::<Mode<Infallible, Infallible>>() == 0);
    assert!(
        mem::size_of::<Result<&mut u64, Mode<Infallible, Infallible>>>()
            == mem::size_of::<&mut u64>()
    );
};
