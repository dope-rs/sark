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

pub enum Commit {
    Ordinary,
    Switch,
}

pub(super) enum State<D> {
    Http,
    Switching { kind: Kind, duplex: D },
    Raw { kind: Kind, duplex: D },
}

/// Inline switching policy retaining the exact raw owner.
pub struct Enabled<D> {
    pub(super) state: State<D>,
}

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
        Self { state: State::Http }
    }
}

impl<D> Enabled<D> {
    pub fn raw(&self) -> Result<&D, Mode<Infallible>> {
        match &self.state {
            State::Raw { duplex, .. } => Ok(duplex),
            State::Http => Err(Mode::Http),
            State::Switching { kind, .. } => Err(Mode::Switching(*kind)),
        }
    }

    pub fn raw_mut(&mut self) -> Result<&mut D, Mode<Infallible>> {
        match &mut self.state {
            State::Raw { duplex, .. } => Ok(duplex),
            State::Http => Err(Mode::Http),
            State::Switching { kind, .. } => Err(Mode::Switching(*kind)),
        }
    }

    pub(crate) fn active_mut(&mut self) -> Result<&mut D, Mode<Infallible, Infallible>> {
        match &mut self.state {
            State::Switching { duplex, .. } | State::Raw { duplex, .. } => Ok(duplex),
            State::Http => Err(Mode::Http),
        }
    }

    /// Accesses staged raw state only for exact ingress attachment.
    #[doc(hidden)]
    pub fn staged_mut(&mut self) -> Result<&mut D, Mode<Kind, Infallible>> {
        match &mut self.state {
            State::Switching { duplex, .. } => Ok(duplex),
            State::Http => Err(Mode::Http),
            State::Raw { kind, .. } => Err(Mode::Raw(*kind)),
        }
    }
}

impl<D> State<D> {
    fn commit(self) -> (Self, Commit) {
        match self {
            Self::Switching { kind, duplex } => (Self::Raw { kind, duplex }, Commit::Switch),
            state @ (Self::Http | Self::Raw { .. }) => (state, Commit::Ordinary),
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
