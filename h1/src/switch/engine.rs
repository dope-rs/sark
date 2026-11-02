//! Static switching policy implementations.

use core::{convert::Infallible, mem};

use crate::switch;

/// Static switching policy implemented only by canonical modes.
pub trait Engine {
    type Raw;
    /// Evidence that a transition was admitted; absent for HTTP-only mode.
    type Staged;

    fn mode(&self) -> switch::Mode;
    fn try_stage(
        &mut self,
        next: switch::Transition<Self::Raw>,
    ) -> Result<Self::Staged, switch::Error<Self::Raw>>;
    fn commit(&mut self) -> switch::Commit;
}

impl Engine for switch::Disabled {
    type Raw = Infallible;
    type Staged = Infallible;

    fn mode(&self) -> switch::Mode {
        switch::Mode::Http
    }

    fn try_stage(
        &mut self,
        next: switch::Transition<Self::Raw>,
    ) -> Result<Self::Staged, switch::Error<Self::Raw>> {
        match next.duplex {}
    }

    fn commit(&mut self) -> switch::Commit {
        switch::Commit::Ordinary
    }
}

impl<D> Engine for switch::Enabled<D> {
    type Raw = D;
    type Staged = ();

    fn mode(&self) -> switch::Mode {
        match &self.state {
            switch::State::Http => switch::Mode::Http,
            switch::State::Switching { kind, .. } => switch::Mode::Switching(*kind),
            switch::State::Raw { kind, .. } => switch::Mode::Raw(*kind),
        }
    }

    fn try_stage(
        &mut self,
        next: switch::Transition<Self::Raw>,
    ) -> Result<Self::Staged, switch::Error<Self::Raw>> {
        let mode = self.mode();
        if !matches!(mode, switch::Mode::Http) {
            return Err(switch::Error::new(mode, next));
        }
        let switch::Transition { kind, duplex } = next;
        self.state = switch::State::Switching { kind, duplex };
        Ok(())
    }

    fn commit(&mut self) -> switch::Commit {
        let (state, commit) = mem::replace(&mut self.state, switch::State::Http).commit();
        self.state = state;
        commit
    }
}
