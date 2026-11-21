//! Static switching policy implementations.

use core::convert::Infallible;

use crate::switch::{self, Phase};

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
}

impl<D> Engine for switch::Enabled<D> {
    type Raw = D;
    type Staged = ();

    fn mode(&self) -> switch::Mode {
        match self.0 {
            None => switch::Mode::Http,
            Some((Phase::Switching, kind, _)) => switch::Mode::Switching(kind),
            Some((Phase::Raw, kind, _)) => switch::Mode::Raw(kind),
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
        self.0 = Some((Phase::Switching, next.kind, next.duplex));
        Ok(())
    }
}

impl<D> switch::Enabled<D> {
    pub(crate) fn commit(&mut self) -> Option<&mut D> {
        let (phase @ Phase::Switching, _, raw) = self.0.as_mut()? else {
            return None;
        };
        *phase = Phase::Raw;
        Some(raw)
    }
}
