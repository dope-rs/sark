use std::mem;

use dope::{
    core::{
        driver::{
            route::{Identity, KeyTag},
            schedule::ready,
        },
        random,
    },
    manifold::client::{close, conn},
    net::link::pool::change::opening,
};
use sark_h2::client;

use crate::{
    client::{
        manifold::{conn::active::Active, error},
        state::plan,
    },
    protocol::section,
};

pub(in crate::client::manifold) enum Phase<'d, ID: Identity> {
    Vacant(Option<error::Error>),
    Active {
        active: Active<'d, ID>,
        terminal: Option<error::Error>,
    },
    Retired {
        active: Active<'d, ID>,
        terminal: Option<error::Error>,
    },
}

const _: () = {
    assert!(
        mem::size_of::<Phase<'static, KeyTag<0>>>()
            == mem::size_of::<(
                Active<'static, KeyTag<0>>,
                Option<error::Error>,
                mem::Discriminant<Phase<'static, KeyTag<0>>>,
            )>()
    );
    assert!(
        mem::size_of::<Option<Phase<'static, KeyTag<0>>>>()
            == mem::size_of::<Phase<'static, KeyTag<0>>>()
    );
};

impl<'d, ID: Identity> Phase<'d, ID> {
    pub(super) const fn new() -> Self {
        Self::Vacant(None)
    }

    pub(super) fn connect(
        &mut self,
        lifetime: conn::Lifetime<'d, ID>,
        ready: ready::Target<'d>,
        protocol: plan::Plan<client::Plan<section::Plan>>,
        seed: &random::Seed,
    ) -> Result<(), close::Reason> {
        if matches!(self, Self::Active { .. } | Self::Retired { .. }) {
            return Err(close::Reason::Local(close::LocalOrigin::Superseded));
        }
        match Active::new(lifetime, ready, protocol, seed) {
            Ok(active) => {
                *self = Self::Active {
                    active,
                    terminal: None,
                };
                Ok(())
            }
            Err(error) => {
                let reason = close::Reason::Capacity(close::Capacity::Transport);
                *self = Self::Vacant(Some(error::Error::Create(error)));
                Err(reason)
            }
        }
    }

    pub(in crate::client::manifold) fn open(&self) -> Option<conn::Id<'d, ID>> {
        match self {
            Self::Active { active, .. } => Some(active.connection()),
            Self::Vacant(_) | Self::Retired { .. } => None,
        }
    }

    pub(in crate::client::manifold) fn has_output(&self, conn: conn::Id<'d, ID>) -> bool {
        if let Self::Active { active, .. } | Self::Retired { active, .. } = self
            && active.connection() == conn
        {
            return active.has_output();
        }
        false
    }

    pub(in crate::client::manifold) fn owner_cap(&self) -> usize {
        if let Self::Active { active, .. } = self {
            return active.owner_cap();
        }
        0
    }

    pub(in crate::client::manifold) fn unavailable(&mut self) -> error::Error {
        match self.terminal() {
            Some(error) => error,
            None => error::Error::NotConnected,
        }
    }

    pub(in crate::client::manifold) fn terminal(&mut self) -> Option<error::Error> {
        let cause = match self {
            Self::Vacant(cause)
            | Self::Active {
                terminal: cause, ..
            }
            | Self::Retired {
                terminal: cause, ..
            } => cause,
        };
        Self::take(cause)
    }

    fn take(cause: &mut Option<error::Error>) -> Option<error::Error> {
        match cause {
            Some(error::Error::Exhausted(cause)) => Some(error::Error::Exhausted(*cause)),
            _ => cause.take(),
        }
    }

    pub(in crate::client::manifold) fn exhaust(&mut self, cause: opening::Exhaustion) {
        if let Self::Vacant(terminal) = self {
            *terminal = Some(error::Error::Exhausted(cause));
        }
    }

    pub(in crate::client::manifold) fn retire(
        &mut self,
        conn: conn::Id<'d, ID>,
        reason: close::Reason,
    ) {
        let phase = mem::replace(self, Self::Vacant(None));
        *self = match phase {
            Self::Active { active, terminal } if active.connection() == conn => Self::Retired {
                active,
                terminal: terminal.or(Some(error::Error::Disconnected(reason))),
            },
            phase => phase,
        };
    }

    pub(in crate::client::manifold) fn disconnect(
        &mut self,
        conn: conn::Id<'d, ID>,
        reason: close::Reason,
    ) {
        let terminal = match self {
            Self::Active { active, terminal } if active.connection() == conn => {
                terminal.take().or(Some(error::Error::Disconnected(reason)))
            }
            Self::Retired { active, terminal } if active.connection() == conn => terminal.take(),
            Self::Vacant(_) | Self::Active { .. } | Self::Retired { .. } => return,
        };
        *self = Self::Vacant(terminal);
    }

    pub(in crate::client::manifold) fn fail(
        &mut self,
        conn: conn::Id<'d, ID>,
        failure: error::Error,
    ) {
        if let Self::Active { active, terminal } = self
            && active.connection() == conn
            && terminal.is_none()
        {
            match failure {
                error::Error::Prepare(_) | error::Error::Payload(_) | error::Error::Reserve(_) => {
                    *self = Self::Vacant(Some(failure));
                }
                failure => *terminal = Some(failure),
            }
        } else if let Self::Retired { active, terminal } = self
            && active.connection() == conn
            && terminal.is_none()
        {
            *terminal = Some(failure);
        }
    }

    pub(in crate::client::manifold) fn signal(&self, conn: conn::Id<'d, ID>) {
        if let Self::Active { active, .. } | Self::Retired { active, .. } = self
            && active.connection() == conn
        {
            active.wake();
        }
    }

    pub(super) fn get_mut(&mut self, conn: conn::Id<'d, ID>) -> Option<&mut Active<'d, ID>> {
        match self {
            Self::Active { active, .. } | Self::Retired { active, .. }
                if active.connection() == conn =>
            {
                Some(active)
            }
            Self::Vacant(_) | Self::Active { .. } | Self::Retired { .. } => None,
        }
    }

    pub(in crate::client::manifold) fn active_mut(
        &mut self,
        conn: conn::Id<'d, ID>,
    ) -> Result<&mut Active<'d, ID>, error::Error> {
        match self {
            Self::Active { active, .. } if active.connection() == conn => Ok(active),
            Self::Active { .. } => Err(error::Error::StaleCapability),
            Self::Retired { terminal, .. } | Self::Vacant(terminal) => {
                Err(Self::take(terminal).unwrap_or(error::Error::NotConnected))
            }
        }
    }
}
