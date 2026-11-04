use std::{convert::Infallible, mem, num::NonZero, ops};

use dope::{
    core::Errno,
    manifold::{
        app::receive,
        client::{close, conn, deadline},
    },
    net::link::{egress::reserve::exact, event},
};
use o3::{
    depot::{
        arena::pool::{Shared, shared},
        queue::indexed::lanes::entry,
    },
    mem::rc,
};

/// A connection cause whose owner can be retained without copying its payload.
pub trait Detail: Sized {
    fn try_clone(&self) -> Result<Self, rc::Error>;
}

impl Detail for Infallible {
    fn try_clone(&self) -> Result<Self, rc::Error> {
        match *self {}
    }
}

/// Stable category of an established-connection failure.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Kind {
    RemoteEof,
    Protocol,
    Receive,
    Transport,
    Wire,
    Timeout(deadline::Kind),
    EndpointRetired,
    Local(close::LocalOrigin),
}

/// Owner-bound reference to one exact wire cause.
///
/// The cause cannot outlive the domain-local store that admitted it.
#[repr(transparent)]
pub struct Cause<'owner, E = Infallible>(
    Shared<'owner, shared::Resident<E>, shared::Header<E>, shared::Initialized>,
);

impl<'owner, E> Cause<'owner, E> {
    #[doc(hidden)]
    pub const fn new(
        lease: Shared<'owner, shared::Resident<E>, shared::Header<E>, shared::Initialized>,
    ) -> Self {
        Self(lease)
    }
}

impl<E> Detail for Cause<'_, E> {
    fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(Self(self.0.try_clone()?))
    }
}

impl<E> ops::Deref for Cause<'_, E> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An established-connection failure with no invalid field combinations.
#[derive(Debug)]
pub enum Error<D = Infallible> {
    Classified(Kind),
    Park(event::ParkError),
    Ready(entry::Error<NonZero<u32>>),
    Retain(rc::Error),
    Ingress(receive::Failure),
    Egress(exact::Failure),
    Receive {
        errno: Errno,
    },
    Wire {
        detail: D,
        receive: Option<Errno>,
    },
    Transmit {
        error: conn::Transmit,
        receive: Option<Errno>,
        wire: Option<D>,
    },
}

impl<D> Error<D> {
    #[doc(hidden)]
    pub const fn wire(detail: D) -> Self {
        Self::Wire {
            detail,
            receive: None,
        }
    }

    #[doc(hidden)]
    pub const fn wire_receive(detail: D, errno: Errno) -> Self {
        Self::Wire {
            detail,
            receive: Some(errno),
        }
    }

    #[doc(hidden)]
    pub const fn receive(errno: Errno) -> Self {
        Self::Receive { errno }
    }

    #[doc(hidden)]
    pub const fn transmit(error: conn::Transmit) -> Self {
        Self::Transmit {
            error,
            receive: None,
            wire: None,
        }
    }

    /// Adds a receive failure without replacing an earlier failure.
    #[doc(hidden)]
    pub fn with_receive(mut self, errno: Errno) -> Self {
        if let Self::Wire { receive, .. } | Self::Transmit { receive, .. } = &mut self {
            receive.get_or_insert(errno);
        }
        self
    }

    /// Adds a send failure while retaining an earlier wire or receive cause.
    #[doc(hidden)]
    pub fn with_transmit(self, error: conn::Transmit) -> Self {
        match self {
            Self::Wire { detail, receive } => Self::Transmit {
                error,
                receive,
                wire: Some(detail),
            },
            Self::Receive { errno } => Self::Transmit {
                error,
                receive: Some(errno),
                wire: None,
            },
            _ => self,
        }
    }

    pub const fn kind(&self) -> Kind {
        match self {
            Self::Classified(kind) => *kind,
            Self::Park(_) => Kind::Receive,
            Self::Ready(_) | Self::Retain(_) | Self::Ingress(_) | Self::Egress(_) => {
                Kind::Local(close::LocalOrigin::ResourceFailure)
            }
            Self::Receive { .. } | Self::Transmit { wire: None, .. } => Kind::Transport,
            Self::Wire { .. } | Self::Transmit { wire: Some(_), .. } => Kind::Wire,
        }
    }

    pub const fn detail(&self) -> Option<&D> {
        match self {
            Self::Wire { detail, .. } => Some(detail),
            Self::Transmit { wire, .. } => wire.as_ref(),
            Self::Classified(_)
            | Self::Park(_)
            | Self::Ready(_)
            | Self::Retain(_)
            | Self::Ingress(_)
            | Self::Egress(_)
            | Self::Receive { .. } => None,
        }
    }
}

impl Error<Infallible> {
    #[doc(hidden)]
    pub fn generalize<D>(self) -> Error<D> {
        match self {
            Self::Classified(kind) => Error::Classified(kind),
            Self::Park(error) => Error::Park(error),
            Self::Ready(error) => Error::Ready(error),
            Self::Retain(error) => Error::Retain(error),
            Self::Ingress(error) => Error::Ingress(error),
            Self::Egress(error) => Error::Egress(error),
            Self::Receive { errno } => Error::Receive { errno },
            Self::Wire { detail, .. } => match detail {},
            Self::Transmit {
                error,
                receive,
                wire,
            } => Error::Transmit {
                error,
                receive,
                wire: wire.map(|detail| match detail {}),
            },
        }
    }
}

impl<D: Detail> Error<D> {
    pub fn try_clone(&self) -> Result<Self, rc::Error> {
        Ok(match self {
            Self::Classified(kind) => Self::Classified(*kind),
            Self::Park(error) => Self::Park(*error),
            Self::Ready(error) => Self::Ready(*error),
            Self::Retain(error) => Self::Retain(*error),
            Self::Ingress(error) => Self::Ingress(*error),
            Self::Egress(error) => Self::Egress(*error),
            Self::Receive { errno } => Self::Receive { errno: *errno },
            Self::Wire { detail, receive } => Self::Wire {
                detail: detail.try_clone()?,
                receive: *receive,
            },
            Self::Transmit {
                error,
                receive,
                wire,
            } => Self::Transmit {
                error: *error,
                receive: *receive,
                wire: wire.as_ref().map(Detail::try_clone).transpose()?,
            },
        })
    }
}

const _: () = {
    assert!(mem::size_of::<Cause<'static, ()>>() == mem::size_of::<usize>());
    assert!(
        mem::size_of::<Error<usize>>()
            <= mem::size_of::<(conn::Transmit, Option<Errno>, Option<usize>, usize)>()
    );
};
