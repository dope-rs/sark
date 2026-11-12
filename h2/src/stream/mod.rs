//! HTTP/2 stream identifiers and lifecycle state.

use core::num::NonZero;

use o3::base::num::bounded;
use sark_core::manifold::stream::{Boundary, Section};
use sark_http::payload::response::Method;

pub(crate) use sark_http::field::metadata::block::Content;

mod key;
pub(crate) mod raw;

pub use key::Key;

#[repr(transparent)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(NonZero<u32>);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Nonzero(Id);

impl Id {
    pub const CONNECTION: Self = Self::encode(0);
    pub const FIRST_CLIENT: Self = Self::encode(1);
    pub const FIRST_SERVER: Self = Self::encode(2);
    pub const MAX: u32 = 0x7fff_ffff;

    const fn encode(raw: u32) -> Self {
        Self(NonZero::<u32>::MIN.saturating_add(raw << 1))
    }

    pub const fn new(raw: u32) -> Option<Self> {
        match bounded::U32::<0, 0x7fff_ffff>::new(raw) {
            Some(value) => Some(Self::encode(value.get())),
            None => None,
        }
    }

    pub const fn into_u32(self) -> u32 {
        self.0.get() >> 1
    }

    pub const fn is_zero(self) -> bool {
        self.into_u32() == 0
    }

    pub const fn is_client(self) -> bool {
        let raw = self.into_u32();
        raw != 0 && raw % 2 == 1
    }

    pub const fn is_server(self) -> bool {
        let raw = self.into_u32();
        raw != 0 && raw.is_multiple_of(2)
    }

    pub(crate) const fn nonzero(self) -> Option<Nonzero> {
        if self.is_zero() {
            None
        } else {
            Some(Nonzero(self))
        }
    }

    pub(crate) const fn from_wire(raw: u32) -> Self {
        Self::encode(raw & Self::MAX)
    }

    pub(crate) const fn wire_bytes(self) -> [u8; 4] {
        self.into_u32().to_be_bytes()
    }
}

impl Nonzero {
    pub(crate) const FIRST_CLIENT: Self = Self(Id::FIRST_CLIENT);
    pub(crate) const FIRST_SERVER: Self = Self(Id::FIRST_SERVER);

    pub(crate) const fn get(self) -> Id {
        self.0
    }
}

const _: () = {
    assert!(core::mem::size_of::<Id>() == core::mem::size_of::<u32>());
    assert!(core::mem::size_of::<Nonzero>() == core::mem::size_of::<Id>());
    assert!(core::mem::size_of::<Option<Id>>() == core::mem::size_of::<Id>());
};

#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum State {
    Idle,
    ReservedLocal,
    ReservedRemote,
    Open,
    HalfClosedLocal,
    HalfClosedRemote,
    Closed,
}

const _: () = {
    assert!(core::mem::size_of::<State>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<State>() == core::mem::align_of::<bool>());
};

#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Side {
    Local,
    Remote,
}

#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Event {
    Headers { boundary: Boundary },
    Data { boundary: Boundary },
}

/// The admitted local HEADERS effect, independent of later receive progress.
pub(crate) struct Sending(Boundary);

impl Sending {
    pub(crate) fn new(state: State, boundary: Boundary) -> Result<Self, Error> {
        state.send(Event::Headers { boundary })?;
        Ok(Self(boundary))
    }

    /// The retained response owns the next local send until this commit.
    pub(crate) fn apply(self, current: State) -> State {
        match (self.0, current) {
            (Boundary::More, State::Idle) => State::Open,
            (Boundary::More, State::ReservedLocal) => State::HalfClosedRemote,
            (Boundary::More, current) => current,
            (Boundary::End, State::ReservedLocal | State::HalfClosedRemote | State::Closed) => {
                State::Closed
            }
            (Boundary::End, _) => State::HalfClosedLocal,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
#[derive(Debug)]
pub enum Error {
    Protocol {
        state: State,
        event: Event,
        side: Side,
    },
    StreamClosed {
        state: State,
        event: Event,
        side: Side,
    },
}

const _: () = assert!(
    core::mem::size_of::<Error>()
        == core::mem::size_of::<State>()
            + core::mem::size_of::<Event>()
            + core::mem::size_of::<Side>()
            + core::mem::size_of::<u8>()
);

impl State {
    pub fn send(self, event: Event) -> Result<Self, Error> {
        self.directional(
            event,
            Side::Local,
            Self::ReservedLocal,
            Self::HalfClosedLocal,
            Self::HalfClosedRemote,
        )
    }

    pub fn recv(self, event: Event) -> Result<Self, Error> {
        self.directional(
            event,
            Side::Remote,
            Self::ReservedRemote,
            Self::HalfClosedRemote,
            Self::HalfClosedLocal,
        )
    }

    fn directional(
        self,
        event: Event,
        side: Side,
        reserved: Self,
        ended: Self,
        active: Self,
    ) -> Result<Self, Error> {
        let (more, end) = match (self, event) {
            (Self::Idle, Event::Headers { .. }) | (Self::Open, _) => (Self::Open, ended),
            (state, Event::Headers { .. }) if state == reserved => (active, Self::Closed),
            (state, _) if state == active => (active, Self::Closed),
            (state, _) if state == ended || state == Self::Closed => {
                return Err(Error::StreamClosed {
                    state: self,
                    event,
                    side,
                });
            }
            _ => {
                return Err(Error::Protocol {
                    state: self,
                    event,
                    side,
                });
            }
        };
        let boundary = match event {
            Event::Headers { boundary } | Event::Data { boundary } => boundary,
        };
        Ok(match boundary {
            Boundary::More => more,
            Boundary::End => end,
        })
    }
}

pub struct Lifecycle {
    pub state: State,
    pub peer_section: Section,
    pub(crate) method: Option<Method>,
    pub(crate) content: Content,
}

const _: () = {
    assert!(core::mem::size_of::<Option<Method>>() == core::mem::size_of::<Method>());
    assert!(core::mem::size_of::<Lifecycle>() == 4);
};

impl Lifecycle {
    pub const fn new() -> Self {
        Self {
            state: State::Idle,
            peer_section: Section::Initial,
            method: None,
            content: Content::Unbounded,
        }
    }

    pub(crate) const fn open_local(boundary: Boundary, method: Method) -> Self {
        Self {
            state: match boundary {
                Boundary::End => State::HalfClosedLocal,
                Boundary::More => State::Open,
            },
            peer_section: Section::Initial,
            method: Some(method),
            content: Content::Unbounded,
        }
    }

    pub(crate) const fn open_remote(
        boundary: Boundary,
        method: Option<Method>,
        content: Content,
    ) -> Self {
        Self {
            state: match boundary {
                Boundary::End => State::HalfClosedRemote,
                Boundary::More => State::Open,
            },
            peer_section: Section::Trailing,
            method,
            content,
        }
    }

    pub const fn reserved() -> Self {
        Self {
            state: State::ReservedRemote,
            peer_section: Section::Initial,
            method: None,
            content: Content::Unbounded,
        }
    }
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct IdGen {
    next: Option<Nonzero>,
}

pub(crate) struct Next<'a> {
    generator: &'a mut IdGen,
    id: Nonzero,
}

impl IdGen {
    pub(crate) fn new(first: Nonzero) -> Self {
        Self { next: Some(first) }
    }

    pub(crate) fn claim(&mut self) -> Option<Next<'_>> {
        Some(Next {
            id: self.next?,
            generator: self,
        })
    }

    pub(crate) fn peek(&self) -> Option<Id> {
        self.next.map(Nonzero::get)
    }
}

impl Next<'_> {
    pub(crate) const fn id(&self) -> Nonzero {
        self.id
    }

    pub(crate) fn commit(self) {
        self.generator.next = Id::new(self.id.get().into_u32() + 2).map(Nonzero);
    }
}
