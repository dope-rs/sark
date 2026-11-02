//! Fiber-free manifold integration for HTTP/3 over dope-quic.
use std::{marker, mem, num};

use conn::{
    scope, stream,
    stream::resident::{Foreign, Handle, Key, Local, Origin, Owner, Stale},
};
use dope_quic::{
    conn,
    conn::stream::{event::Item, recv::credit},
    errors,
    stream::Recv,
    wire::stream::Id,
};
use o3::buffer::resident;
use sark_http::field::metadata::section::{self, rules, state::Selected};

use crate::{conn::event, frame};

mod credits;
pub mod encoder;
mod error;
pub mod flow;
pub mod owners;
mod requests;
pub mod server;
mod session;

pub use error::Error;
pub use session::Session;

#[derive(Debug)]
pub enum Credit {
    Capacity {
        stream_id: Id,
        credit: credit::Detached<credit::Local>,
    },
    Mismatch {
        stream_id: Id,
        held: usize,
        credit: credit::Detached<credit::Local>,
    },
    Missing {
        stream_id: Id,
        requested: num::NonZero<usize>,
    },
    Insufficient {
        stream_id: Id,
        requested: num::NonZero<usize>,
        held: usize,
    },
    Absent {
        stream_id: Id,
        retained: usize,
        queued: usize,
    },
    Reconcile {
        stream_id: Id,
        retained: usize,
        queued: usize,
        credit: credit::Detached<credit::Local>,
    },
}

fn close_connection_scoped<'d, const ID: u8, B: Recv, const RECONCILE: bool>(
    quic: &mut scope::Access<'_, ID, B, RECONCILE, resident::Shared<'d>>,
    code: frame::ErrorCode,
) {
    quic.lifecycle()
        .close(errors::Code::from_u16(code.wire_code()), Vec::new());
}

fn resident_token(key: Option<Key>) -> Result<Option<event::Token>, crate::conn::Error> {
    key.map(|key| {
        let raw = key.raw().get();
        event::Token::new(raw).ok_or(crate::conn::Error::Identity { raw })
    })
    .transpose()
}

fn resident_key(token: event::Token) -> Result<Key, crate::conn::Error> {
    let raw = token.raw();
    num::NonZero::<u64>::new(raw)
        .and_then(Key::from_raw)
        .ok_or(crate::conn::Error::Identity { raw })
}

fn resident_target(handle: Handle<'_>) -> crate::stream::Target {
    let local = handle.local();
    crate::stream::Target::resident(local.id(), local.key())
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Writable,
    Readable {
        stream_id: Id,
    },
    Reset {
        stream_id: Id,
        error_code: errors::Code,
    },
    Stopped {
        stream_id: Id,
        error_code: errors::Code,
    },
}

impl Event {
    const fn stream_id(self) -> Option<Id> {
        match self {
            Self::Writable => None,
            Self::Readable { stream_id: id }
            | Self::Reset { stream_id: id, .. }
            | Self::Stopped { stream_id: id, .. } => Some(id),
        }
    }
}

#[derive(Clone, Copy)]
pub struct Notification<'conn> {
    event: Event,
    key: Option<Key>,
    invariant: marker::PhantomData<fn(&'conn mut ()) -> &'conn mut ()>,
}

pub(in crate::manifold) struct BoundNote<'conn> {
    event: Event,
    owner: Owner<'conn>,
    handle: Option<Handle<'conn>>,
}

pub(in crate::manifold) struct Delivery<'conn, 'd, P: section::Policy> {
    event: event::Event<'d, Selected<P>, P::Block>,
    resident: Resident<'conn>,
}

enum Resident<'conn> {
    None,
    Active { handle: Handle<'conn>, slot: u32 },
    Unbound(Handle<'conn>),
    Ending(Handle<'conn>),
}

impl<'conn> Notification<'conn> {
    pub fn resident(event: stream::BoundEvent<'conn>) -> Self {
        Self {
            event: event.event().into(),
            key: event.key(),
            invariant: marker::PhantomData,
        }
    }

    pub const fn event(self) -> Event {
        self.event
    }

    pub(in crate::manifold) fn bind<
        'd,
        P: rules::Policy,
        const ID: u8,
        B: Recv,
        const RECONCILE: bool,
    >(
        self,
        session: &mut Session<'d, P>,
        quic: &mut scope::Access<'conn, ID, B, RECONCILE, resident::Shared<'d>>,
    ) -> Result<BoundNote<'conn>, Error> {
        let owner = session.bind_scope(quic)?.owner();
        let handle = match (self.key, self.event) {
            (Some(key), event) => event
                .stream_id()
                .map(|id| quic.streams().resident().bind(owner, Local::new(id, key)))
                .transpose()?,
            (None, event) => {
                let handle = event
                    .stream_id()
                    .and_then(|id| quic.streams().resident().handle(id));
                if handle.is_none() && matches!(event, Event::Readable { .. }) {
                    return Err(stream::Error::InvalidStream.into());
                }
                handle
            }
        };
        Ok(BoundNote {
            event: self.event,
            owner,
            handle,
        })
    }
}

impl<'conn> From<Item<'conn>> for Notification<'conn> {
    fn from(event: Item<'conn>) -> Self {
        let event = match event {
            Item::Writable => Event::Writable,
            Item::Readable { id } => Event::Readable { stream_id: id.id() },
            Item::Reset {
                id,
                error_code: code,
            } => Event::Reset {
                stream_id: id.id(),
                error_code: code,
            },
            Item::Stopped {
                id,
                error_code: code,
            } => Event::Stopped {
                stream_id: id.id(),
                error_code: code,
            },
        };
        Self {
            event,
            key: None,
            invariant: marker::PhantomData,
        }
    }
}

impl From<stream::Event> for Event {
    fn from(event: stream::Event) -> Self {
        match event {
            stream::Event::Writable => Self::Writable,
            stream::Event::Readable { id } => Self::Readable { stream_id: id },
            stream::Event::Reset {
                id,
                error_code: code,
            } => Self::Reset {
                stream_id: id,
                error_code: code,
            },
            stream::Event::Stopped {
                id,
                error_code: code,
            } => Self::Stopped {
                stream_id: id,
                error_code: code,
            },
        }
    }
}

#[must_use = "a scoped binding must be consumed for its origin or owner"]
struct Binding<'scope> {
    origin: Origin,
    owner: Owner<'scope>,
}

impl<'scope> Binding<'scope> {
    const fn new(origin: Origin, owner: Owner<'scope>) -> Self {
        Binding { origin, owner }
    }

    const fn origin(self) -> Origin {
        self.origin
    }

    const fn owner(self) -> Owner<'scope> {
        self.owner
    }
}

const _: () = assert!(mem::size_of::<Notification<'static>>() == 4 * mem::size_of::<u64>());
const _: () = assert!(
    mem::size_of::<Delivery<'static, 'static, rules::RetainRaw>>()
        == mem::size_of::<
            event::Event<
                'static,
                Selected<rules::RetainRaw>,
                <rules::RetainRaw as section::Policy>::Block,
            >,
        >() + mem::size_of::<Resident<'static>>()
);
const _: () = assert!(mem::size_of::<Binding<'static>>() == mem::size_of::<Origin>());
const _: () = assert!(
    mem::size_of::<Session<'static>>()
        == mem::size_of::<crate::conn::Connection<'static>>() + mem::size_of::<credits::Ledger>()
);

/// A borrowed local observation; the original error keeps every typed owner.
///
/// Request failures are observed as their own [`requests::Error`], so this type
/// only names the resident binding causes that have no request-level error.
#[derive(Debug)]
pub enum Diagnostic<'a> {
    ResolveNone,
    Unbound {
        stream: u64,
        resident: num::NonZero<u64>,
    },
    Stale {
        stream: u64,
        resident: num::NonZero<u64>,
        slot: u32,
    },
    ResetForeign(&'a Foreign),
    ResetStale(&'a Stale),
    ResetStream(&'a stream::Error),
}

/// Resident phase of one HTTP/3 request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    Missing,
    Reserved,
    Pending,
    Building,
    Deferred,
    Responding,
    Duplex,
    Active,
    Stopped,
}
