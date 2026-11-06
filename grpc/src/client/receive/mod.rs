//! Client response values and receive-side assembly.
use std::marker;

use o3::{
    base::num::bounded,
    buffer::{self, storage},
    depot::arena::slab::key,
};
use sark_core::manifold::client::event::Shutdown;

use crate::{Reason, message, metadata};

mod assemble;
pub(crate) mod budget;
pub(crate) mod claims;
mod data;
pub(in crate::client) mod policies;
mod turn;

pub(super) use assemble::Assembler;
use dope::core::driver::schedule::ready::wake::table;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::client) struct Scope<'scope>(table::Scope<'scope>);

impl<'scope> Scope<'scope> {
    pub(in crate::client) const fn new(scope: table::Scope<'scope>) -> Self {
        Self(scope)
    }

    const fn slot(self, index: Index) -> Slot<'scope> {
        Slot {
            index,
            scope: marker::PhantomData,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::client) struct Slot<'scope> {
    index: Index,
    scope: marker::PhantomData<Scope<'scope>>,
}

const _: () = {
    assert!(core::mem::size_of::<Scope<'static>>() == 0);
    assert!(core::mem::size_of::<Slot<'static>>() == core::mem::size_of::<usize>());
    assert!(core::mem::align_of::<Slot<'static>>() == core::mem::align_of::<usize>());
};

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(in crate::client) struct Index(usize);

/// The pending index is retained from the same checked claim as the poll.
pub(in crate::client) enum Progress<T> {
    Ready(T),
    Pending(Index),
}

impl<T> Progress<T> {
    pub(in crate::client) fn into_option(self) -> Option<T> {
        match self {
            Self::Ready(value) => Some(value),
            Self::Pending(_) => None,
        }
    }
}

impl Index {
    const fn new(index: usize) -> Self {
        Self(index)
    }

    pub(in crate::client) const fn index(self) -> usize {
        self.0
    }
}

const _: () = {
    assert!(core::mem::size_of::<Index>() == core::mem::size_of::<usize>());
    assert!(core::mem::align_of::<Index>() == core::mem::align_of::<usize>());
};

pub struct UnaryResult<C: buffer::raw::Segment = storage::Shared> {
    pub metadata: metadata::Decoded,
    pub message: Option<message::Frame<C>>,
    pub status: crate::Status,
}

impl<C: buffer::raw::Segment> UnaryResult<C> {
    pub fn into_payload(self) -> Result<message::Payload<C>, crate::Status> {
        self.try_map(|message| Ok(message.payload))
    }

    pub(in crate::client) fn try_map<T>(
        self,
        map: impl FnOnce(message::Frame<C>) -> Result<T, crate::Status>,
    ) -> Result<T, crate::Status> {
        let input = (self, map);
        if input.0.status.code() != crate::Code::Ok {
            return Err(input.0.status);
        }
        let message = input
            .0
            .message
            .ok_or_else(|| crate::Status::new(crate::Code::Internal, Reason::UnaryResponse))?;
        input.1(message)
    }
}

pub trait Unary {
    type Storage: buffer::raw::Segment;
    type Decoded<D: crate::Codec>;

    fn into_decoded<D: crate::Codec>(
        self,
        codec: &mut D,
    ) -> Result<Self::Decoded<D>, crate::Status>;
}

impl<C: buffer::raw::Segment> Unary for UnaryResult<C> {
    type Storage = C;
    type Decoded<D: crate::Codec> = D::Owned<C>;

    fn into_decoded<D: crate::Codec>(self, codec: &mut D) -> Result<D::Owned<C>, crate::Status> {
        self.try_map(|message| codec.decode_frame(message))
    }
}

/// One value delivered by an affine response stream.
pub enum Response<C: buffer::raw::Segment = storage::Shared> {
    Headers { metadata: metadata::Decoded },
    Message { message: message::Frame<C> },
    Trailers { status: crate::Status },
}

pub enum Decoded<T> {
    Headers { metadata: metadata::Decoded },
    Message { message: T },
    Trailers { status: crate::Status },
}

pub trait Event {
    type Storage: buffer::raw::Segment;
    type Decoded<D: crate::Codec>;

    fn into_decoded<D: crate::Codec>(
        self,
        codec: &mut D,
    ) -> Result<Self::Decoded<D>, crate::Status>;
}

impl<C: buffer::raw::Segment> Event for Response<C> {
    type Storage = C;
    type Decoded<D: crate::Codec> = Decoded<D::Owned<C>>;

    fn into_decoded<D: crate::Codec>(
        self,
        codec: &mut D,
    ) -> Result<Decoded<D::Owned<C>>, crate::Status> {
        Ok(match self {
            Self::Headers { metadata } => Decoded::Headers { metadata },
            Self::Message { message } => Decoded::Message {
                message: codec.decode_frame(message)?,
            },
            Self::Trailers { status } => Decoded::Trailers { status },
        })
    }
}

#[derive(Debug)]
pub(crate) enum UnaryTag {}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(super) struct UnaryKey(key::Handle<UnaryTag>);

impl UnaryKey {
    pub(in crate::client) const fn slot(self) -> Index {
        Index::new(self.0.index() as usize)
    }
}

#[derive(Debug)]
pub(crate) enum StreamTag {}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(super) struct StreamKey(key::Handle<StreamTag>);

impl StreamKey {
    pub(in crate::client) const fn slot(self, offset: usize) -> Index {
        Index::new(offset + self.0.index() as usize)
    }
}

pub(in crate::client) enum Cancel {
    Missing,
    Complete(Index),
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::client) enum Phase {
    #[default]
    Open,
    Drain,
    Failed,
}

impl Phase {
    fn observe(&mut self, shutdown: Shutdown) {
        if *self == Self::Failed {
            return;
        }
        *self = match shutdown {
            Shutdown::Clean => Self::Drain,
            Shutdown::Failed => Self::Failed,
        };
    }

    pub(in crate::client) const fn is_open(self) -> bool {
        matches!(self, Self::Open)
    }
}

struct Fault(Option<crate::Status>);

impl Fault {
    const fn new() -> Self {
        Self(None)
    }

    fn release(&mut self, budget: &mut budget::Budget, charge: Option<budget::Charge>) {
        let Some(charge) = charge else {
            return;
        };
        if let Err(error) = budget.release(charge) {
            self.store(crate::Status::from(error));
        }
    }

    fn store(&mut self, error: crate::Status) {
        if self.0.is_none() {
            self.0 = Some(error);
        }
    }

    fn take(&mut self) -> Result<(), crate::Status> {
        match self.0.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    const fn has_error(&self) -> bool {
        self.0.is_some()
    }
}

const _: () = {
    assert!(core::mem::size_of::<UnaryKey>() == core::mem::size_of::<key::Handle<UnaryTag>>());
    assert!(core::mem::align_of::<UnaryKey>() == core::mem::align_of::<key::Handle<UnaryTag>>());
    assert!(core::mem::size_of::<StreamKey>() == core::mem::size_of::<key::Handle<StreamTag>>());
    assert!(core::mem::align_of::<StreamKey>() == core::mem::align_of::<key::Handle<StreamTag>>());
};

// SAFETY: only Notify brands an index, after the receiving claims admitted its
// key. The scope comes from channel::Access, which retains the same State and
// completion table built from that State's immutable protocol limits. Neither
// the slot nor its invariant scope can escape that source operation.
unsafe impl<'scope> table::raw::Index<'scope> for Slot<'scope> {
    fn index(self) -> bounded::U32<0, { u32::MAX - 1 }> {
        // SAFETY: the table constructor admitted the common extent as a compact
        // capacity, and the scoped receive index lies strictly below it.
        unsafe { bounded::raw::u32(self.index.index() as u32) }
    }
}
