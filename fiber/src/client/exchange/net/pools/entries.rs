use core::cell;
use std::{marker::PhantomData, mem, num::NonZero};

use dope::{
    core::driver::route::Identity,
    manifold::client::{conn, port::error::Open},
};
use dope_fiber::reply::layout;
use o3::{
    alloc::{self, array},
    base::{cell::region, time::Positive},
    depot::queue::indexed::lanes::entry,
};

use crate::client::{
    config::report,
    exchange::{
        net::{self, ReadyQueue as _},
        reuse,
    },
};

#[derive(Clone, Copy)]
pub(super) enum State<P, T> {
    Vacant,
    Admitting,
    Admitted(P),
    Connecting(P),
    Ready(P, T),
    Receiving(P, T),
    Closing(P, T),
    Drained(P, T),
}

impl<P: Copy, T: Copy> State<P, T> {
    pub(super) fn current(self) -> Option<(P, T)> {
        match self {
            Self::Ready(position, tag)
            | Self::Receiving(position, tag)
            | Self::Closing(position, tag)
            | Self::Drained(position, tag) => Some((position, tag)),
            Self::Vacant | Self::Admitting | Self::Admitted(_) | Self::Connecting(_) => None,
        }
    }
}

pub(super) struct Entry<'d, Tag: Copy, S, P> {
    pub(super) admission: cell::Cell<reuse::Admission<'d>>,
    pub(super) state: cell::Cell<State<P, Tag>>,
    pub(super) report: S,
}

pub(in crate::client) struct Bound<'a, 'd, ID: Identity, Tag: Copy, S, P> {
    pub(super) entry: &'a Entry<'d, Tag, S, P>,
    pub(super) tag: Tag,
    pub(super) position: P,
    pub(super) route: crate::client::Covariant<ID>,
}

pub(in crate::client) type Positioned<'a, 'b, 'd, ID, Tag, S> =
    Bound<'a, 'd, ID, Tag, S, layout::Position<'b, 'd, ID>>;

/// Holds an admitted reply position until transport activation commits.
pub(in crate::client::exchange::net) struct Admission<'a, 'b, 'd, ID: Identity, Tag: Copy, S> {
    entry: &'a Entry<'d, Tag, S, layout::Position<'b, 'd, ID>>,
    position: layout::Position<'b, 'd, ID>,
}

impl<'b, 'd, ID: Identity, Tag: Copy, S> Admission<'_, 'b, 'd, ID, Tag, S> {
    pub(in crate::client::exchange::net) fn commit(self) -> layout::Position<'b, 'd, ID> {
        let this = mem::ManuallyDrop::new(self);
        this.entry.state.set(State::Admitted(this.position));
        this.position
    }
}

impl<ID: Identity, Tag: Copy, S> Drop for Admission<'_, '_, '_, ID, Tag, S> {
    fn drop(&mut self) {
        self.entry.state.set(State::Vacant);
    }
}

/// Reserves an admitted generation until activation commits.
/// Its report remains vacant: construction initializes it, and reuse requires
/// the transport's completed retirement proof before admission can begin again.
pub(super) struct Connection<'a, 'b, 'd, ID: Identity, Tag: Copy, S> {
    entry: &'a Entry<'d, Tag, S, layout::Position<'b, 'd, ID>>,
    position: layout::Position<'b, 'd, ID>,
}

impl<'a, 'b, 'd, ID: Identity, Tag: Copy, S> Connection<'a, 'b, 'd, ID, Tag, S> {
    pub(super) fn commit(self, tag: Tag) -> Positioned<'a, 'b, 'd, ID, Tag, S> {
        let this = mem::ManuallyDrop::new(self);
        this.entry.state.set(State::Ready(this.position, tag));
        Bound {
            entry: this.entry,
            tag,
            position: this.position,
            route: PhantomData,
        }
    }
}

impl<ID: Identity, Tag: Copy, S> Drop for Connection<'_, '_, '_, ID, Tag, S> {
    fn drop(&mut self) {
        self.entry.state.set(State::Admitted(self.position));
    }
}

impl<'b, 'd, ID: Identity, Tag: Copy, S> Positioned<'_, 'b, 'd, ID, Tag, S> {
    pub(in crate::client::exchange::net) const fn tag(&self) -> Tag {
        self.tag
    }

    pub(in crate::client::exchange::net) const fn connection(&self) -> conn::Id<'d, ID> {
        self.position.connection()
    }

    pub(in crate::client) const fn position(&self) -> layout::Position<'b, 'd, ID> {
        self.position
    }
}

pub(in crate::client) struct Directory<'b, 'd, ID: Identity, Tag, S = ()>
where
    Tag: net::PoolTag<'d, ID>,
{
    pub(super) entries:
        array::Block<Entry<'d, Tag, S, layout::Position<'b, 'd, ID>>, layout::Domain<'b>>,
    pub(in crate::client) ready: Tag::Ready,
    pub(super) live: cell::Cell<u32>,
    pub(super) failure: cell::Cell<Option<entry::Error<NonZero<u32>>>>,
}

impl<'b, 'd, ID: Identity, Tag: net::PoolTag<'d, ID>, S> Directory<'b, 'd, ID, Tag, S> {
    pub(super) fn try_new<'f, E, F>(
        domain: layout::Domain<'b>,
        ready: Tag::Ready,
        token: &region::Token<region::Identity<'d>>,
    ) -> Result<Self, alloc::Error>
    where
        E: 'f,
        F: report::Policy<E, State<'f, 'd> = S>,
    {
        Ok(Self {
            entries: array::Block::try_new(domain, ())?.repeat_with(|| Entry {
                admission: cell::Cell::new(reuse::Admission::Sealed),
                state: cell::Cell::new(State::Vacant),
                report: F::vacant(token),
            }),
            ready,
            live: cell::Cell::new(0),
            failure: cell::Cell::new(None),
        })
    }

    pub(in crate::client::exchange::net) fn try_admit(
        &self,
        id: conn::Id<'d, ID>,
    ) -> Result<Admission<'_, 'b, 'd, ID, Tag, S>, net::Open> {
        let position =
            layout::Position::try_new(*self.entries.extent(), id).map_err(net::Open::Reply)?;
        let entry = position.entry(&self.entries);
        match entry.state.get() {
            State::Vacant => {
                entry.state.set(State::Admitting);
                Ok(Admission { entry, position })
            }
            _ => Err(net::Open::Transport(Open::Occupied)),
        }
    }

    pub(super) fn try_connect(
        &self,
        id: conn::Id<'d, ID>,
    ) -> Result<Connection<'_, 'b, 'd, ID, Tag, S>, net::Open> {
        let entry = self
            .entries
            .get(id.index().into_usize())
            .ok_or(net::Open::Transport(Open::Unknown))?;
        match entry.state.get() {
            State::Admitted(position) if position.connection() == id => {
                entry.state.set(State::Connecting(position));
                Ok(Connection { entry, position })
            }
            State::Vacant | State::Admitted(_) => Err(net::Open::Transport(Open::Unknown)),
            _ => Err(net::Open::Transport(Open::Occupied)),
        }
    }

    pub(super) fn entry(&self, id: conn::Id<'d, ID>) -> Option<Positioned<'_, 'b, 'd, ID, Tag, S>> {
        let bound = self.at(id.index().into_usize())?;
        (bound.connection() == id).then_some(bound)
    }

    pub(super) fn at(&self, index: usize) -> Option<Positioned<'_, 'b, 'd, ID, Tag, S>> {
        let entry = self.entries.get(index)?;
        let (position, tag) = entry.state.get().current()?;
        Some(Bound {
            entry,
            tag,
            position,
            route: PhantomData,
        })
    }

    pub(in crate::client) fn try_current(
        &self,
        id: conn::Id<'d, ID>,
    ) -> Result<Positioned<'_, 'b, 'd, ID, Tag, S>, net::Open> {
        self.entry(id).ok_or(net::Open::Transport(Open::Unknown))
    }

    pub(super) fn schedule(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        bound: &Positioned<'_, 'b, 'd, ID, Tag, S>,
    ) -> Option<Tag> {
        let entry = bound.entry;
        if !matches!(entry.state.get(), State::Ready(..)) || !entry.admission.get().can_queue() {
            return None;
        }
        if self.failure.get().is_none()
            && let Err(cause) = self.ready.push_back(token, bound)
        {
            self.failure.set(Some(cause));
        }
        Some(bound.tag)
    }

    pub(super) fn pop_ready(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        tag: Tag,
    ) -> Result<Option<usize>, entry::Error<NonZero<u32>>> {
        match self.failure.get() {
            Some(cause) => Err(cause),
            None => Ok(self.ready.pop_front(token, tag)),
        }
    }

    pub(in crate::client) fn len(&self) -> usize {
        self.live.get() as usize
    }

    pub(in crate::client) fn idle_window(&self, lane: usize) -> Option<Positive> {
        self.entries
            .get(lane)
            .and_then(|entry| entry.admission.get().idle_window())
    }
}
