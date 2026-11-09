use crate::manifold::owners;
use sark_core::manifold::task::app;
use std::mem;

use entries::status;
use o3::depot::{
    arena::{hash::runtime, slab::key},
    queue::{indexed::keyed, links},
};
use sark_core::body::{self, stream::Stream};
use sark_http::payload::response::{Method, status::Code};

use crate::manifold::{
    Phase, encoder, requests,
    requests::{
        duplex, entries,
        entries::{Destination, Detached, Member, Occupied, Tag},
        exchange, task,
    },
};

/// Allocation-free state of one resident HTTP/3 request.
pub(in crate::manifold::requests) enum Entry<'d, A: app::Runtime<'d>, M: owners::Handoff> {
    Reserved,
    Pending(entries::Buffered<'d, A::Prepared>),
    Building(Stream<'d, A::Build>),
    Deferred(task::Input<'d, A::Prepared, <A::Build as body::Build<'d>>::Output>),
    Responding(Code),
    Duplex(duplex::State<'d, A::Duplex>),
    Active(task::Active<'d, A::Task, M>),
    Response(encoder::Pending<'d, M>),
    Closing(encoder::Pending<'d, M>),
    Stopped,
}

impl<'d, A: app::Runtime<'d>, M: owners::Handoff> Entry<'d, A, M> {
    pub(in crate::manifold::requests) const fn phase(&self) -> Phase {
        match self {
            Self::Reserved => Phase::Reserved,
            Self::Pending(_) => Phase::Pending,
            Self::Building(_) => Phase::Building,
            Self::Deferred(_) => Phase::Deferred,
            Self::Responding(_) | Self::Response(_) | Self::Closing(_) => Phase::Responding,
            Self::Duplex(_) => Phase::Duplex,
            Self::Active(_) => Phase::Active,
            Self::Stopped => Phase::Stopped,
        }
    }

    pub(in crate::manifold::requests) fn drain(
        self,
    ) -> Option<exchange::Drained<task::Owned<'d, A::Task, M>>> {
        Some(match self {
            Self::Reserved | Self::Deferred(_) | Self::Responding(_) | Self::Duplex(_) => {
                exchange::Drained::Exchange { pending: None }
            }
            Self::Pending(body) => exchange::Drained::Exchange {
                pending: Some(body.into_release()),
            },
            Self::Building(body) => exchange::Drained::Exchange {
                pending: Some(body.into_release()),
            },
            Self::Active(active) => exchange::Drained::Task(task::Owned::Active(active)),
            Self::Response(response) | Self::Closing(response) => {
                exchange::Drained::Task(task::Owned::Response(response))
            }
            Self::Stopped => return None,
        })
    }
}

pub(in crate::manifold::requests::entries) struct Slot<'d, A: app::Runtime<'d>, M: owners::Handoff>
{
    pub(super) identity: requests::Identity,
    pub(super) finished: bool,
    pub(super) method: Method,
    pub(super) queue: status::Queue,
    pub(super) entry: Entry<'d, A, M>,
    pub(super) ready: keyed::Link<key::Handle<runtime::Domain<Tag>>, runtime::Indexed>,
}

impl<'d, A: app::Runtime<'d>, M: owners::Handoff> Slot<'d, A, M> {
    pub(super) fn new(
        identity: requests::Identity,
        index: links::Index,
        entry: Entry<'d, A, M>,
    ) -> Self {
        Self {
            identity,
            finished: false,
            method: Method::Other,
            queue: status::Queue::Idle,
            entry,
            ready: keyed::Link::from(index),
        }
    }
}

pub(in crate::manifold::requests::entries) enum State<
    'a,
    'd,
    A: app::Runtime<'d>,
    M: owners::Handoff,
> {
    Ready(Member<'a, 'd, A, M>),
    Blocked(Member<'a, 'd, A, M>),
    Idle(Detached<'a, 'd, A, M>),
}

impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> State<'a, 'd, A, M> {
    pub(super) fn key(&self) -> key::Handle<runtime::Domain<Tag>> {
        self.owner().entry.key()
    }

    pub(super) fn get(&self) -> &Slot<'d, A, M> {
        self.owner().entry.get()
    }

    pub(super) fn get_mut(&mut self) -> &mut Entry<'d, A, M> {
        match self {
            Self::Ready(member) | Self::Blocked(member) => &mut member.owner.entry.get_mut().entry,
            Self::Idle(vacant) => &mut vacant.owner.entry.get_mut().entry,
        }
    }

    pub(super) fn run<R>(
        mut self,
        change: impl FnOnce(Entry<'d, A, M>) -> Destination<Entry<'d, A, M>, R>,
    ) -> R {
        let entry = self.get_mut();
        let current = mem::replace(entry, Entry::Stopped);
        let (value, next) = match change(current) {
            Destination::Stay(entry, out) => (entry, Destination::Stay((), out)),
            Destination::Ready(entry, out) => (entry, Destination::Ready((), out)),
            Destination::Blocked(entry, out) => (entry, Destination::Blocked((), out)),
            Destination::Rotate(entry, out) => (entry, Destination::Rotate((), out)),
            Destination::Idle(entry, out) => (entry, Destination::Idle((), out)),
            Destination::Remove(out) => (Entry::Stopped, Destination::Remove(out)),
        };
        *entry = value;
        match next {
            Destination::Stay((), out) => out,
            Destination::Ready((), out) => {
                self.ready();
                out
            }
            Destination::Blocked((), out) => {
                self.block();
                out
            }
            Destination::Rotate((), out) => {
                self.ready().rotate();
                out
            }
            Destination::Idle((), out) => {
                self.idle();
                out
            }
            Destination::Remove(out) => {
                self.idle().owner.entry.remove();
                out
            }
        }
    }

    pub(super) fn ready(self) -> Member<'a, 'd, A, M> {
        match self {
            Self::Ready(member) => member,
            Self::Idle(vacant) => vacant.push::<false>(),
            Self::Blocked(member) => member.remove().route().push::<false>(),
        }
    }

    pub(super) fn block(self) -> Self {
        match self {
            Self::Blocked(member) => Self::Blocked(member),
            change => Self::Blocked(change.idle().route().push::<true>()),
        }
    }

    pub(super) fn idle(self) -> Detached<'a, 'd, A, M> {
        match self {
            Self::Ready(member) => member.remove(),
            Self::Blocked(member) => member.remove().route(),
            Self::Idle(vacant) => vacant,
        }
    }

    pub(super) fn owner(&self) -> &Occupied<'a, 'd, A, M> {
        match self {
            Self::Ready(member) | Self::Blocked(member) => &member.owner,
            Self::Idle(vacant) => &vacant.owner,
        }
    }
}
