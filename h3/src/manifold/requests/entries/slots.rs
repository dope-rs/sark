use std::mem;

use entries::status;
use o3::depot::{
    arena::{hash::runtime, slab::key},
    queue::{indexed::keyed, links},
};
use sark_core::{
    body::{self, stream::Stream},
    decode::request,
    duplex::Session,
};
use sark_http::payload::response::Method;

use crate::manifold::{
    Phase, requests,
    requests::{
        duplex, entries,
        entries::{Destination, Detached, Member, Occupied, Tag},
        exchange, task,
    },
};

/// Allocation-free state of one resident HTTP/3 request.
pub(in crate::manifold::requests) enum Entry<
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: Session<'d>,
> {
    Reserved,
    Pending(entries::Buffered<'d, P>),
    Building(Stream<'d, B>),
    Deferred(task::Input<'d, P, B::Output>),
    Responding(status::State),
    Duplex(duplex::State<'d, D>),
    Active(T),
    Stopped,
}

impl<'d, P: request::Prepared, B: body::Build<'d>, T, D: Session<'d>> Entry<'d, P, B, T, D> {
    pub(in crate::manifold::requests) const fn phase(&self) -> Phase {
        match self {
            Self::Reserved => Phase::Reserved,
            Self::Pending(_) => Phase::Pending,
            Self::Building(_) => Phase::Building,
            Self::Deferred(_) => Phase::Deferred,
            Self::Responding(_) => Phase::Responding,
            Self::Duplex(_) => Phase::Duplex,
            Self::Active(_) => Phase::Active,
            Self::Stopped => Phase::Stopped,
        }
    }

    pub(in crate::manifold::requests) fn drain(self) -> Option<exchange::Drained<T>> {
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
            Self::Active(task) => exchange::Drained::Task(task),
            Self::Stopped => return None,
        })
    }
}

pub(in crate::manifold::requests::entries) struct Slot<
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: Session<'d>,
> {
    pub(super) identity: requests::Identity,
    pub(super) finished: bool,
    pub(super) method: Method,
    pub(super) entry: Entry<'d, P, B, T, D>,
    pub(super) ready: keyed::Link<key::Handle<runtime::Domain<Tag>>, runtime::Indexed>,
}

impl<'d, P: request::Prepared, B: body::Build<'d>, T, D: Session<'d>> Slot<'d, P, B, T, D> {
    pub(super) fn new(
        identity: requests::Identity,
        index: links::Index,
        entry: Entry<'d, P, B, T, D>,
    ) -> Self {
        Self {
            identity,
            finished: false,
            method: Method::Other,
            entry,
            ready: keyed::Link::from(index),
        }
    }
}

pub(in crate::manifold::requests::entries) enum State<
    'a,
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: Session<'d>,
> {
    Ready(Member<'a, 'd, P, B, T, D>),
    Blocked(Member<'a, 'd, P, B, T, D>),
    Idle(Detached<'a, 'd, P, B, T, D>),
}

impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: Session<'d>>
    State<'a, 'd, P, B, T, D>
{
    pub(super) fn key(&self) -> key::Handle<runtime::Domain<Tag>> {
        self.owner().entry.key()
    }

    pub(super) fn get(&self) -> &Slot<'d, P, B, T, D> {
        self.owner().entry.get()
    }

    pub(super) fn get_mut(&mut self) -> &mut Entry<'d, P, B, T, D> {
        match self {
            Self::Ready(member) | Self::Blocked(member) => &mut member.owner.entry.get_mut().entry,
            Self::Idle(vacant) => &mut vacant.owner.entry.get_mut().entry,
        }
    }

    pub(super) fn run<R>(
        mut self,
        change: impl FnOnce(Entry<'d, P, B, T, D>) -> Destination<Entry<'d, P, B, T, D>, R>,
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

    pub(super) fn ready(self) -> Member<'a, 'd, P, B, T, D> {
        match self {
            Self::Ready(member) => member,
            Self::Idle(vacant) => vacant.push(),
            Self::Blocked(member) => member.remove().route().push(),
        }
    }

    pub(super) fn block(self) -> Self {
        match self {
            Self::Blocked(member) => Self::Blocked(member),
            change => Self::Blocked(change.idle().route().push()),
        }
    }

    pub(super) fn idle(self) -> Detached<'a, 'd, P, B, T, D> {
        match self {
            Self::Ready(member) => member.remove(),
            Self::Blocked(member) => member.remove().route(),
            Self::Idle(vacant) => vacant,
        }
    }

    pub(super) fn owner(&self) -> &Occupied<'a, 'd, P, B, T, D> {
        match self {
            Self::Ready(member) | Self::Blocked(member) => &member.owner,
            Self::Idle(vacant) => &vacant.owner,
        }
    }
}
