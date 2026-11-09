//! Request records deduplicate QUIC streams before carrier admission and pull work beside ingress.
//! This pre-carrier domain cannot use the carrier-driven [`sark_core::manifold::schedule`] runtime.
//! o3 keys, owner proofs and keyed FIFOs preserve one queue membership per record.

use crate::manifold::owners;
use sark_core::manifold::task::app;

use std::mem;

use o3::{
    alloc, depot,
    depot::{
        arena::{
            hash,
            hash::{errors, runtime, runtime::entry},
            slab::key,
        },
        index::Initialized,
        queue::{indexed::keyed, links},
    },
};
use sark_core::body;

use crate::manifold::requests;

mod buffered;
mod live;
mod output;
mod reserved;
mod slots;
pub(in crate::manifold) mod status;

pub(in crate::manifold) use buffered::{Buffered, Finished, Receiving};
pub(in crate::manifold) use live::Failure;
pub(in crate::manifold::requests) use live::Push;
pub(in crate::manifold) use reserved::Reserved;
pub(in crate::manifold::requests) use slots::Entry;

use crate::manifold::Phase;

pub(in crate::manifold::requests) type Built<E> = (
    body::BuildOutcome<requests::Id, std::convert::Infallible, Failure<E>>,
    Option<requests::Id>,
);

#[derive(Debug)]
pub enum Tag {}

pub(super) struct Entries<'d, A: app::Runtime<'d>, M: owners::Handoff> {
    slots: runtime::Map<Slot<'d, A, M>, Tag>,
    ready: keyed::Fifo<key::Handle<runtime::Domain<Tag>>, runtime::Indexed>,
    blocked: keyed::Fifo<key::Handle<runtime::Domain<Tag>>, runtime::Indexed>,
}

impl<'d, A: app::Runtime<'d>, M: owners::Handoff> Entries<'d, A, M> {
    pub(super) fn try_new(plan: hash::Plan) -> Result<Self, alloc::Error> {
        Ok(Self {
            slots: runtime::Map::try_new(plan)?,
            ready: keyed::Fifo::new(),
            blocked: keyed::Fifo::new(),
        })
    }

    pub(super) fn insert(
        &mut self,
        owner: requests::Identity,
        entry: Entry<'d, A, M>,
    ) -> Result<requests::Id, errors::Create<Tag>> {
        let stream = owner.stream_id();
        let key = self.slots.try_create(
            stream.number(),
            |slot| slot.identity.stream_id() == stream,
            |index| Slot::new(owner, index, entry),
        )?;
        Ok(requests::Id::from_entry(owner, key))
    }

    pub(super) fn reserve(
        &mut self,
        owner: requests::Identity,
    ) -> Result<Reserved<'_, 'd, A, M>, errors::Create<Tag>> {
        let stream = owner.stream_id();
        let entry = self.slots.try_live(
            stream.number(),
            |slot| slot.identity.stream_id() == stream,
            |index| Slot::new(owner, index, Entry::Reserved),
        )?;
        Ok(Occupied {
            entry,
            queue: &mut self.ready,
            other: &mut self.blocked,
        }
        .reserved())
    }

    pub(super) fn resolve(
        &mut self,
        owner: requests::Identity,
        index: u32,
        finished: bool,
    ) -> Option<requests::Id> {
        let mut entry = entry::Live::at(&mut self.slots, index as usize)?;
        if entry.get().identity != owner {
            return None;
        }
        if finished {
            entry.get_mut().finished = true;
        }
        Some(requests::Id::from_entry(owner, entry.key()))
    }

    pub(super) fn find(&self, id: requests::Id) -> Option<&Entry<'d, A, M>> {
        let key = id.entry();
        let slot = self.slots.get(key)?;
        (slot.identity == id.identity()).then_some(&slot.entry)
    }

    pub(super) fn phase(&self, owner: requests::Identity, index: u32) -> Phase {
        self.slots
            .key_value_at(index as usize)
            .filter(|(_, slot)| slot.identity == owner)
            .map_or(Phase::Missing, |(_, slot)| slot.entry.phase())
    }

    pub(super) fn reserved(&self) -> impl Iterator<Item = requests::Identity> + '_ {
        self.slots
            .values()
            .filter_map(|slot| matches!(&slot.entry, Entry::Reserved).then_some(slot.identity))
    }

    pub(super) fn try_id(&self, key: key::Handle<runtime::Domain<Tag>>) -> Option<requests::Id> {
        let slot = self.slots.get(key)?;
        Some(requests::Id::from_entry(slot.identity, key))
    }

    pub(super) fn entry(
        &mut self,
        id: requests::Id,
    ) -> Result<Occupied<'_, 'd, A, M>, requests::Error> {
        use runtime::entry;

        let entry =
            entry::Live::try_new(&mut self.slots, id.entry()).map_err(requests::Error::Lookup)?;
        if entry.get().identity != id.identity() {
            return Err(requests::Error::Identity);
        }
        Ok(Occupied {
            entry,
            queue: &mut self.ready,
            other: &mut self.blocked,
        })
    }

    pub(super) fn front(&mut self) -> Option<live::Live<'_, 'd, A, M>> {
        let cursor = keyed::Slot::front(Front::<A, M, false> { entries: self })?;
        Some(live::Live {
            entry: State::Ready(cursor.release()),
        })
    }

    pub(super) fn resume(&mut self) {
        if let Some(cursor) = keyed::Slot::front(Front::<A, M, true> { entries: self }) {
            State::Blocked(cursor.release()).ready();
        }
    }

    pub(super) fn is_empty<const WAIT: bool>(&self) -> bool {
        if WAIT {
            self.blocked.is_empty()
        } else {
            self.ready.is_empty()
        }
    }

    pub(super) fn identify(
        &mut self,
        id: requests::Id,
    ) -> Result<live::Live<'_, 'd, A, M>, requests::Error> {
        Ok(live::Live {
            entry: self.entry(id)?.classify(),
        })
    }

    /// Removes every resident entry through its canonical queue membership,
    /// reporting the roots that survived the exhaustive sweep.
    pub(super) fn drain(
        mut self,
        mut drain: impl FnMut(requests::Id, Entry<'d, A, M>),
    ) -> Residual {
        use runtime::entry;

        for index in 0..self.slots.capacity() {
            let Some(slot) = entry::Live::at(&mut self.slots, index) else {
                continue;
            };
            let key = slot.key();
            let occupied = Occupied {
                entry: slot,
                queue: &mut self.ready,
                other: &mut self.blocked,
            };
            let slot = occupied.classify().idle().owner.entry.remove();
            let id = requests::Id::from_entry(slot.identity, key);
            drain(id, slot.entry);
        }
        Residual {
            entries: self.slots.len(),
            ready: usize::from(!self.ready.is_empty() || !self.blocked.is_empty()),
        }
    }
}

/// Exact roots that survived one exhaustive drain of the resident table.
pub(super) struct Residual {
    pub(super) entries: usize,
    pub(super) ready: usize,
}

enum Destination<E, R> {
    Stay(E, R),
    Ready(E, R),
    Blocked(E, R),
    Rotate(E, R),
    Idle(E, R),
    Remove(R),
}

impl<'d, A: app::Runtime<'d>, M: owners::Handoff, R> Destination<Entry<'d, A, M>, R> {
    fn stop(finished: bool, out: R) -> Self {
        if finished {
            Self::Remove(out)
        } else {
            Self::Idle(Entry::Stopped, out)
        }
    }
}

pub(super) struct Occupied<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> {
    entry: entry::Live<'a, Slot<'d, A, M>, Tag, entry::Occupied>,
    queue: &'a mut keyed::Fifo<key::Handle<runtime::Domain<Tag>>, runtime::Indexed>,
    other: &'a mut keyed::Fifo<key::Handle<runtime::Domain<Tag>>, runtime::Indexed>,
}

impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> Occupied<'a, 'd, A, M> {
    pub(in crate::manifold::requests) fn reserve(self) -> Result<Reserved<'a, 'd, A, M>, Phase> {
        match &self.entry.get().entry {
            Entry::Reserved => Ok(self.reserved()),
            entry => Err(entry.phase()),
        }
    }

    fn reserved(self) -> Reserved<'a, 'd, A, M> {
        Reserved {
            // SAFETY: both callers retain the exclusive map loan after creating or
            // observing `Entry::Reserved`, which is unlinked in both resident FIFOs.
            entry: unsafe { keyed::Slot::<_, _, false>::from_source(depot::raw::Proof::new(self)) }
                .release(),
        }
    }

    fn classify(mut self) -> State<'a, 'd, A, M> {
        match self.entry.get().queue {
            status::Queue::Ready => State::Ready(
                // SAFETY: the queue marker places this slot in the ready FIFO carried by `queue`.
                unsafe { keyed::Slot::<_, _, true>::from_source(depot::raw::Proof::new(self)) }
                    .release(),
            ),
            status::Queue::Blocked => {
                mem::swap(&mut self.queue, &mut self.other);
                State::Blocked(
                    // SAFETY: the swap names the blocked FIFO that stored this membership.
                    unsafe { keyed::Slot::<_, _, true>::from_source(depot::raw::Proof::new(self)) }
                        .release(),
                )
            }
            status::Queue::Idle => State::Idle(
                // SAFETY: `Queue::Idle` is unlinked in both resident FIFOs.
                unsafe { keyed::Slot::<_, _, false>::from_source(depot::raw::Proof::new(self)) }
                    .release(),
            ),
        }
    }
}

pub(super) struct Member<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> {
    owner: Occupied<'a, 'd, A, M>,
}

impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> Member<'a, 'd, A, M> {
    fn remove(mut self) -> Detached<'a, 'd, A, M> {
        self.owner.entry.get_mut().queue = status::Queue::Idle;
        // SAFETY: a member retains its own membership in the FIFO its owner names.
        unsafe { keyed::Slot::from_source(depot::raw::Proof::new(self.owner)) }
            .remove()
            .release()
    }

    fn rotate(self) {
        // SAFETY: a member retains its own membership in the FIFO its owner names.
        unsafe { keyed::Slot::from_source(depot::raw::Proof::new(self.owner)) }
            .move_to_back()
            .release();
    }
}

pub(super) struct Detached<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> {
    owner: Occupied<'a, 'd, A, M>,
}

impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> Detached<'a, 'd, A, M> {
    fn push<const WAIT: bool>(mut self) -> Member<'a, 'd, A, M> {
        self.owner.entry.get_mut().queue = if WAIT {
            status::Queue::Blocked
        } else {
            status::Queue::Ready
        };
        // SAFETY: a detached owner left its FIFO, so this slot is vacant in the target plane.
        unsafe { keyed::Slot::<_, _, false>::from_source(depot::raw::Proof::new(self.owner)) }
            .push::<false>()
            .release()
    }

    fn route(mut self) -> Self {
        mem::swap(&mut self.owner.queue, &mut self.owner.other);
        self
    }
}

pub(super) struct Front<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff, const WAIT: bool> {
    entries: &'a mut Entries<'d, A, M>,
}

// SAFETY: `Occupied` holds one slot's exclusive map loan plus both resident FIFOs; `parts`
// yields that slot's exact key and link, `fifo` the queue it is a member of, and every
// transition re-establishes the pair before the borrowed root authority escapes.
unsafe impl<'d, A: app::Runtime<'d>, M: owners::Handoff> depot::raw::KeyOwner<runtime::Indexed>
    for Occupied<'_, 'd, A, M>
{
    type Key = key::Handle<runtime::Domain<Tag>>;
    type Fifo = keyed::Fifo<Self::Key, runtime::Indexed>;

    fn parts(&self) -> (Self::Key, &keyed::Link<Self::Key, runtime::Indexed>) {
        (self.entry.key(), &self.entry.get().ready)
    }

    fn fifo(&self) -> &keyed::Fifo<Self::Key, runtime::Indexed> {
        self.queue
    }

    fn link(
        live: depot::raw::Proof<(&Self, links::Index), Initialized<runtime::Indexed>>,
    ) -> &keyed::Link<Self::Key, runtime::Indexed> {
        // SAFETY: the native guard borrows the unchanged store retained by the live source.
        let live = unsafe { live.map(|(owner, index)| (&owner.entry, index)) };
        &entry::Live::project(live).1.ready
    }
}

// SAFETY: `Member` is only released for a slot whose queue marker proves membership in the
// FIFO its owner names, and it consumes that owner and root authority together.
unsafe impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff>
    depot::raw::KeyFinish<true, runtime::Indexed> for Occupied<'a, 'd, A, M>
{
    type Output = Member<'a, 'd, A, M>;

    unsafe fn finish(self) -> Self::Output {
        Member { owner: self }
    }
}

// SAFETY: `Detached` is only released for a slot that left its FIFO or was proven vacant by
// `keyed::Slot::try_new`, and it consumes the same owner and root authority.
unsafe impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff>
    depot::raw::KeyFinish<false, runtime::Indexed> for Occupied<'a, 'd, A, M>
{
    type Output = Detached<'a, 'd, A, M>;

    unsafe fn finish(self) -> Self::Output {
        Detached { owner: self }
    }
}

// SAFETY: `Front` borrows both resident FIFOs for `'a` and returns exactly the one named by
// `WAIT`, so a retained source keeps that FIFO's storage, endpoints and link plane.
unsafe impl<'d, A: app::Runtime<'d>, M: owners::Handoff, const WAIT: bool>
    depot::raw::KeySource<runtime::Indexed> for Front<'_, 'd, A, M, WAIT>
{
    type Key = key::Handle<runtime::Domain<Tag>>;

    fn fifo(&self) -> &keyed::Fifo<Self::Key, runtime::Indexed> {
        if WAIT {
            &self.entries.blocked
        } else {
            &self.entries.ready
        }
    }
}

// SAFETY: selection receives an initialized link of the FIFO `KeySource::fifo` returned and
// rebuilds the owning `Occupied` from that exact slot, so every edge it walks is live in it.
unsafe impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff, const WAIT: bool>
    depot::raw::KeyPlane<runtime::Indexed> for Front<'a, 'd, A, M, WAIT>
{
    type Slot = Occupied<'a, 'd, A, M>;

    fn select(
        live: depot::raw::Proof<(Self, links::Index), Initialized<runtime::Indexed>>,
    ) -> Self::Slot {
        // SAFETY: the initialized live entry of the selected FIFO proves that the slot is
        // initialized in the map and that the paired queue is the one currently holding it;
        // splitting the two resident FIFOs preserves that exact key, link and storage.
        let live = unsafe {
            live.map(|(front, index)| {
                let Entries {
                    slots,
                    ready,
                    blocked,
                } = front.entries;
                let queues = if WAIT {
                    (blocked, ready)
                } else {
                    (ready, blocked)
                };
                ((slots, queues), index)
            })
        };
        let (entry, (queue, other)) = entry::Live::from_live(live);
        Occupied {
            entry,
            queue,
            other,
        }
    }
}

use slots::{Slot, State};
