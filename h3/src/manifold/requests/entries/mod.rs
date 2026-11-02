//! Request records deduplicate QUIC streams before carrier admission and pull work beside ingress.
//! This pre-carrier domain cannot use the carrier-driven [`sark_core::manifold::schedule`] runtime.
//! o3 keys, owner proofs and keyed FIFOs preserve one queue membership per record.

use std::mem;

use borrowed::Borrowed;
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
use sark_core::{body, decode::request, duplex};

use crate::manifold::requests;

mod borrowed;
mod buffered;
mod live;
mod output;
mod reserved;
mod slots;
pub(in crate::manifold) mod status;

pub(in crate::manifold) use buffered::{Buffered, Finished, Receiving};
pub(in crate::manifold::requests) use live::Push;
pub(in crate::manifold) use live::{Completion, Failure};
pub(super) use output::{Blocked, Output};
pub(in crate::manifold) use reserved::Reserved;
pub(in crate::manifold::requests) use slots::Entry;

use crate::manifold::Phase;

pub(in crate::manifold::requests) type Built<E> = (
    body::BuildOutcome<requests::Id, std::convert::Infallible, Failure<E>>,
    Option<requests::Id>,
);

#[derive(Debug)]
pub enum Tag {}

pub(super) struct Entries<'d, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>> {
    slots: runtime::Map<Slot<'d, P, B, T, D>, Tag>,
    ready: keyed::Fifo<key::Handle<runtime::Domain<Tag>>, runtime::Indexed>,
    blocked: keyed::Fifo<key::Handle<runtime::Domain<Tag>>, runtime::Indexed>,
}

impl<'d, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>>
    Entries<'d, P, B, T, D>
{
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
        entry: Entry<'d, P, B, T, D>,
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
    ) -> Result<Reserved<'_, 'd, P, B, T, D>, errors::Create<Tag>> {
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

    pub(super) fn find(&self, id: requests::Id) -> Option<&Entry<'d, P, B, T, D>> {
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
    ) -> Result<Occupied<'_, 'd, P, B, T, D>, requests::Error> {
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

    pub(super) fn front(&mut self) -> Option<live::Live<'_, 'd, P, B, T, D>> {
        let cursor = keyed::Slot::front(Front::<P, B, T, D, false> { entries: self })?;
        Some(live::Live {
            entry: State::Ready(cursor.release()),
        })
    }

    pub(super) fn blocked(&mut self) -> Option<Blocked<'_, 'd, P, B, T, D>> {
        let cursor = keyed::Slot::front(Front::<P, B, T, D, true> { entries: self })?;
        Some(Blocked {
            member: cursor.release(),
        })
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
    ) -> Result<live::Live<'_, 'd, P, B, T, D>, requests::Error> {
        Ok(live::Live {
            entry: self.entry(id)?.classify(),
        })
    }

    /// Removes every resident entry through its canonical queue membership,
    /// reporting the roots that survived the exhaustive sweep.
    pub(super) fn drain(
        mut self,
        mut drain: impl FnMut(requests::Id, Entry<'d, P, B, T, D>),
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

impl<'d, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>, R>
    Destination<Entry<'d, P, B, T, D>, R>
{
    fn stop(finished: bool, out: R) -> Self {
        if finished {
            Self::Remove(out)
        } else {
            Self::Idle(Entry::Stopped, out)
        }
    }
}

pub(super) struct Occupied<
    'a,
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: duplex::Session<'d>,
> {
    entry: entry::Live<'a, Slot<'d, P, B, T, D>, Tag, entry::Occupied>,
    queue: &'a mut keyed::Fifo<key::Handle<runtime::Domain<Tag>>, runtime::Indexed>,
    other: &'a mut keyed::Fifo<key::Handle<runtime::Domain<Tag>>, runtime::Indexed>,
}

impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>>
    Occupied<'a, 'd, P, B, T, D>
{
    pub(in crate::manifold::requests) fn reserve(
        self,
    ) -> Result<Reserved<'a, 'd, P, B, T, D>, Phase> {
        match &self.entry.get().entry {
            Entry::Reserved => Ok(self.reserved()),
            entry => Err(entry.phase()),
        }
    }

    fn reserved(self) -> Reserved<'a, 'd, P, B, T, D> {
        Reserved {
            // SAFETY: both callers retain the exclusive map loan after creating or
            // observing `Entry::Reserved`, which is unlinked in both resident FIFOs.
            entry: unsafe { keyed::Slot::<_, _, false>::from_source(depot::raw::Proof::new(self)) }
                .release(),
        }
    }

    fn classify(mut self) -> State<'a, 'd, P, B, T, D> {
        let place = match &self.entry.get().entry {
            Entry::Building(_) => {
                return match keyed::Slot::try_new(self) {
                    Ok(cursor) => State::Ready(cursor.release()),
                    Err(vacant) => State::Idle(vacant.release()),
                };
            }
            Entry::Responding(state) => state.queue,
            Entry::Duplex(state) => state.queue,
            Entry::Deferred(_) => status::Queue::Ready,
            Entry::Reserved | Entry::Pending(_) | Entry::Active(_) | Entry::Stopped => {
                status::Queue::Idle
            }
        };
        match place {
            status::Queue::Ready => State::Ready(
                // SAFETY: the recorded phase places this slot in the ready FIFO carried by `queue`.
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
                // SAFETY: every phase reaching `Queue::Idle` is unlinked in both resident FIFOs.
                unsafe { keyed::Slot::<_, _, false>::from_source(depot::raw::Proof::new(self)) }
                    .release(),
            ),
        }
    }
}

pub(super) struct Member<
    'a,
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: duplex::Session<'d>,
> {
    owner: Occupied<'a, 'd, P, B, T, D>,
}

impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>>
    Member<'a, 'd, P, B, T, D>
{
    fn remove(self) -> Detached<'a, 'd, P, B, T, D> {
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

pub(super) struct Detached<
    'a,
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: duplex::Session<'d>,
> {
    owner: Occupied<'a, 'd, P, B, T, D>,
}

impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>>
    Detached<'a, 'd, P, B, T, D>
{
    fn push(self) -> Member<'a, 'd, P, B, T, D> {
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

pub(super) struct Front<
    'a,
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: duplex::Session<'d>,
    const WAIT: bool,
> {
    entries: &'a mut Entries<'d, P, B, T, D>,
}

// SAFETY: `Occupied` holds one slot's exclusive map loan plus both resident FIFOs; `parts`
// yields that slot's exact key and link, `fifo` the queue it is a member of, and every
// transition re-establishes the pair before the borrowed root authority escapes.
unsafe impl<'d, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>>
    depot::raw::KeyOwner<runtime::Indexed> for Occupied<'_, 'd, P, B, T, D>
{
    type Key = key::Handle<runtime::Domain<Tag>>;
    type Fifo = keyed::Fifo<Self::Key, runtime::Indexed>;

    fn parts(&self) -> (Self::Key, &keyed::Link<Self::Key, runtime::Indexed>) {
        depot::raw::KeyBorrow::parts(Borrowed(&self.entry))
    }

    fn fifo(&self) -> &keyed::Fifo<Self::Key, runtime::Indexed> {
        self.queue
    }

    fn entry<'a>(
        live: depot::raw::Proof<(&'a Self, links::Index), Initialized<runtime::Indexed>>,
    ) -> keyed::Entry<'a, Self::Key, runtime::Indexed> {
        keyed::Entry::from_source(Borrowed(live))
    }
}

// SAFETY: `Member` is only released for a slot whose recorded phase proves membership in the
// FIFO its owner names, and it consumes that owner and root authority together.
unsafe impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>>
    depot::raw::KeyFinish<true, runtime::Indexed> for Occupied<'a, 'd, P, B, T, D>
{
    type Output = Member<'a, 'd, P, B, T, D>;

    unsafe fn finish(self) -> Self::Output {
        Member { owner: self }
    }
}

// SAFETY: `Detached` is only released for a slot that left its FIFO or was proven vacant by
// `keyed::Slot::try_new`, and it consumes the same owner and root authority.
unsafe impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: duplex::Session<'d>>
    depot::raw::KeyFinish<false, runtime::Indexed> for Occupied<'a, 'd, P, B, T, D>
{
    type Output = Detached<'a, 'd, P, B, T, D>;

    unsafe fn finish(self) -> Self::Output {
        Detached { owner: self }
    }
}

// SAFETY: `Front` borrows both resident FIFOs for `'a` and returns exactly the one named by
// `WAIT`, so a retained source keeps that FIFO's storage, endpoints and link plane.
unsafe impl<
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: duplex::Session<'d>,
    const WAIT: bool,
> depot::raw::KeySource<runtime::Indexed> for Front<'_, 'd, P, B, T, D, WAIT>
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
unsafe impl<
    'a,
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: duplex::Session<'d>,
    const WAIT: bool,
> depot::raw::KeyPlane<runtime::Indexed> for Front<'a, 'd, P, B, T, D, WAIT>
{
    type Slot = Occupied<'a, 'd, P, B, T, D>;

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
