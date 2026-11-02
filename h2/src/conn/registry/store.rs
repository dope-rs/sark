use core::marker;

use o3::{
    alloc,
    depot::{
        arena::{slab, slab::entries},
        map,
        map::flat,
        queue::indexed::keyed,
    },
};

use crate::{
    conn,
    conn::registry::{Active, Changes, History, Past, Peer, Wake},
    flow,
    frame::numbers,
    stream,
};

enum State {
    Active(Active),
    Reset {
        error: numbers::ErrorCode,
        next: Option<stream::Key>,
    },
}

pub(super) struct Record {
    pub(super) key: stream::Key,
    pub(super) send: keyed::Link<stream::Key, Send>,
    state: State,
}

pub(super) enum Change {
    Closed,
    Idle,
    Opened,
}

impl Record {
    pub(super) fn update<E>(
        &mut self,
        edit: impl FnOnce(&mut Active) -> Result<(), E>,
    ) -> Result<Change, E> {
        let State::Active(active) = &mut self.state else {
            return Ok(Change::Closed);
        };
        let blocked = active.send_window.is_stalled();
        edit(active)?;
        Ok(if blocked && !active.send_window.is_stalled() {
            Change::Opened
        } else {
            Change::Idle
        })
    }
}

pub(super) enum Send {}

pub(crate) enum Lookup<'a> {
    Active(stream::Key, &'a mut Active),
    Class(Past),
}

pub(crate) enum Admission<'s, 'h, R: crate::Role, T> {
    Active(T),
    Past(Past),
    Open(Vacant<'s>, Peer<'h, R>),
    Full(Peer<'h, R>),
    Exhausted(stream::Id),
}

/// The caller decides while borrowing the original active record; the store
/// applies the transition through the same occupied entry after that borrow ends.
pub(crate) enum Action<T> {
    Keep(T),
    Remove(T),
    Reset(numbers::ErrorCode, T),
}

type Occupied<'a> = slab::Occupied<
    entries::Busy<'a, Record, { u32::MAX }, entries::Exclusive<false>, 1, ()>,
    (),
    true,
>;

pub(crate) struct Vacant<'a> {
    id: stream::Nonzero,
    index: map::Vacancy<&'a mut flat::Table<stream::Key>, u64>,
    slot: slab::Vacant<
        entries::Pending<'a, Record, { u32::MAX }, entries::Exclusive<false>, 1, ()>,
        (),
        true,
    >,
    count: &'a mut usize,
}

pub(super) struct Transition<'a> {
    index: map::Found<&'a mut flat::Table<stream::Key>>,
    slot: keyed::Slot<Wake<'a>, Send, false>,
    count: &'a mut usize,
}

pub(super) struct Reset<'a> {
    index: map::Found<&'a mut flat::Table<stream::Key>>,
    slot: Occupied<'a>,
    error: numbers::ErrorCode,
    next: Option<stream::Key>,
}

pub(crate) struct Store<R: crate::Role> {
    role: marker::PhantomData<R>,
    pub(super) by_id: flat::Table<stream::Key>,
    pub(super) streams: slab::Store<Record>,
    local_count: usize,
    peer_count: usize,
}

impl<R: crate::Role> Store<R> {
    pub(crate) fn try_new(plan: map::Plan) -> Result<Self, alloc::Error> {
        let capacity = plan.capacity();
        Ok(Self {
            role: marker::PhantomData,
            by_id: flat::Table::try_new(plan)?,
            streams: slab::Store::<Record>::try_new(capacity)?,
            local_count: 0,
            peer_count: 0,
        })
    }

    pub(super) fn admit<'s, 'h, T, E>(
        &'s mut self,
        history: &'h mut History<R>,
        proof: stream::Nonzero,
        limit: usize,
        changes: Changes<'_>,
        edit: impl FnOnce(stream::Key, &mut Active) -> Result<Action<T>, E>,
    ) -> Result<Admission<'s, 'h, R, T>, E> {
        use flat::Entry;

        let id = proof.get();
        let Self {
            by_id,
            streams,
            peer_count: count,
            ..
        } = self;
        let index = match by_id.entry(u64::from(id.into_u32()), |key| key.id() == id) {
            Some(Entry::Occupied(entry)) => {
                let key = *entry.get();
                let Some(slot) = slab::Slots::new(&mut *streams).entry_at(key.slot()) else {
                    return Ok(Admission::Past(history.classify(proof)));
                };
                let count = if History::<R>::is_local(id) {
                    &mut self.local_count
                } else {
                    count
                };
                return Self::edit(entry, slot, count, changes, (), |key, active, ()| {
                    edit(key, active)
                })
                .map(|result| match result {
                    Ok(value) => Admission::Active(value),
                    Err(()) => Admission::Past(Past::Closed),
                });
            }
            Some(Entry::Vacant(entry)) => Some(entry),
            None => None,
        };
        let past = history.classify(proof);
        if past != Past::Idle {
            return Ok(Admission::Past(past));
        }
        let peer = match history.peer(proof) {
            Some(peer) => peer,
            None => return Ok(Admission::Past(Past::Idle)),
        };
        let Some(index) = index else {
            return Ok(Admission::Full(peer));
        };
        if *count >= limit {
            return Ok(Admission::Full(peer));
        }
        let slot = match streams.try_vacant() {
            Ok(slot) => slot,
            Err(slab::Vacancy::Full) => return Ok(Admission::Full(peer)),
            Err(slab::Vacancy::IdentityExhausted) => return Ok(Admission::Exhausted(id)),
        };
        Ok(Admission::Open(
            Vacant {
                id: proof,
                index,
                slot,
                count,
            },
            peer,
        ))
    }

    pub(super) fn visit<I, T, E>(
        &mut self,
        history: &History<R>,
        proof: stream::Nonzero,
        changes: Changes<'_>,
        input: I,
        edit: impl FnOnce(stream::Key, &mut Active, I) -> Result<Action<T>, E>,
    ) -> Result<Result<T, (Past, I)>, E> {
        let id = proof.get();
        let Some(index) = self
            .by_id
            .occupied(u64::from(id.into_u32()), |key| key.id() == id)
        else {
            return Ok(Err((history.classify(proof), input)));
        };
        let key = *index.get();
        let Some(slot) = slab::Slots::new(&mut self.streams).entry_at(key.slot()) else {
            return Ok(Err((history.classify(proof), input)));
        };
        let count = if History::<R>::is_local(id) {
            &mut self.local_count
        } else {
            &mut self.peer_count
        };
        Self::edit(index, slot, count, changes, input, edit)
            .map(|value| value.map_err(|input| (Past::Closed, input)))
    }

    fn edit<'a, I, T, E>(
        index: map::Found<&'a mut flat::Table<stream::Key>>,
        mut slot: Occupied<'a>,
        count: &'a mut usize,
        changes: Changes<'a>,
        input: I,
        edit: impl FnOnce(stream::Key, &mut Active, I) -> Result<Action<T>, E>,
    ) -> Result<Result<T, I>, E> {
        let key = *index.get();
        let record = slot.get_mut();
        if record.key != key {
            return Ok(Err(input));
        }
        let State::Active(active) = &mut record.state else {
            return Ok(Err(input));
        };
        let action = edit(key, active, input)?;
        let value = match action {
            Action::Keep(value) => value,
            Action::Remove(value) => {
                Transition {
                    index,
                    slot: Wake::clear(slot, changes.wakes),
                    count,
                }
                .remove();
                value
            }
            Action::Reset(error, value) => {
                Transition {
                    index,
                    slot: Wake::clear(slot, changes.wakes),
                    count,
                }
                .reset(error, *changes.head);
                *changes.head = Some(key);
                *changes.count += 1;
                value
            }
        };
        Ok(Ok(value))
    }

    pub(crate) fn vacant(
        &mut self,
        nonzero: stream::Nonzero,
    ) -> Result<Vacant<'_>, conn::Admission> {
        use flat::Entry;

        let id = nonzero.get();
        let Self {
            by_id,
            streams,
            local_count: local,
            peer_count: peer,
            ..
        } = self;
        let full = conn::Admission::Full {
            stream: id,
            used: streams.len(),
            limit: streams.capacity(),
        };
        let index = match by_id.entry(u64::from(id.into_u32()), |key| key.id() == id) {
            Some(Entry::Vacant(entry)) => entry,
            Some(Entry::Occupied(_)) => return Err(conn::Admission::Duplicate(id)),
            None => return Err(full),
        };
        let slot = streams.try_vacant().map_err(|error| match error {
            slab::Vacancy::Full => full,
            slab::Vacancy::IdentityExhausted => conn::Admission::IdentityExhausted(id),
        })?;
        let count = if History::<R>::is_local(id) {
            local
        } else {
            peer
        };
        Ok(Vacant {
            id: nonzero,
            index,
            slot,
            count,
        })
    }

    pub(crate) fn lookup<'a>(
        &'a mut self,
        history: &History<R>,
        proof: stream::Nonzero,
    ) -> Lookup<'a> {
        let id = proof.get();
        let key = match self
            .by_id
            .get(u64::from(id.into_u32()), |key| key.id() == id)
            .copied()
        {
            Some(key) => key,
            None => return Lookup::Class(history.classify(proof)),
        };
        match self.slot_mut(key).map(|record| &mut record.state) {
            Some(State::Active(active)) => Lookup::Active(key, active),
            Some(State::Reset { .. }) => Lookup::Class(Past::Closed),
            None => Lookup::Class(history.classify(proof)),
        }
    }

    pub(crate) fn key(&self, id: stream::Nonzero) -> Option<stream::Key> {
        let key = self
            .by_id
            .get(u64::from(id.get().into_u32()), |key| key.nonzero() == id)
            .copied()?;
        self.record_at(key)?;
        Some(key)
    }

    pub(crate) fn record_at(&self, key: stream::Key) -> Option<&Active> {
        match &self.slot(key)?.state {
            State::Active(active) => Some(active),
            State::Reset { .. } => None,
        }
    }

    pub(crate) fn active_mut(&mut self, key: stream::Key) -> Option<&mut Active> {
        match &mut self.slot_mut(key)?.state {
            State::Active(active) => Some(active),
            State::Reset { .. } => None,
        }
    }

    pub(super) fn entry<'a>(
        &'a mut self,
        id: stream::Id,
        matches: impl Fn(&stream::Key) -> bool,
        wakes: &'a mut keyed::Fifo<stream::Key, Send>,
    ) -> Option<Transition<'a>> {
        let Self {
            by_id,
            streams,
            local_count: local,
            peer_count: peer,
            ..
        } = self;
        let index = by_id.occupied(u64::from(id.into_u32()), matches)?;
        let key = *index.get();
        let slot = slab::Slots::new(&mut *streams).entry_at(key.index() as u32)?;
        if slot.get().key != key || !matches!(slot.get().state, State::Active(_)) {
            return None;
        }
        let count = if History::<R>::is_local(key.id()) {
            local
        } else {
            peer
        };
        Some(Transition {
            index,
            slot: Wake::clear(slot, wakes),
            count,
        })
    }

    pub(super) fn reset(&mut self, key: stream::Key) -> Option<Reset<'_>> {
        let Self { by_id, streams, .. } = self;
        let index = by_id.occupied(u64::from(key.id().into_u32()), |entry| *entry == key)?;
        let slot = slab::Slots::new(&mut *streams).entry_at(key.index() as u32)?;
        if slot.get().key != key {
            return None;
        }
        let (error, next) = match slot.get().state {
            State::Reset { error, next } => (error, next),
            State::Active(_) => return None,
        };
        Some(Reset {
            index,
            slot,
            error,
            next,
        })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.streams.capacity()
    }

    pub(crate) fn active_len(&self) -> usize {
        self.local_count + self.peer_count
    }

    pub(crate) fn local_len(&self) -> usize {
        self.local_count
    }

    fn slot(&self, key: stream::Key) -> Option<&Record> {
        let record = slab::Slots::new(&self.streams).value_at(key.slot())?;
        (record.key == key).then_some(record)
    }

    fn slot_mut(&mut self, key: stream::Key) -> Option<&mut Record> {
        let record = slab::Slots::new(&mut self.streams)
            .entry_at(key.slot())?
            .into_mut();
        (record.key == key).then_some(record)
    }
}

impl Vacant<'_> {
    pub(crate) fn insert(
        self,
        stream: stream::Lifecycle,
        send: u32,
        recv: u32,
        remaining: u64,
    ) -> stream::Key {
        use flow::Window;

        let Self {
            id,
            index,
            slot,
            count,
        } = self;
        let slot = slot
            .stage_with(|index| {
                let key = stream::Key::from_parts(id, index);
                Record {
                    key,
                    send: keyed::Link::from(index),
                    state: State::Active(Active {
                        stream,
                        send_window: Window::with(send as i32),
                        recv_window: Window::with(recv as i32),
                        pending_release: 0,
                        remaining,
                    }),
                }
            })
            .commit();
        let key = slot.get().key;
        index.insert(key);
        *count += 1;
        key
    }
}

impl Transition<'_> {
    pub(super) fn remove(self) -> stream::Key {
        let Self { index, slot, count } = self;
        let key = index.remove();
        slot.release().remove();
        *count -= 1;
        key
    }

    pub(super) fn reset(self, error: numbers::ErrorCode, next: Option<stream::Key>) {
        self.slot.release().get_mut().state = State::Reset { error, next };
        *self.count -= 1;
    }
}

impl Reset<'_> {
    pub(super) fn key(&self) -> stream::Key {
        self.slot.get().key
    }

    pub(super) const fn error(&self) -> numbers::ErrorCode {
        self.error
    }

    pub(super) const fn next(&self) -> Option<stream::Key> {
        self.next
    }

    pub(super) fn finish(self) {
        self.index.remove();
        self.slot.remove();
    }
}
