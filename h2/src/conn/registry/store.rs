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
    conn::registry::{Active, Changes, History, Past, Peer, Registry, Update, Wake},
    flow,
    frame::numbers,
    stream,
};

enum State {
    Active(Active),
    Reset(ResetState, Option<Closed>),
    Closed(Closed),
}

#[derive(Clone, Copy)]
pub(super) enum Live {
    Initial,
    Trailing,
    Duplex,
    End,
    DuplexEnd,
}

#[derive(Clone, Copy)]
enum Closed {
    Duplex,
    End,
    DuplexEnd,
    Reset,
    DuplexReset,
}

#[derive(Clone, Copy)]
struct ResetState {
    error: numbers::ErrorCode,
    next: Option<stream::Key>,
}

#[derive(Clone, Copy)]
pub(crate) enum Notice {
    End,
    Reset,
    Cancel,
    Duplex,
}

impl Live {
    fn retire(self, reset: bool) -> Option<Closed> {
        Some(match (self, reset) {
            (Self::Initial | Self::Trailing, false) => return None,
            (Self::Duplex, false) => Closed::Duplex,
            (Self::End, false) => Closed::End,
            (Self::DuplexEnd, false) => Closed::DuplexEnd,
            (Self::Initial | Self::Trailing | Self::End, true) => Closed::Reset,
            (Self::Duplex | Self::DuplexEnd, true) => Closed::DuplexReset,
        })
    }
}

impl Closed {
    fn release(&mut self, notice: Notice) -> bool {
        *self = match (*self, notice) {
            (Self::End, Notice::End | Notice::Cancel)
            | (Self::Reset, Notice::Reset | Notice::Cancel)
            | (Self::Duplex, Notice::Duplex) => return true,
            (Self::DuplexEnd, Notice::End | Notice::Cancel)
            | (Self::DuplexReset, Notice::Reset | Notice::Cancel) => Self::Duplex,
            (Self::DuplexEnd, Notice::Duplex) => Self::End,
            (Self::DuplexReset, Notice::Duplex) => Self::Reset,
            (_, Notice::Duplex) => unreachable!("stream"),
            (phase, _) => phase,
        };
        false
    }
}

impl State {
    fn active(&self) -> Option<&Active> {
        match self {
            Self::Active(active) => Some(active),
            _ => None,
        }
    }

    fn active_mut(&mut self) -> Option<&mut Active> {
        match self {
            Self::Active(active) => Some(active),
            _ => None,
        }
    }

    fn reset(&self) -> Option<ResetState> {
        match self {
            Self::Reset(reset, _) => Some(*reset),
            _ => None,
        }
    }

    fn owns_exchange(&self) -> bool {
        !matches!(self, Self::Reset(_, None))
    }

    fn close(&mut self, reset: bool) -> bool {
        let owner = match self {
            Self::Active(active) => active.peer.retire(reset),
            Self::Reset(_, owner) => *owner,
            Self::Closed(_) => unreachable!("stream"),
        };
        match owner {
            Some(owner) => *self = Self::Closed(owner),
            None => return true,
        }
        false
    }

    fn begin_reset(&mut self, reset: ResetState, notify: bool) {
        let Self::Active(active) = self else {
            unreachable!("stream")
        };
        *self = Self::Reset(reset, active.peer.retire(notify));
    }

    fn release(&mut self, notice: Notice) -> bool {
        match self {
            Self::Active(active) => {
                active.peer = match (active.peer, notice) {
                    (Live::End, Notice::End | Notice::Cancel) => Live::Trailing,
                    (Live::DuplexEnd, Notice::End | Notice::Cancel) => Live::Duplex,
                    (Live::Duplex, Notice::Duplex) => Live::Trailing,
                    (Live::DuplexEnd, Notice::Duplex) => Live::End,
                    (_, Notice::Duplex) => unreachable!("stream"),
                    (phase, _) => phase,
                };
            }
            Self::Reset(_, owner) => {
                if let Some(phase) = owner {
                    if phase.release(notice) {
                        *owner = None;
                    }
                } else if matches!(notice, Notice::Duplex) {
                    unreachable!("stream");
                }
            }
            Self::Closed(owner) => return owner.release(notice),
        }
        false
    }

    fn hold_duplex(&mut self) {
        let Self::Active(active) = self else {
            unreachable!("stream")
        };
        active.peer = match active.peer {
            Live::Trailing => Live::Duplex,
            Live::End => Live::DuplexEnd,
            _ => unreachable!("stream"),
        };
    }
}

pub(super) struct Record {
    pub(super) key: stream::Key,
    pub(super) send: keyed::Link<stream::Key, Send>,
    state: State,
}

impl Wake<'_> {
    pub(super) fn update<E>(
        mut self,
        edit: impl FnOnce(&mut Active) -> Result<(), E>,
    ) -> Result<Update, E> {
        let Some(active) = self.entry.get_mut().state.active_mut() else {
            return Ok(Update::Past(Past::Closed));
        };
        let blocked = active.send_window.is_stalled();
        edit(active)?;
        if blocked
            && !active.send_window.is_stalled()
            && let Ok(vacant) = keyed::Slot::try_vacant(self)
        {
            vacant.push::<false>();
        }
        Ok(Update::Applied)
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
    Reset(numbers::ErrorCode, bool, T),
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
        let Some(active) = record.state.active_mut() else {
            return Ok(Err(input));
        };
        let value = match edit(key, active, input)? {
            Action::Keep(value) => value,
            Action::Remove(value) => {
                Transition {
                    index,
                    slot: Wake::clear(slot, changes.wakes),
                    count,
                }
                .remove(false);
                value
            }
            Action::Reset(error, notify, value) => {
                Transition {
                    index,
                    slot: Wake::clear(slot, changes.wakes),
                    count,
                }
                .reset(error, *changes.head, notify);
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
        match self.slot_mut(key) {
            Some(record) => match record.state.active_mut() {
                Some(active) => Lookup::Active(key, active),
                None => Lookup::Class(Past::Closed),
            },
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
        self.slot(key)?.state.active()
    }

    pub(crate) fn active_mut(&mut self, key: stream::Key) -> Option<&mut Active> {
        self.slot_mut(key)?.state.active_mut()
    }

    pub(super) fn entry<'a>(
        &'a mut self,
        key: stream::Key,
        wakes: &'a mut keyed::Fifo<stream::Key, Send>,
    ) -> Option<Transition<'a>> {
        let Self {
            by_id,
            streams,
            local_count: local,
            peer_count: peer,
            ..
        } = self;
        let index = by_id.occupied(u64::from(key.id().into_u32()), |entry| *entry == key)?;
        let slot = slab::Slots::new(&mut *streams).entry_at(key.index() as u32)?;
        if slot.get().state.active().is_none() {
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
        let ResetState { error, next } = slot.get().state.reset()?;
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

    pub(crate) fn owns_exchange(&self, key: stream::Key) -> bool {
        self.slot(key)
            .is_some_and(|record| record.state.owns_exchange())
    }

    pub(crate) fn hold_duplex(&mut self, key: stream::Key) {
        if let Some(record) = self.slot_mut(key) {
            record.state.hold_duplex();
        }
    }

    pub(crate) fn release(&mut self, key: stream::Key, notice: Notice) {
        let Some(mut slot) = slab::Slots::new(&mut self.streams).entry_at(key.slot()) else {
            return;
        };
        if slot.get().key != key {
            return;
        }
        if slot.get_mut().state.release(notice) {
            slot.discard();
        }
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

impl<R: crate::Role> Registry<R> {
    pub(crate) fn admit<'s, 'h, T, E>(
        &'s mut self,
        history: &'h mut History<R>,
        proof: stream::Nonzero,
        limit: usize,
        edit: impl FnOnce(stream::Key, &mut Active) -> Result<Action<T>, E>,
    ) -> Result<Admission<'s, 'h, R, T>, E> {
        use flat::Entry;

        let id = proof.get();
        let Store {
            by_id,
            streams,
            peer_count: count,
            local_count,
            ..
        } = &mut self.store;
        let index = match by_id.entry(u64::from(id.into_u32()), |key| key.id() == id) {
            Some(Entry::Occupied(entry)) => {
                let key = *entry.get();
                let Some(slot) = slab::Slots::new(&mut *streams).entry_at(key.slot()) else {
                    return Ok(Admission::Past(history.classify(proof)));
                };
                let count = if History::<R>::is_local(id) {
                    local_count
                } else {
                    count
                };
                return Store::<R>::edit(
                    entry,
                    slot,
                    count,
                    Changes {
                        wakes: &mut self.send_wakes,
                        head: &mut self.pending_reset,
                        count: &mut self.reset_count,
                    },
                    (),
                    |key, active, ()| edit(key, active),
                )
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

    pub(crate) fn visit<I, T, E>(
        &mut self,
        history: &History<R>,
        proof: stream::Nonzero,
        input: I,
        edit: impl FnOnce(stream::Key, &mut Active, I) -> Result<Action<T>, E>,
    ) -> Result<Result<T, (Past, I)>, E> {
        let id = proof.get();
        let Some(index) = self
            .store
            .by_id
            .occupied(u64::from(id.into_u32()), |key| key.id() == id)
        else {
            return Ok(Err((history.classify(proof), input)));
        };
        let key = *index.get();
        let Some(slot) = slab::Slots::new(&mut self.store.streams).entry_at(key.slot()) else {
            return Ok(Err((history.classify(proof), input)));
        };
        let count = if History::<R>::is_local(id) {
            &mut self.store.local_count
        } else {
            &mut self.store.peer_count
        };
        Store::<R>::edit(
            index,
            slot,
            count,
            Changes {
                wakes: &mut self.send_wakes,
                head: &mut self.pending_reset,
                count: &mut self.reset_count,
            },
            input,
            edit,
        )
        .map(|value| value.map_err(|input| (Past::Closed, input)))
    }
}

impl Vacant<'_> {
    pub(crate) fn insert(
        self,
        stream: stream::Lifecycle,
        send: u32,
        recv: u32,
        remaining: u64,
        end: bool,
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
                let active = Active {
                    state: stream.state,
                    method: stream.method,
                    content: stream.content,
                    peer: if end {
                        Live::End
                    } else {
                        match stream.peer_section {
                            sark_core::manifold::stream::Section::Initial => Live::Initial,
                            sark_core::manifold::stream::Section::Trailing => Live::Trailing,
                        }
                    },
                    send_window: Window::with(send as i32),
                    recv_window: Window::with(recv as i32),
                    pending_release: 0,
                    remaining,
                };
                Record {
                    key,
                    send: keyed::Link::from(index),
                    state: State::Active(active),
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
    pub(super) fn remove(self, reset: bool) {
        let Self { index, slot, count } = self;
        finish(index, slot.release(), reset);
        *count -= 1;
    }

    pub(super) fn reset(self, error: numbers::ErrorCode, next: Option<stream::Key>, notify: bool) {
        let mut slot = self.slot.release();
        slot.get_mut()
            .state
            .begin_reset(ResetState { error, next }, notify);
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
        finish(self.index, self.slot, false);
    }
}

fn finish(index: map::Found<&mut flat::Table<stream::Key>>, mut slot: Occupied<'_>, reset: bool) {
    index.remove();
    if slot.get_mut().state.close(reset) {
        slot.discard();
    }
}
