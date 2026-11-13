use o3::{
    alloc, depot,
    depot::{
        arena::{slab, slab::entries},
        index::Initialized,
        map,
        queue::{indexed::keyed, links},
    },
};
use sark_core::manifold::stream::Section;
use sark_http::payload::response::Method;

use crate::{Endpoint, conn, flow, frame::numbers, stream};

mod history;
pub(crate) mod store;

pub(crate) use history::{History, Peer};

#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) enum Past {
    Closed,
    Idle,
}

pub(crate) enum Update {
    Applied,
    Past(Past),
}

/// Zero-storage proof that PUSH_PROMISE has a valid associated request.
#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) struct Association;

pub(crate) struct Active {
    pub(crate) state: stream::State,
    pub(crate) method: Option<Method>,
    pub(crate) content: stream::Content,
    peer: store::Live,
    pub(crate) send_window: flow::Window,
    pub(crate) recv_window: u32,
    pub(crate) remaining: u64,
}

impl Active {
    pub(crate) fn section(&self) -> Section {
        match self.peer {
            store::Live::Initial => Section::Initial,
            _ => Section::Trailing,
        }
    }

    pub(crate) fn complete_head(&mut self) {
        if matches!(self.peer, store::Live::Initial) {
            self.peer = store::Live::Trailing;
        }
    }

    #[inline]
    pub(crate) fn queue_end(&mut self) {
        self.peer = match self.peer {
            store::Live::Trailing => store::Live::End,
            store::Live::Duplex => store::Live::DuplexEnd,
            _ => unreachable!("stream"),
        };
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Begin {
    Started,
    Inactive,
}

pub(super) struct Changes<'a> {
    wakes: &'a mut keyed::Fifo<stream::Key, store::Send>,
    head: &'a mut Option<stream::Key>,
    count: &'a mut usize,
}

pub(crate) struct Pending<'a> {
    head: &'a mut Option<stream::Key>,
    count: &'a mut usize,
    reset: store::Reset<'a>,
}

pub(crate) struct Registry<R: crate::Role> {
    pub(crate) store: store::Store<R>,
    send_wakes: keyed::Fifo<stream::Key, store::Send>,
    pending_reset: Option<stream::Key>,
    reset_count: usize,
}

impl<R: crate::Role> Registry<R> {
    pub(crate) fn try_new(plan: map::Plan) -> Result<Self, alloc::Error> {
        Ok(Self {
            store: store::Store::try_new(plan)?,
            send_wakes: keyed::Fifo::new(),
            pending_reset: None,
            reset_count: 0,
        })
    }

    pub(crate) fn add_send(
        &mut self,
        history: &History<R>,
        proof: stream::Nonzero,
        delta: numbers::Increment,
    ) -> Result<Update, conn::Error> {
        let Some(wake) = Plane::new(self).find(proof)? else {
            return Ok(Update::Past(history.classify(proof)));
        };
        wake.update(|active| {
            if active.state == stream::State::ReservedRemote {
                return Err(conn::Error::Protocol(conn::Protocol::WindowUpdate {
                    stream: proof.get(),
                    state: active.state,
                }));
            }
            active
                .send_window
                .increase(delta)
                .map_err(conn::Error::Increase)
        })
    }

    pub(crate) fn adjust_send(&mut self, delta: i32) -> Result<(), flow::Adjust> {
        if self.store.active_len() == 0 {
            return Ok(());
        }
        for index in 0..self.store.capacity() {
            let Some(wake) = Plane::new(self).at(index as u32) else {
                continue;
            };
            wake.update(|active| active.send_window.adjust(delta))?;
        }
        Ok(())
    }

    pub(crate) fn poll_wake(&mut self) -> Option<stream::Key> {
        let cursor = keyed::Slot::front(Plane::new(self))?;
        let key = cursor.key();
        cursor.remove();
        Some(key)
    }

    pub(crate) fn remove_key(&mut self, key: stream::Key, reset: bool) {
        let Self {
            store,
            send_wakes: wakes,
            ..
        } = self;
        if let Some(entry) = store.entry(key, wakes) {
            entry.remove(reset);
        }
    }

    pub(crate) fn begin_reset(&mut self, key: stream::Key, error: numbers::ErrorCode) -> Begin {
        let Self {
            store,
            send_wakes: wakes,
            pending_reset: head,
            reset_count: count,
        } = self;
        let Some(entry) = store.entry(key, wakes) else {
            return Begin::Inactive;
        };
        entry.reset(error, *head, false);
        *head = Some(key);
        *count += 1;
        Begin::Started
    }

    pub(crate) fn reset(&mut self) -> Option<Pending<'_>> {
        let Self {
            store,
            pending_reset: head,
            reset_count: count,
            ..
        } = self;
        let reset = store.reset((*head)?)?;
        Some(Pending { head, count, reset })
    }

    pub(crate) fn reset_len(&self) -> usize {
        self.reset_count
    }

    pub(crate) fn association(
        &mut self,
        history: &History<R>,
        proof: stream::Nonzero,
    ) -> Result<Association, conn::Association> {
        let id = proof.get();
        match R::ENDPOINT {
            Endpoint::Server => {
                return Err(conn::Association::Role {
                    stream: id,
                    endpoint: Endpoint::Server,
                });
            }
            Endpoint::Client => {}
        }
        if !History::<R>::is_local(id) {
            return Err(conn::Association::Peer(id));
        }
        match self.store.lookup(history, proof) {
            store::Lookup::Active(_, record) if record.content == stream::Content::Tunnel => {
                Err(conn::Association::Tunnel(id))
            }
            store::Lookup::Active(_, record) => match record.state {
                stream::State::Open | stream::State::HalfClosedLocal => Ok(Association),
                state => Err(conn::Association::State { stream: id, state }),
            },
            store::Lookup::Class(Past::Closed) => Ok(Association),
            store::Lookup::Class(Past::Idle) => Err(conn::Association::Idle(id)),
        }
    }
}

impl Pending<'_> {
    pub(crate) fn key(&self) -> stream::Key {
        self.reset.key()
    }

    pub(crate) const fn error(&self) -> numbers::ErrorCode {
        self.reset.error()
    }

    pub(crate) fn commit(self) {
        *self.head = self.reset.next();
        *self.count -= 1;
        self.reset.finish();
    }
}

const _: () = assert!(core::mem::size_of::<Association>() == 0);

pub(super) struct Plane<'a, R: crate::Role>(&'a mut Registry<R>);

pub(super) struct Wake<'a, P = ()> {
    entry: slab::Occupied<
        entries::Busy<'a, store::Record, { u32::MAX }, entries::Exclusive<false>, 1, P>,
        (),
        true,
    >,
    fifo: &'a mut keyed::Fifo<stream::Key, store::Send>,
}

impl<'a, R: crate::Role> Plane<'a, R> {
    fn new(owner: &'a mut Registry<R>) -> Self {
        Self(owner)
    }

    fn find(self, proof: stream::Nonzero) -> Result<Option<Wake<'a>>, conn::Error> {
        let id = proof.get();
        let Some(key) = self
            .0
            .store
            .by_id
            .get(u64::from(id.into_u32()), |key| key.id() == id)
            .copied()
        else {
            return Ok(None);
        };
        let entry = self
            .0
            .store
            .streams
            .try_entry_at(key.slot())
            .map_err(|cause| conn::Error::WakeSlot { key, cause })?;
        let actual = entry.get().key;
        if actual != key {
            return Err(conn::Error::WakeIdentity {
                expected: key,
                actual,
            });
        }
        Ok(Some(Wake {
            entry,
            fifo: &mut self.0.send_wakes,
        }))
    }

    fn at(self, index: u32) -> Option<Wake<'a>> {
        let entry = self.0.store.streams.entry_at(index)?;
        Some(Wake {
            entry,
            fifo: &mut self.0.send_wakes,
        })
    }
}

impl<'a> Wake<'a> {
    fn clear(
        entry: slab::Occupied<
            entries::Busy<'a, store::Record, { u32::MAX }, entries::Exclusive<false>, 1, ()>,
            (),
            true,
        >,
        fifo: &'a mut keyed::Fifo<stream::Key, store::Send>,
    ) -> keyed::Slot<Self, store::Send, false> {
        match keyed::Slot::try_new(Self { entry, fifo }) {
            Ok(member) => member.remove(),
            Err(vacant) => vacant,
        }
    }
}

// SAFETY: the detached capability retains this exact initialized slab slot.
unsafe impl<'a> depot::raw::KeyFinish<false, store::Send> for Wake<'a> {
    type Output = slab::Occupied<
        entries::Busy<'a, store::Record, { u32::MAX }, entries::Exclusive<false>, 1, ()>,
        (),
        true,
    >;

    unsafe fn finish(self) -> Self::Output {
        self.entry
    }
}

// SAFETY: the plane retains the original exclusive Registry and its permanently paired FIFO.
unsafe impl<R: crate::Role> depot::raw::KeySource<store::Send> for Plane<'_, R> {
    type Key = stream::Key;

    fn fifo(&self) -> &keyed::Fifo<stream::Key, store::Send> {
        &self.0.send_wakes
    }
}

unsafe impl<'a, R: crate::Role> depot::raw::KeyPlane<store::Send> for Plane<'a, R> {
    type Slot = Wake<'a, store::Send>;

    fn select(
        live: depot::raw::Proof<(Self, links::Index), Initialized<store::Send>>,
    ) -> Self::Slot {
        let live = unsafe {
            live.map(|(owner, index)| {
                ((&mut owner.0.store.streams, &mut owner.0.send_wakes), index)
            })
        };
        let (entry, fifo) = slab::Occupied::from_live(live);
        Wake { entry, fifo }
    }
}

// SAFETY: Wake retains this root's exclusive slot guard and its sole eligible FIFO.
unsafe impl<P> depot::raw::KeyOwner<store::Send> for Wake<'_, P> {
    type Key = stream::Key;
    type Fifo = keyed::Fifo<stream::Key, store::Send>;

    fn parts(&self) -> (stream::Key, &keyed::Link<stream::Key, store::Send>) {
        let record = self.entry.get();
        (record.key, &record.send)
    }

    fn fifo(&self) -> &keyed::Fifo<stream::Key, store::Send> {
        self.fifo
    }

    fn link(
        live: depot::raw::Proof<(&Self, links::Index), Initialized<store::Send>>,
    ) -> &keyed::Link<stream::Key, store::Send> {
        let live = unsafe { live.map(|(owner, index)| (&owner.entry, index)) };
        &slab::Occupied::project(live).1.send
    }
}
