use ledger::{frontier, store};
use o3::{
    alloc::tail,
    depot::{self, capacity, map, map::compact},
};
use store::{
    Row,
    layout::{Node, Queue},
};

use crate::{qpack, qpack::ledger};

pub(in crate::qpack::ledger) struct Table<'a, I> {
    pub(in crate::qpack::ledger) ledger: &'a mut ledger::Ledger,
    pub(in crate::qpack::ledger) input: I,
}

impl<I> Table<'_, I> {
    fn row<'s>(value: &'s compact::Value<'_, Self>) -> Row<&'s store::Store> {
        // SAFETY: Value retains this owner's live stream row; every plane has
        // the exact capacity used by the native directory.
        Row::new(unsafe {
            depot::raw::Proof::new((&value.owner().ledger.store, value.index() as u32))
        })
    }
}

impl<'a> Table<'a, store::Free> {
    pub(in crate::qpack::ledger) fn vacant(
        ledger: &'a mut ledger::Ledger,
        id: u64,
    ) -> Option<Self> {
        let input = ledger.store.vacant(id)?;
        Some(Self { ledger, input })
    }

    pub(in crate::qpack::ledger) fn state(slot: &compact::Entry<Self>) -> (u32, u32) {
        match slot {
            compact::Entry::Occupied(occupied) => {
                let value = occupied.value();
                (
                    Self::row(&value).queue(Queue::Blocking),
                    value.owner().ledger.store.header().blocked_streams,
                )
            }
            compact::Entry::Vacant(owner) => (0, owner.ledger.store.header().blocked_streams),
        }
    }

    /// Free retains a vacancy; both counters are at most heap_len, strictly
    /// below the u32-bounded capacity in the original owner.
    pub(in crate::qpack::ledger) fn blocked(slot: &compact::Entry<Self>) -> (u32, Option<u32>) {
        let (blocking, blocked) = Self::state(slot);
        let next = (u64::from(blocking) + 1) as u32;
        let active = (blocking == 0).then(|| (u64::from(blocked) + 1) as u32);
        (next, active)
    }

    pub(in crate::qpack::ledger) fn apply(
        slot: compact::Entry<Self>,
        counts: store::Counts,
        blocking: u32,
        received: u64,
        active: Option<u32>,
    ) {
        let table = match slot {
            compact::Entry::Occupied(occupied) => Self::append(occupied, counts, blocking),
            compact::Entry::Vacant(owner) => owner.insert(counts, blocking),
        };
        table.commit(received, active);
    }

    fn append(
        mut occupied: compact::Found<Self>,
        counts: store::Counts,
        blocking: u32,
    ) -> Table<'a, store::Allocated> {
        let value = occupied.value();
        let stream = value.index() as u32;
        let tail = Self::row(&value).queue(Queue::Tail);
        let owner = unsafe { compact::raw::Write::owner_mut(&mut occupied) };
        let store = &mut owner.ledger.store;
        let input = store.acquire(&owner.input, counts, store::Stream::from_index(stream));
        // SAFETY: the occupied stream retains its live tail and exact owner;
        // admission has initialized the new section before this link publishes it.
        Row::new(unsafe { depot::raw::Proof::new((&mut *store, tail)) })
            .node(Node::Next, input.section.raw());
        let mut row = Row::new(unsafe { depot::raw::Proof::new((store, stream)) });
        row.queue(Queue::Tail, input.section.raw());
        row.queue(Queue::Blocking, blocking);
        let owner = occupied.into_owner();
        Table {
            ledger: owner.ledger,
            input,
        }
    }

    fn insert(self, counts: store::Counts, blocking: u32) -> Table<'a, store::Allocated> {
        use compact::Insert as _;
        let store = &mut self.ledger.store;
        let stream = self.input.stream;
        let input = store.acquire(&self.input, counts, stream);
        // SAFETY: this Table retains its admitted free stream row and owner.
        let mut row = Row::new(unsafe { depot::raw::Proof::new((&mut *store, stream.raw())) });
        let next = row.view().queue(Queue::Head);
        row.stream_id(self.input.id);
        row.queue(Queue::Head, input.section.raw());
        row.queue(Queue::Tail, input.section.raw());
        row.queue(Queue::Blocking, blocking);
        store.header_mut().stream_free = next;
        Table {
            ledger: self.ledger,
            input: (stream, input),
        }
        .insert()
    }
}

impl Table<'_, store::Allocated> {
    fn commit(self, received: u64, active: Option<u32>) {
        let occupied = ledger::raw::Heap::push(store::Insert {
            owner: &mut self.ledger.store,
            section: self.input.section,
        });
        frontier::Frontier::insert(occupied, received);
        if let Some(active) = active {
            self.ledger.store.header_mut().blocked_streams = active;
        }
    }
}

impl Table<'_, ()> {
    /// Retire one head under the original Found loan. No section identity or
    /// owner-free resident escapes between queue, frontier, heap and free list.
    pub(in crate::qpack::ledger) fn release_head(
        occupied: &mut compact::Found<Self>,
    ) -> (u64, bool) {
        let value = occupied.value();
        let stream = value.index() as u32;
        let section = store::Section(capacity::Count::new(Self::row(&value).queue(Queue::Head)));
        let owner = unsafe { compact::raw::Write::owner_mut(occupied) };
        let store = &mut owner.ledger.store;
        let head = store::Occupied {
            owner: store,
            section,
        };
        let required = head.counts().required();
        let next = head.row().node(Node::Next);
        let head = frontier::Frontier::remove(head);
        ledger::raw::Heap::remove(head.resident()).release();
        if next != store::NONE {
            // SAFETY: Found still retains the same stream row. The successor
            // came from its live queue before the head was retired.
            Row::new(unsafe { depot::raw::Proof::new((&mut *store, stream)) })
                .queue(Queue::Head, next);
        }
        (required, next != store::NONE)
    }

    pub(in crate::qpack::ledger) fn advance(
        occupied: &mut compact::Found<Self>,
        advance: frontier::Advance,
    ) {
        // SAFETY: frontier changes only section links and counts, preserving the
        // indexed stream's record identity and hash under this Found loan.
        unsafe { compact::raw::Write::owner_mut(occupied) }
            .ledger
            .advance(advance);
    }
}

unsafe impl<I> compact::raw::Read for Table<'_, I> {
    fn layout(&self) -> compact::Layout {
        self.ledger.store.layout().index()
    }
    fn bytes(&self) -> &[u8] {
        tail::View::new(&self.ledger.store.bytes).as_slice()
    }
}

unsafe impl<I> compact::raw::Source for Table<'_, I> {
    fn hash(value: compact::Value<'_, Self>) -> u64 {
        map::Hasher::hash(
            &qpack::StreamHash,
            &<Self as compact::raw::Query>::get(value),
        )
    }
}

unsafe impl<I> compact::raw::Query for Table<'_, I> {
    type Item = u64;
    fn get(value: compact::Value<'_, Self>) -> Self::Item {
        let owner = &value.owner().ledger.store;
        // SAFETY: Value retains this directory's initialized bounded stream row.
        Row::new(unsafe { depot::raw::Proof::new((owner, value.index() as u32)) }).stream_id()
    }
}

unsafe impl<I> compact::raw::Write for Table<'_, I> {
    unsafe fn bytes_mut(&mut self) -> &mut [u8] {
        self.ledger.store.bytes.as_mut_slice()
    }
}

unsafe impl<'a> compact::raw::Remove for Table<'a, ()> {
    type Output = &'a mut ledger::Ledger;
    fn removed(removed: compact::Removed<Self>) -> Self::Output {
        let stream = removed.value().index() as u32;
        let owner = unsafe { compact::raw::Remove::into_owner(removed) };
        let store = &mut owner.ledger.store;
        let free = store.header().stream_free;
        // SAFETY: native removal transfers this exact initialized stream row
        // after directory membership and its last section have been retired.
        let mut row = Row::new(unsafe { depot::raw::Proof::new((&mut *store, stream)) });
        row.stream_id(0);
        row.queue(Queue::Head, free);
        row.queue(Queue::Tail, store::NONE);
        row.queue(Queue::Blocking, 0);
        store.header_mut().stream_free = stream;
        owner.ledger
    }
}

unsafe impl<'a> compact::raw::Insert for Table<'a, (store::Stream, store::Allocated)> {
    type Output = Table<'a, store::Allocated>;
    fn input(&self) -> compact::Value<'_, Self> {
        unsafe { compact::Value::new(depot::raw::Proof::new((self, self.input.0.raw()))) }
    }
    unsafe fn inserted(self) -> Self::Output {
        Table {
            ledger: self.ledger,
            input: self.input.1,
        }
    }
}

const _: () = {
    assert!(size_of::<Table<'static, ()>>() == size_of::<&mut ledger::Ledger>());
    assert!(size_of::<Table<'static, store::Allocated>>() == 16);
};
