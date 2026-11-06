use layout::{Count, Layout, Node, Queue};
use ledger::frontier;
use o3::{
    alloc,
    alloc::tail,
    depot::{self, capacity, index::Initialized, map::compact},
};

use crate::{qpack, qpack::ledger};

pub(super) mod layout;
mod row;
mod table;

pub(in crate::qpack::ledger) use row::Row;
pub(in crate::qpack::ledger) use table::Table;

pub(super) const NONE: u32 = u32::MAX;
// The u32-bounded allocation contains multiple planes per section, so both
// terminal links lie above every admitted section index.
pub(super) const UNPUBLISHED: u32 = NONE - 1;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Section(pub(in crate::qpack::ledger::store) capacity::Count);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Position(capacity::Count);

pub(super) struct Free {
    id: u64,
    section: Section,
    stream: Stream,
}

pub(super) struct Allocated {
    section: Section,
}

pub(super) struct Insert<'a> {
    pub(super) owner: &'a mut Store,
    pub(super) section: Section,
}

pub(super) struct Resident<'a> {
    pub(super) owner: &'a mut Store,
    pub(super) section: Section,
    pub(super) position: Position,
}

pub(crate) struct Occupied<'a> {
    pub(super) owner: &'a mut Store,
    pub(super) section: Section,
}

pub(super) struct Retired<'a> {
    pub(super) owner: &'a mut Store,
    pub(super) section: Section,
}

impl Section {
    pub(super) const fn raw(self) -> u32 {
        self.0.raw()
    }
}

impl Position {
    const fn from_raw(raw: u32) -> Self {
        Self(capacity::Count::new(raw))
    }
    pub(super) const fn raw(self) -> u32 {
        self.0.raw()
    }
}

impl<'a> Occupied<'a> {
    pub(super) fn new(
        proof: depot::raw::Proof<(&'a mut Store, u32), Initialized<Section>>,
    ) -> Self {
        let (owner, section) = proof.into_inner();
        Self {
            owner,
            section: Section(capacity::Count::new(section)),
        }
    }

    pub(super) fn row(&self) -> Row<&Store> {
        // SAFETY: this occupied projection retains its live section and original owner.
        Row::new(unsafe { depot::raw::Proof::new((&*self.owner, self.section.raw())) })
    }

    pub(super) fn row_mut(&mut self) -> Row<&mut Store> {
        // SAFETY: the exclusive occupied projection preserves the same physical row.
        Row::new(unsafe { depot::raw::Proof::new((&mut *self.owner, self.section.raw())) })
    }

    pub(super) fn counts(&self) -> Counts {
        Counts {
            required: self.row().count(Count::Required),
            minimum: self.row().count(Count::Minimum),
        }
    }

    pub(crate) fn is_ready(&self, published: u64) -> bool {
        self.counts().required() <= published
    }

    /// Consumes the native response's live unpublished head after queue admission.
    pub(crate) fn publish_initial(mut self) {
        self.row_mut().node(Node::Next, NONE);
    }

    pub(super) fn resident(self) -> Resident<'a> {
        let position = Position::from_raw(self.row().node(Node::HeapPosition));
        Resident {
            owner: self.owner,
            section: self.section,
            position,
        }
    }
}

impl Retired<'_> {
    pub(super) fn release(self) {
        let free = self.owner.header().section_free;
        // SAFETY: native heap removal retains this initialized section and its
        // original owner until retirement writes the free-list link.
        let mut row =
            Row::new(unsafe { depot::raw::Proof::new((&mut *self.owner, self.section.raw())) });
        row.count(Count::Required, 0);
        row.count(Count::Minimum, 0);
        row.node(Node::Owner, NONE);
        row.node(Node::HeapPosition, NONE);
        row.node(Node::BlockPrevious, NONE);
        row.node(Node::BlockNext, NONE);
        row.node(Node::Next, free);
        self.owner.header_mut().section_free = self.section.raw();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Stream(pub(in crate::qpack::ledger::store) capacity::Count);

impl Stream {
    pub(super) const fn from_index(index: u32) -> Self {
        Self(capacity::Count::new(index))
    }
    pub(super) const fn raw(self) -> u32 {
        self.0.raw()
    }
}

#[derive(Clone, Copy)]
pub(super) struct Counts {
    required: u64,
    minimum: u64,
}

impl Counts {
    pub(super) const fn new(required: qpack::Reference, minimum: qpack::Reference) -> Self {
        Self {
            required: required.required().get(),
            minimum: minimum.absolute(),
        }
    }

    pub(super) const fn required(self) -> u64 {
        self.required
    }

    pub(super) const fn minimum(self) -> u64 {
        self.minimum
    }
}

pub(super) struct State {
    layout: Layout,
    pub(super) section_free: u32,
    pub(super) stream_free: u32,
    pub(super) heap_len: u32,
    pub(super) blocked_streams: u32,
    pub(super) blocked_heads: frontier::Heads,
}

#[derive(Clone)]
pub(crate) struct Plan {
    layout: Layout,
    allocation: compact::raw::Plan<State>,
}

pub(super) struct Store {
    pub(in crate::qpack::ledger) bytes: tail::Block<State, u8>,
}

impl Plan {
    pub(crate) fn new(capacity: usize) -> Result<Self, super::Error> {
        let capacity = capacity::Count::try_from(capacity)?;
        let layout = Layout::new(capacity)?;
        let memory = compact::raw::Plan::new(layout.index())?;
        Ok(Self {
            layout,
            allocation: memory,
        })
    }
}

impl Store {
    pub(super) fn try_new(plan: Plan) -> Result<Self, alloc::Error> {
        let free = if plan.layout.capacity() == 0 { NONE } else { 0 };
        let state = State {
            layout: plan.layout,
            section_free: free,
            stream_free: free,
            heap_len: 0,
            blocked_streams: 0,
            blocked_heads: frontier::Heads::new(NONE),
        };
        let bytes = plan.allocation.try_allocate(state)?;
        let mut store = Self { bytes };
        store.initialize();
        Ok(store)
    }

    pub(super) fn header(&self) -> &State {
        tail::View::new(&self.bytes).header()
    }
    pub(super) fn header_mut(&mut self) -> &mut State {
        self.bytes.header_mut()
    }
    fn layout(&self) -> Layout {
        self.header().layout
    }
    pub(super) fn capacity_raw(&self) -> u32 {
        self.layout().capacity_raw()
    }
    /// Admitted sections consume heap positions; initialization and retirement
    /// preserve the corresponding section and stream free lists.
    fn vacant(&self, id: u64) -> Option<Free> {
        let position = self.header().heap_len;
        if position >= self.capacity_raw() {
            return None;
        }
        Some(Free {
            id,
            section: Section(capacity::Count::new(self.header().section_free)),
            stream: Stream::from_index(self.header().stream_free),
        })
    }

    fn acquire(
        &mut self,
        free: &Free,
        counts: Counts,
        stream: Stream,
        published: bool,
    ) -> Allocated {
        let section = free.section;
        // SAFETY: only Table<Free> calls this leaf, while retaining the exact
        // admission owner; its vacant section has not been published or reused.
        let mut row = Row::new(unsafe { depot::raw::Proof::new((&mut *self, section.raw())) });
        let next = row.view().node(Node::Next);
        row.node(Node::Next, if published { NONE } else { UNPUBLISHED });
        row.node(Node::Owner, stream.raw());
        row.node(Node::HeapPosition, NONE);
        row.node(Node::BlockPrevious, NONE);
        row.node(Node::BlockNext, NONE);
        row.count(Count::Required, counts.required());
        row.count(Count::Minimum, counts.minimum());
        self.header_mut().section_free = next;
        Allocated { section }
    }

    pub(super) fn floor(&self) -> Option<u64> {
        if self.header().heap_len == 0 {
            return None;
        }
        // SAFETY: the nonempty heap establishes position zero's initialized
        // membership in this owner; heap_read preserves its live section identity.
        let section = Self::heap_read(unsafe { depot::raw::Proof::new((self, 0)) });
        Some(Self::heap_counts(unsafe { depot::raw::Proof::new((self, section)) }).minimum())
    }

    pub(super) fn heap_read(
        proof: depot::raw::Proof<(&Self, usize), Initialized<Position>>,
    ) -> Section {
        let (owner, position) = proof.into_inner();
        // SAFETY: the native heap's initialized position implies physical range;
        // its stored identity remains a live section in the same retained owner.
        let row = Row::new(unsafe { depot::raw::Proof::new((owner, position as u32)) });
        Section(capacity::Count::new(row.heap()))
    }

    pub(super) fn heap_clear<const REMOVE: bool>(
        proof: depot::raw::Proof<(&mut Self, usize), Initialized<Position>>,
    ) -> Section {
        let (owner, position) = proof.into_inner();
        // SAFETY: the native heap transfers this initialized position and its
        // live identity into a hole or a terminal removal, under one owner loan.
        let mut row = Row::new(unsafe { depot::raw::Proof::new((&mut *owner, position as u32)) });
        let section = Section(capacity::Count::new(row.view().heap()));
        row.heap(NONE);
        if REMOVE {
            let mut row = Row::new(unsafe { depot::raw::Proof::new((owner, section.raw())) });
            row.node(Node::HeapPosition, NONE);
        }
        section
    }

    /// The proof retains a physical heap hole and a live section belonging to
    /// the same Store, transferred together by native heap Step or Finish.
    pub(super) fn heap_write(
        proof: depot::raw::Proof<(&mut Self, usize, Section), capacity::Physical>,
    ) {
        let (owner, position, section) = proof.into_inner();
        // SAFETY: both projections come from this one heap transition's owner,
        // initialized section and physical destination hole.
        Row::new(unsafe { depot::raw::Proof::new((&mut *owner, position as u32)) })
            .heap(section.raw());
        Row::new(unsafe { depot::raw::Proof::new((owner, section.raw())) })
            .node(Node::HeapPosition, position as u32);
    }

    pub(super) fn heap_counts(
        proof: depot::raw::Proof<(&Self, Section), Initialized<Section>>,
    ) -> Counts {
        let (owner, section) = proof.into_inner();
        // SAFETY: the heap's initialized or held member remains live and bounded.
        let row = Row::new(unsafe { depot::raw::Proof::new((owner, section.raw())) });
        Counts {
            required: row.count(Count::Required),
            minimum: row.count(Count::Minimum),
        }
    }

    fn initialize(&mut self) {
        let capacity = self.capacity_raw();
        for index in 0..capacity {
            let next = if u64::from(index) + 1 < u64::from(capacity) {
                index + 1
            } else {
                NONE
            };
            // SAFETY: iteration proves this row belongs to the exact allocated
            // plane extent, before any free list or occupied index is exposed.
            let mut row = Row::new(unsafe { depot::raw::Proof::new((&mut *self, index)) });
            row.node(Node::Next, next);
            row.node(Node::Owner, NONE);
            row.node(Node::HeapPosition, NONE);
            row.heap(NONE);
            row.node(Node::BlockPrevious, NONE);
            row.node(Node::BlockNext, NONE);
            row.queue(Queue::Head, next);
            row.queue(Queue::Tail, NONE);
        }
    }
}

const _: () = assert!(core::mem::size_of::<Store>() == core::mem::size_of::<usize>());
const _: () = assert!(core::mem::size_of::<Option<Store>>() == core::mem::size_of::<usize>());
