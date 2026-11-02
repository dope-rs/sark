use core::mem;

use o3::{base::num::bounded, depot};

use crate::qpack::ledger::store::{
    NONE, Occupied, Row, Store,
    layout::{Node, Queue},
};

#[repr(transparent)]
pub(super) struct Heads([u32; 65]);

#[derive(Clone, Copy)]
#[repr(transparent)]
struct Bucket(bounded::U32<0, { u64::BITS }>);

impl Bucket {
    fn new(frontier: u64, key: u64) -> Self {
        Self::from_raw(u64::BITS - (frontier ^ key).leading_zeros())
    }
    fn from_raw(raw: u32) -> Self {
        Self(bounded::U32::clamp(raw as usize))
    }
    /// Only keys above the frontier are linked, so bucket zero stays empty.
    fn iter(frontier: u64, key: u64) -> impl Iterator<Item = Self> {
        (1..=Self::new(frontier, key).0.get()).map(Self::from_raw)
    }
    const fn index(self) -> usize {
        self.0.into_usize()
    }
    fn link(self) -> u32 {
        NONE - 1 - self.0.get()
    }
}

impl Heads {
    pub(super) const fn new(value: u32) -> Self {
        Self([value; 65])
    }
    fn get(&self, bucket: Bucket) -> u32 {
        self.0[bucket.index()]
    }
    fn get_mut(&mut self, bucket: Bucket) -> &mut u32 {
        &mut self.0[bucket.index()]
    }
}

/// A live frontier member retains its stream, positive counts and actual
/// incoming edge under the original Store loan until detachment commits.
struct Blocked<'a> {
    occupied: Occupied<'a>,
    previous: u32,
    next: u32,
}

/// The same member after its incoming and outgoing edges have been retired.
struct Detached<'a> {
    occupied: Occupied<'a>,
}

/// One received-count advance, monotone because it is built only from its own
/// exact increment step.
#[derive(Clone, Copy)]
pub(in crate::qpack) struct Advance {
    previous: u64,
    received: u64,
}

impl Advance {
    /// Builds the advance one exact increment step proves. The single
    /// `checked_add` bounds the step and establishes that the new count follows
    /// the previous one, so no later stage re-checks that order.
    pub(in crate::qpack) const fn from_step(previous: u64, step: u64) -> Option<Self> {
        match previous.checked_add(step) {
            Some(received) => Some(Self { previous, received }),
            None => None,
        }
    }

    /// The exact received count this advance establishes.
    pub(in crate::qpack) const fn received(self) -> u64 {
        self.received
    }

    /// Advances to at least `floor`, keeping the frontier when it already
    /// reached it. The result is bounded by the larger of the two counts, so no
    /// step here can overflow or fall below the frontier.
    pub(in crate::qpack) const fn at_least(previous: u64, floor: u64) -> Self {
        Self {
            previous,
            received: if floor > previous { floor } else { previous },
        }
    }
}

pub(super) struct Frontier<'a> {
    store: &'a mut Store,
}

impl<'a> Frontier<'a> {
    pub(super) const fn new(store: &'a mut Store) -> Self {
        Self { store }
    }

    pub(super) fn insert(occupied: Occupied<'a>, received: u64) {
        let required = occupied.counts().required();
        if required > received {
            Self::link(occupied, Bucket::new(received, required));
        }
    }

    pub(super) fn remove(occupied: Occupied<'a>) -> Occupied<'a> {
        if occupied.row().node(Node::BlockPrevious) == NONE {
            return occupied;
        }
        // SAFETY: the stored incoming edge is written only by link and moved
        // only with that live membership; NONE is the detached state.
        unsafe { Blocked::linked(occupied) }.detach().unblock()
    }

    /// Drain each affected chain under one owner loan, then relocate or retire
    /// each membership without per-node predecessor splices.
    pub(super) fn advance(self, advance: Advance) {
        let Advance { previous, received } = advance;
        if previous == received {
            return;
        }
        for bucket in Bucket::iter(previous, received) {
            let mut next =
                mem::replace(self.store.header_mut().blocked_heads.get_mut(bucket), NONE);
            while next != NONE {
                // SAFETY: the drained head and its initialized successor chain
                // retain this owner. Each Next edge is consumed once; incoming
                // edges are rebuilt when the corresponding section is relocated.
                let occupied =
                    Occupied::new(unsafe { depot::raw::Proof::new((&mut *self.store, next)) });
                let mut occupied = occupied;
                let required = occupied.counts().required();
                next = occupied.row().node(Node::BlockNext);
                occupied.row_mut().node(Node::BlockPrevious, NONE);
                if required > received {
                    Self::link(occupied, Bucket::new(received, required));
                } else {
                    occupied.row_mut().node(Node::BlockNext, NONE);
                    Detached { occupied }.unblock();
                }
            }
        }
    }

    /// Layout reserves 60 bytes per section with a u32-bounded total extent,
    /// so physical section indices are disjoint from the 65 top bucket tags.
    fn link(mut occupied: Occupied<'_>, bucket: Bucket) {
        let head = occupied.owner.header().blocked_heads.get(bucket);
        let section = occupied.section.raw();
        occupied.row_mut().node(Node::BlockPrevious, bucket.link());
        occupied.row_mut().node(Node::BlockNext, head);
        if head != NONE {
            // SAFETY: the original bucket's live head retains the same owner;
            // publication transfers its incoming edge to this new section.
            Row::new(unsafe { depot::raw::Proof::new((&mut *occupied.owner, head)) })
                .node(Node::BlockPrevious, section);
        }
        *occupied.owner.header_mut().blocked_heads.get_mut(bucket) = section;
    }
}

impl<'a> Blocked<'a> {
    /// # Safety
    /// The occupied section is currently linked in this owner's frontier.
    unsafe fn linked(occupied: Occupied<'a>) -> Self {
        let row = occupied.row();
        let previous = row.node(Node::BlockPrevious);
        let next = row.node(Node::BlockNext);
        Self {
            occupied,
            previous,
            next,
        }
    }

    fn detach(mut self) -> Detached<'a> {
        let owner = &mut *self.occupied.owner;
        if self.previous < owner.capacity_raw() {
            // SAFETY: this member retains its actual predecessor, whose outgoing
            // edge names it. The section/owner pair never leaves this transition.
            Row::new(unsafe { depot::raw::Proof::new((&mut *owner, self.previous)) })
                .node(Node::BlockNext, self.next);
        } else {
            let bucket = Bucket::from_raw(NONE - 1 - self.previous);
            *owner.header_mut().blocked_heads.get_mut(bucket) = self.next;
        }
        if self.next != NONE {
            // SAFETY: the live successor inherits this member's incoming edge.
            Row::new(unsafe { depot::raw::Proof::new((&mut *owner, self.next)) })
                .node(Node::BlockPrevious, self.previous);
        }
        self.occupied.row_mut().node(Node::BlockPrevious, NONE);
        self.occupied.row_mut().node(Node::BlockNext, NONE);
        Detached {
            occupied: self.occupied,
        }
    }
}

impl<'a> Detached<'a> {
    fn unblock(self) -> Occupied<'a> {
        let stream = self.occupied.row().node(Node::Owner);
        // SAFETY: the consumed frontier membership retains this owning stream
        // and contributes one blocked section and one active stream. Detachment
        // changes only links; exclusive ownership prevents intervening updates.
        let mut row =
            Row::new(unsafe { depot::raw::Proof::new((&mut *self.occupied.owner, stream)) });
        let blocking = row.view().queue(Queue::Blocking) - 1;
        row.queue(Queue::Blocking, blocking);
        if blocking == 0 {
            self.occupied.owner.header_mut().blocked_streams -= 1;
        }
        self.occupied
    }
}

const _: () = assert!(core::mem::size_of::<Heads>() == 65 * core::mem::size_of::<u32>());
