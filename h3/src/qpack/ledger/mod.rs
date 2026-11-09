use o3::{
    alloc,
    depot::{capacity, map, map::compact},
};

mod frontier;
mod raw;
mod store;

pub(in crate::qpack) use frontier::Advance;
use store::{Counts, Store, layout::Node};
pub(crate) use store::{Occupied, Plan};

use crate::qpack::{self, stream};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    Capacity(capacity::Error),
    Plane {
        offset: usize,
        capacity: usize,
        width: usize,
    },
    Index(compact::Error),
    Length {
        len: usize,
    },
    Overflow(alloc::Overflow),
}

impl From<capacity::Error> for Error {
    fn from(error: capacity::Error) -> Self {
        Self::Capacity(error)
    }
}

impl From<alloc::Overflow> for Error {
    fn from(error: alloc::Overflow) -> Self {
        Self::Overflow(error)
    }
}

/// Move-only proof that one section and its stream queue can be committed to
/// this ledger generation without another capacity check or index lookup.
pub(super) struct Admission<'a> {
    slot: compact::Entry<store::Table<'a, store::Free>>,
    received: u64,
    selection: Selection,
}

#[derive(Clone, Copy)]
pub(super) struct Candidate(qpack::Reference);

pub(super) struct Tracked<'a> {
    slot: compact::Entry<store::Table<'a, store::Free>>,
    counts: Counts,
    blocking: u32,
    active: Option<u32>,
    received: u64,
}

/// Fixed-capacity ownership ledger for committed dynamic field references.
/// Every slab, index, link, and ordering plane occupies one exact allocation.
pub(super) struct Ledger {
    store: Store,
}

#[derive(Clone, Copy)]
struct Range {
    required: qpack::Reference,
    minimum: qpack::Reference,
}

#[derive(Clone, Copy)]
enum Selection {
    Received,
    ReceivedRange(Range),
    Blocking,
    BlockingRange(Range),
    Blocked(Range),
}

impl Range {
    const fn new(index: qpack::Reference) -> Self {
        Self {
            required: index,
            minimum: index,
        }
    }

    fn extend(range: Self, index: qpack::Reference) -> Self {
        Self {
            required: range.required.max(index),
            minimum: range.minimum.min(index),
        }
    }
}

impl<'a> Admission<'a> {
    /// Publishes only the winner found while this admission stays unchanged.
    pub(super) fn select(
        &mut self,
        find: impl FnOnce(&Self) -> Option<Candidate>,
    ) -> Option<qpack::Reference> {
        let Candidate(index) = find(self)?;
        self.selection = match self.selection {
            Selection::Received => Selection::ReceivedRange(Range::new(index)),
            Selection::ReceivedRange(range) => {
                Selection::ReceivedRange(Range::extend(range, index))
            }
            Selection::Blocking => {
                let range = Range::new(index);
                if index.is_received(self.received) {
                    Selection::BlockingRange(range)
                } else {
                    Selection::Blocked(range)
                }
            }
            Selection::BlockingRange(range) => {
                let range = Range::extend(range, index);
                if index.is_received(self.received) {
                    Selection::BlockingRange(range)
                } else {
                    Selection::Blocked(range)
                }
            }
            Selection::Blocked(range) => Selection::Blocked(Range::extend(range, index)),
        };
        Some(index)
    }

    pub(super) fn accept(&self, index: qpack::Reference) -> Option<Candidate> {
        if matches!(
            self.selection,
            Selection::Received | Selection::ReceivedRange(_)
        ) && !index.is_received(self.received)
        {
            return None;
        }
        Some(Candidate(index))
    }

    pub(super) fn required(&self) -> u64 {
        match self.selection {
            Selection::ReceivedRange(range)
            | Selection::BlockingRange(range)
            | Selection::Blocked(range) => range.required.required().get(),
            Selection::Received | Selection::Blocking => 0,
        }
    }

    pub(super) fn track(self) -> Option<Tracked<'a>> {
        let (prior, _) = store::Table::state(&self.slot);
        let (range, blocking, active) = match self.selection {
            Selection::ReceivedRange(range) | Selection::BlockingRange(range) => {
                (range, prior, None)
            }
            Selection::Blocked(range) => {
                let (blocking, active) = store::Table::blocked(&self.slot);
                (range, blocking, active)
            }
            Selection::Received | Selection::Blocking => return None,
        };
        Some(Tracked {
            slot: self.slot,
            counts: Counts::new(range.required, range.minimum),
            blocking,
            active,
            received: self.received,
        })
    }
}

impl Tracked<'_> {
    /// `false` is reserved for the native response's sole initial section.
    pub(super) fn apply(self, published: bool) {
        let Self {
            slot,
            counts,
            blocking,
            active,
            received,
        } = self;
        store::Table::apply(slot, counts, blocking, received, active, published);
    }
}

impl Ledger {
    pub(super) fn try_new(plan: Plan) -> Result<Self, alloc::Error> {
        Ok(Self {
            store: Store::try_new(plan)?,
        })
    }

    pub(super) fn admit(&mut self, id: u64, limit: u64, received: u64) -> Option<Admission<'_>> {
        let ledger = self;
        use compact::Access as _;

        let slot = store::Table::vacant(ledger, id)?
            .entry(map::Hasher::hash(&qpack::StreamHash, &id), &id);
        let (prior, blocked) = store::Table::state(&slot);
        let mode = if prior != 0 || u64::from(blocked) < limit {
            Selection::Blocking
        } else {
            Selection::Received
        };
        Some(Admission {
            slot,
            received,
            selection: mode,
        })
    }

    pub(super) fn floor(&self) -> Option<u64> {
        self.store.floor()
    }

    fn find(&mut self, id: u64) -> Option<compact::Found<store::Table<'_, ()>>> {
        use compact::Access as _;

        store::Table {
            ledger: self,
            input: (),
        }
        .find(map::Hasher::hash(&qpack::StreamHash, &id), &id)
    }

    pub(super) fn initial(&mut self, id: u64) -> Option<Occupied<'_>> {
        let mut occupied = self.find(id)?;
        let section = store::Table::head(&mut occupied).section;
        // Found returns the same exclusive ledger without mutating its live head.
        let table = occupied.into_owner();
        Some(Occupied {
            owner: &mut table.ledger.store,
            section,
        })
    }

    /// Discards the consumed native head's sole unpublished section;
    /// static sections and prior peer cancellation have no remaining row.
    pub(super) fn discard_initial(&mut self, id: u64) {
        if let Some(mut occupied) = self.find(id) {
            store::Table::release_head(&mut occupied);
            occupied.remove();
        }
    }

    /// Releases the oldest section on `stream_id` and returns the exact insert
    /// count proven received by that acknowledgment.
    pub(super) fn acknowledge(&mut self, id: u64, received: u64) -> Result<u64, stream::Error> {
        let mut occupied = self
            .find(id)
            .ok_or(stream::Error::UnknownSection { stream_id: id })?;
        if store::Table::head(&mut occupied).row().node(Node::Next) == store::UNPUBLISHED {
            return Err(stream::Error::UnknownSection { stream_id: id });
        }
        let (required, has_next) = store::Table::release_head(&mut occupied);
        let advance = frontier::Advance::at_least(received, required);
        if has_next {
            store::Table::advance(&mut occupied, advance);
        } else {
            occupied.remove().advance(advance);
        }
        Ok(required)
    }

    pub(super) fn cancel(&mut self, id: u64) {
        let Some(mut occupied) = self.find(id) else {
            return;
        };
        loop {
            let (_, has_next) = store::Table::release_head(&mut occupied);
            if !has_next {
                occupied.remove();
                return;
            }
        }
    }

    /// Advances the frontier by one step the decoder already proved monotone.
    pub(super) fn advance(&mut self, advance: Advance) {
        frontier::Frontier::new(&mut self.store).advance(advance);
    }
}

const _: () = {
    assert!(core::mem::size_of::<Admission<'static>>() == 64);
    assert!(core::mem::size_of::<Tracked<'static>>() == 72);
    assert!(core::mem::size_of::<Option<Tracked<'static>>>() == 72);
    assert!(core::mem::size_of::<Selection>() <= 3 * core::mem::size_of::<u64>());
    assert!(
        core::mem::size_of::<Option<Admission<'static>>>()
            == core::mem::size_of::<Admission<'static>>()
    );
    assert!(core::mem::size_of::<Ledger>() == core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Option<Ledger>>() == core::mem::size_of::<usize>());
};
