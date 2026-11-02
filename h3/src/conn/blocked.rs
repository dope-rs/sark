use dope_quic::wire::stream::Id;
use o3::{
    alloc,
    depot::map::{self, priority},
};
use priority::errors;

use crate::qpack::{self, decoder};

#[derive(Clone, Copy)]
pub(super) struct Plan {
    waits: priority::Plan<Id, qpack::Reference, ()>,
}

impl Plan {
    pub(super) fn new(capacity: usize) -> Result<Self, map::Capacity> {
        Ok(Self {
            waits: priority::Plan::new(capacity)?,
        })
    }

    pub(super) const fn capacity(self) -> usize {
        self.waits.capacity().get()
    }
}

/// Fixed receiver-side ownership for QPACK-blocked streams.
pub(super) struct Blocked {
    waits: priority::Min<Id, qpack::Reference, (), qpack::StreamHash>,
}

#[must_use = "a blocked-stream removal must be committed after feedback is prepared"]
pub(super) struct Removal<'a> {
    entry: priority::Occupied<'a, Id, qpack::Reference, ()>,
}

impl Blocked {
    pub(super) fn try_new(plan: Plan) -> Result<Self, alloc::Error> {
        Ok(Self {
            waits: priority::Min::try_new(plan.waits, qpack::StreamHash)?,
        })
    }

    pub(super) fn contains(&self, id: Id) -> bool {
        self.waits.contains_key(&id)
    }

    pub(super) fn block(&mut self, id: Id, needed: qpack::Reference) -> Result<(), decoder::Error> {
        use priority::Entry;

        match self.waits.try_entry(id) {
            Ok(Entry::Occupied(entry)) if *entry.priority() == needed => Ok(()),
            Ok(Entry::Occupied(entry)) => Err(decoder::Error::BlockedConflict {
                stream_id: id.get(),
                current: *entry.priority(),
                required: needed,
            }),
            Ok(Entry::Vacant(entry)) => {
                entry.insert(needed, ());
                Ok(())
            }
            Err(rejected) => {
                let (id, errors::Full) = rejected.into_parts();
                Err(decoder::Error::BlockedLimit {
                    stream_id: id.get(),
                    required: needed,
                    limit: self.waits.len(),
                })
            }
        }
    }

    pub(super) fn pop_ready(&mut self, count: u64) -> Option<Id> {
        self.waits
            .pop_if(|required| required.is_received(count))
            .map(|entry| entry.0)
    }

    pub(super) fn has_ready(&self, count: u64) -> bool {
        self.waits
            .peek()
            .is_some_and(|entry| entry.1.is_received(count))
    }

    pub(super) fn removal(&mut self, id: Id) -> Option<Removal<'_>> {
        self.waits.occupied(&id).map(|entry| Removal { entry })
    }
}

impl Removal<'_> {
    pub(super) fn commit(self) {
        self.entry.remove();
    }
}
