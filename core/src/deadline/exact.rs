use o3::depot::{
    capacity,
    heap::{self, keyed},
};

use crate::{deadline, manifold};

/// Specialized one-action book with the minimum resident footprint.
pub struct Exact<K: manifold::raw::Key, P: Copy + Ord> {
    heap: keyed::Min<K, deadline::Scheduled<P>>,
}

impl<K: manifold::raw::Key, P: Copy + Ord> Exact<K, P> {
    pub fn try_new(capacity: capacity::Count) -> Result<Self, heap::Error> {
        Ok(Self {
            heap: keyed::Min::try_new(capacity)?,
        })
    }
}

impl<K: manifold::raw::Key, P: Copy + Ord> deadline::Book<K, P> for Exact<K, P> {
    fn schedule(&mut self, key: K, item: deadline::Scheduled<P>) -> Result<(), deadline::Error<K>> {
        match self.heap.entry(key) {
            Err(error) => {
                return Err(deadline::Error::OutOfRange {
                    key: error.key(),
                    capacity: error.capacity(),
                });
            }
            Ok(keyed::Entry::Vacant(entry)) => entry.insert(item),
            Ok(keyed::Entry::Occupied(entry)) if *entry.priority() == item => {}
            Ok(keyed::Entry::Occupied(entry)) => entry.set(item),
            Ok(keyed::Entry::Conflict(error)) => {
                return Err(deadline::Error::Conflict {
                    requested: error.requested(),
                    current: error.current(),
                });
            }
        }
        Ok(())
    }

    fn cancel(&mut self, key: K) -> deadline::Mutation {
        let heap::Probe::Occupied(entry) = self.heap.probe(key) else {
            return deadline::Mutation::Unchanged;
        };
        entry.retire_min().into()
    }

    fn cancel_one(&mut self, key: K, action: deadline::Action) -> deadline::Mutation {
        let heap::Probe::Occupied(entry) = self.heap.probe(key) else {
            return deadline::Mutation::Unchanged;
        };
        if entry.priority().action != action {
            return deadline::Mutation::Unchanged;
        }
        entry.retire_min().into()
    }

    fn peek(&self) -> Option<(K, deadline::Scheduled<P>)> {
        self.heap.peek().map(|(key, item)| (key, *item))
    }

    fn poll_due(&mut self, now: P) -> deadline::Poll<deadline::Expired<K, P>> {
        let Some(entry) = self.heap.first_entry() else {
            return deadline::Poll::Idle;
        };
        if entry.priority().at > now {
            return deadline::Poll::Idle;
        }
        deadline::Poll::Expired(deadline::Expired {
            key: entry.key(),
            scheduled: entry.remove(),
        })
    }

    fn len(&self) -> usize {
        self.heap.len()
    }
}
