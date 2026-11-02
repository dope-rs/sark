use dope::core::driver::schedule::clock;
use o3::depot::heap::keyed;
use sark_h2::stream::Key;

use crate::server::{
    runtime::state::active,
    service::timers::{Error, wake::Wake},
};

/// Deferred output retaining the admitted call and readiness owner.
pub struct Book<'r, 'd> {
    pub(in crate::server) heap: keyed::Min<Key, Wake<'r, 'd>>,
}

impl<'r, 'd> Book<'r, 'd> {
    pub(in crate::server) fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub(in crate::server::service::timers) fn defer(
        &mut self,
        entry: active::Entry<'r>,
        at: clock::Deadline<'d>,
    ) -> Result<(), Error> {
        let wake = Wake { at, entry };
        match self.heap.entry(entry.key()).map_err(Error::Bounds)? {
            keyed::Entry::Vacant(slot) => slot.insert(wake),
            keyed::Entry::Occupied(slot) => {
                if slot.priority().at != at {
                    slot.set(wake);
                }
            }
            keyed::Entry::Conflict(error) => return Err(Error::Conflict(error)),
        }
        Ok(())
    }

    pub(in crate::server) fn cancel(&mut self, entry: active::Entry<'r>) {
        self.heap.retire(entry.key());
    }

    pub(in crate::server) fn pop(&mut self, now: clock::Moment<'d>) -> Option<active::Entry<'r>> {
        let first = self.heap.first_entry()?;
        if first.priority().at > now {
            return None;
        }
        Some(first.remove().entry)
    }
}
