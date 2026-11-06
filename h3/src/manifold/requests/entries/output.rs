use crate::manifold::owners;
use sark_core::manifold::task::app;

use crate::manifold::requests::{
    duplex::{self, work},
    entries::{self, Destination, live::Live},
};

impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> Live<'a, 'd, A, M> {
    pub(in crate::manifold::requests) fn with_duplex<R>(
        self,
        run: impl for<'turn> FnOnce(work::Work<'turn, 'd, A::Duplex>) -> duplex::Next<R>,
    ) -> Option<R> {
        let id = self.id();
        self.entry.run(|entry| match entry {
            entries::Entry::Duplex(mut state) => {
                match run(work::Work::new(id, &mut state.input, &mut state.response)) {
                    duplex::Next::Idle(out) => {
                        Destination::Idle(entries::Entry::Duplex(state), Some(out))
                    }
                    duplex::Next::Ready(out) => {
                        Destination::Rotate(entries::Entry::Duplex(state), Some(out))
                    }
                    duplex::Next::Blocked(out) => {
                        Destination::Blocked(entries::Entry::Duplex(state), Some(out))
                    }
                    duplex::Next::Finished(out) => Destination::Remove(Some(out)),
                }
            }
            entry => Destination::Stay(entry, None),
        })
    }
}
