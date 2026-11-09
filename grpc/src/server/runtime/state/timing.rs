use dope::core::driver::schedule::clock;
use o3::{
    buffer::{resident, storage},
    depot::{
        capacity::Count,
        heap::{self, keyed},
    },
};
use sark_core::manifold::{schedule::Quiescence, server};
use sark_h2::stream::Key;

use crate::server::{
    runtime::state::{self, active},
    service::timers,
};

pub trait Timing<'d>: timers::Policy {
    fn try_new<'r>(cap: Count) -> Result<Self::State<'r, 'd>, heap::Error>;

    fn is_quiescent<T, S>(state: &state::Store<'_, 'd, T, Self, S>) -> bool
    where
        S: server::Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
    {
        state.calls.is_quiescent()
            && state.egress.is_quiescent()
            && state.active.get().is_quiescent()
            && state.segments.is_idle()
            && state.deadline().is_none()
    }

    fn activity<T, S>(state: &state::Store<'_, 'd, T, Self, S>) -> Quiescence
    where
        S: server::Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>;

    fn timers<'a, 'r>(
        times: &'a mut <Self as timers::Policy>::State<'r, 'd>,
    ) -> Option<&'a mut timers::Book<'r, 'd>>;

    fn deadline(times: &Self::State<'_, 'd>) -> Option<clock::Deadline<'d>>;

    fn cancel<'r>(times: &mut <Self as timers::Policy>::State<'r, 'd>, entry: active::Entry<'r>) {
        entry.cancel();
        if let Some(times) = Self::timers(times) {
            times.cancel(entry);
        }
    }
}

impl<'d> Timing<'d> for timers::Off {
    fn try_new<'r>(_: Count) -> Result<Self::State<'r, 'd>, heap::Error> {
        Ok(())
    }

    fn activity<T, S>(state: &state::Store<'_, 'd, T, Self, S>) -> Quiescence
    where
        S: server::Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
    {
        if Self::is_quiescent(state) {
            Quiescence::Quiet
        } else {
            Quiescence::Busy
        }
    }

    fn timers<'a, 'r>(
        _: &'a mut <Self as timers::Policy>::State<'r, 'd>,
    ) -> Option<&'a mut timers::Book<'r, 'd>> {
        None
    }

    fn deadline(_: &Self::State<'_, 'd>) -> Option<clock::Deadline<'d>> {
        None
    }
}

impl<'d> Timing<'d> for timers::On {
    fn try_new<'r>(cap: Count) -> Result<Self::State<'r, 'd>, heap::Error> {
        Ok(timers::Book {
            heap: keyed::Min::try_new(cap)?,
        })
    }

    fn activity<T, S>(state: &state::Store<'_, 'd, T, Self, S>) -> Quiescence
    where
        S: server::Connection<Key = Key, Owner = resident::Shared<'d>, Output = storage::Shared>,
    {
        if Self::is_quiescent(state) && state.output_deadlines.is_empty() {
            Quiescence::Quiet
        } else {
            Quiescence::Busy
        }
    }

    fn timers<'a, 'r>(
        times: &'a mut <Self as timers::Policy>::State<'r, 'd>,
    ) -> Option<&'a mut timers::Book<'r, 'd>> {
        Some(times)
    }

    fn deadline(times: &Self::State<'_, 'd>) -> Option<clock::Deadline<'d>> {
        times.heap.peek().map(|item| item.1.at)
    }
}
