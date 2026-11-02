use sark_core::{body, decode::request, duplex::Session};
use sark_http::payload::response::{Method, status::Code};

use crate::manifold::requests::{
    self,
    duplex::{self, work},
    entries::{self, Destination, State, status},
};

#[repr(transparent)]
pub(in crate::manifold::requests) struct Output<
    'a,
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: Session<'d>,
> {
    pub(super) entry: State<'a, 'd, P, B, T, D>,
}

impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: Session<'d>>
    Output<'a, 'd, P, B, T, D>
{
    pub(in crate::manifold::requests) fn with_duplex<R>(
        self,
        run: impl for<'turn> FnOnce(work::Work<'turn, 'd, D>) -> duplex::Next<R>,
    ) -> Option<R> {
        let id = requests::Id::from_entry(self.entry.get().identity, self.entry.key());
        self.entry.run(|entry| match entry {
            entries::Entry::Duplex(mut state) => {
                state.queue = status::Queue::Idle;
                match run(work::Work::new(id, &mut state.input, &mut state.response)) {
                    duplex::Next::Idle(out) => {
                        Destination::Idle(entries::Entry::Duplex(state), Some(out))
                    }
                    duplex::Next::Ready(out) => {
                        state.queue = status::Queue::Ready;
                        Destination::Rotate(entries::Entry::Duplex(state), Some(out))
                    }
                    duplex::Next::Blocked(out) => {
                        state.queue = status::Queue::Blocked;
                        Destination::Blocked(entries::Entry::Duplex(state), Some(out))
                    }
                    duplex::Next::Finished(out) => Destination::Remove(Some(out)),
                }
            }
            entry => Destination::Stay(entry, None),
        })
    }

    pub(in crate::manifold::requests) fn with_status<R>(
        self,
        run: impl FnOnce(requests::Id, Method, Code) -> status::Next<R>,
    ) -> Result<Option<(R, bool)>, requests::Error> {
        let id = requests::Id::from_entry(self.entry.get().identity, self.entry.key());
        let finished = self.entry.get().finished;
        let method = self.entry.get().method;
        self.entry.run(|entry| match entry {
            entries::Entry::Responding(mut state) => match run(id, method, state.code) {
                status::Next::Blocked(out) => {
                    state.queue = status::Queue::Blocked;
                    Destination::Blocked(
                        entries::Entry::Responding(state),
                        Ok(Some((out, finished))),
                    )
                }
                status::Next::Finished(out) => Destination::Remove(Ok(Some((out, finished)))),
            },
            entry @ (entries::Entry::Pending(_)
            | entries::Entry::Building(_)
            | entries::Entry::Deferred(_)
            | entries::Entry::Duplex(_)) => Destination::Stay(entry, Ok(None)),
            entry => Destination::Stay(entry, Err(requests::Error::ReadyStatus)),
        })
    }
}

pub(in crate::manifold::requests) struct Blocked<
    'a,
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: Session<'d>,
> {
    pub(super) member: entries::Member<'a, 'd, P, B, T, D>,
}

impl<'d, P: request::Prepared, B: body::Build<'d>, T, D: Session<'d>> Blocked<'_, 'd, P, B, T, D> {
    pub(in crate::manifold::requests) fn resume(self) -> Result<(), requests::Error> {
        let mut state = State::Blocked(self.member);
        match state.get_mut() {
            entries::Entry::Responding(head) => head.queue = status::Queue::Ready,
            entries::Entry::Duplex(duplex) => duplex.queue = status::Queue::Ready,
            _ => return Err(requests::Error::Resume),
        }
        state.ready();
        Ok(())
    }
}
