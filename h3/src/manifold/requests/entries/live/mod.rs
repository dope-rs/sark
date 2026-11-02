use sark_core::{
    body::{self, credit, flow},
    decode::request,
    duplex::Session,
    manifold::stream::Boundary,
};

mod raw;

use crate::manifold::{
    Phase,
    requests::{
        self,
        entries::{self, Destination, State},
        task,
    },
};

pub(in crate::manifold) enum Failure<E> {
    Step(flow::Error<E>),
    StepRelease {
        error: flow::Error<E>,
        release: body::Error,
    },
    Admit(body::Error),
    AdmitRelease {
        error: body::Error,
        release: credit::Error,
    },
}

impl<E> Failure<E> {
    pub(super) fn admit<F: credit::Evidence, O: credit::Origin>(
        ledger: &mut credit::raw::Ledger<'_, O>,
        mut proof: F,
        release: credit::Release<O>,
        error: body::Error,
    ) -> Self {
        match release.try_discard(ledger, &mut proof) {
            credit::Discard::Applied => Self::Admit(error),
            credit::Discard::Rejected {
                release,
                error: rejected,
            } => {
                release.discard(proof);
                Self::AdmitRelease {
                    error,
                    release: rejected,
                }
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::manifold) enum Completion {
    Open,
    Finished,
}

impl Completion {
    pub(in crate::manifold) const fn from(boundary: Boundary) -> Self {
        match boundary {
            Boundary::More => Self::Open,
            Boundary::End => Self::Finished,
        }
    }
}

pub(in crate::manifold::requests) enum Push<E> {
    Accepted { completion: Completion },
    Queued,
    Inactive { phase: Phase },
    Request(requests::Error),
    Build(E),
}

#[repr(transparent)]
pub(in crate::manifold::requests) struct Live<
    'a,
    'd,
    P: request::Prepared,
    B: body::Build<'d>,
    T,
    D: Session<'d>,
> {
    pub(super) entry: State<'a, 'd, P, B, T, D>,
}

impl<'a, 'd, P: request::Prepared, B: body::Build<'d>, T, D: Session<'d>> Live<'a, 'd, P, B, T, D> {
    pub(in crate::manifold::requests) fn output(self) -> entries::Output<'a, 'd, P, B, T, D> {
        entries::Output { entry: self.entry }
    }

    pub(in crate::manifold::requests) fn id(&self) -> requests::Id {
        let slot = self.entry.get();
        requests::Id::from_entry(slot.identity, self.entry.key())
    }

    pub(in crate::manifold::requests) fn phase(&self) -> Phase {
        self.entry.get().entry.phase()
    }

    pub(in crate::manifold::requests) fn dispatch<E>(
        self,
        route: impl FnOnce(requests::Id, task::Input<'d, P, B::Output>) -> Result<T, E>,
    ) -> Result<task::Dispatch<E>, Phase> {
        let id = self.id();
        let finished = self.entry.get().finished;
        self.entry.run(|entry| match entry {
            entries::Entry::Deferred(input) => match route(id, input) {
                Ok(task) => Destination::Idle(
                    entries::Entry::Active(task),
                    Ok(task::Dispatch::Active { key: id }),
                ),
                Err(error) => {
                    Destination::stop(finished, Ok(task::Dispatch::Rejected { key: id, error }))
                }
            },
            entry @ (entries::Entry::Pending(_)
            | entries::Entry::Building(_)
            | entries::Entry::Responding(_)
            | entries::Entry::Duplex(_)) => Destination::Stay(entry, Ok(task::Dispatch::Idle)),
            entry => {
                let phase = entry.phase();
                Destination::Stay(entry, Err(phase))
            }
        })
    }

    pub(in crate::manifold::requests) fn close_task(self) -> task::Close {
        use task::Close;

        let finished = self.entry.get().finished;
        let method = self.entry.get().method;
        self.entry.run(|entry| match entry {
            entries::Entry::Active(_) => Destination::stop(finished, Close::Closed(method)),
            entry => Destination::Stay(entry, Close::Missing),
        })
    }

    pub(in crate::manifold::requests) fn task(self) -> Option<T> {
        self.entry.run(|entry| match entry {
            entries::Entry::Active(task) => Destination::Remove(Some(task)),
            entry => Destination::Stay(entry, None),
        })
    }
}
