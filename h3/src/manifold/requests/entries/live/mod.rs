use crate::manifold::owners;
use sark_core::manifold::task::app;
use std::ops::ControlFlow;

use sark_core::{
    body::{self, credit, flow},
    manifold::stream::Boundary,
};
use sark_http::payload::response::{Method, status::Code};

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

pub(in crate::manifold::requests) enum Push<E> {
    Accepted { completion: Boundary },
    Queued,
    Inactive { phase: Phase },
    Request(requests::Error),
    Build(E),
}

#[repr(transparent)]
pub(in crate::manifold::requests) struct Live<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> {
    pub(super) entry: State<'a, 'd, A, M>,
}

impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> Live<'a, 'd, A, M> {
    pub(in crate::manifold::requests) fn id(&self) -> requests::Id {
        let slot = self.entry.get();
        requests::Id::from_entry(slot.identity, self.entry.key())
    }

    pub(in crate::manifold::requests) fn phase(&self) -> Phase {
        self.entry.get().entry.phase()
    }

    pub(in crate::manifold::requests) fn dispatch<E>(
        self,
        route: impl FnOnce(
            requests::Id,
            task::Input<'d, A::Prepared, <A::Build as body::Build<'d>>::Output>,
        ) -> Result<A::Task, E>,
    ) -> Result<task::Dispatch<E>, Phase> {
        let id = self.id();
        let finished = self.entry.get().finished;
        self.entry.run(|entry| match entry {
            entries::Entry::Deferred(input) => match route(id, input) {
                Ok(task) => Destination::Idle(
                    entries::Entry::Active(task::Active {
                        task,
                        pending: task::Pending::Future,
                    }),
                    Ok(task::Dispatch::Active { key: id }),
                ),
                Err(error) => {
                    Destination::stop(finished, Ok(task::Dispatch::Rejected { key: id, error }))
                }
            },
            entry @ (entries::Entry::Pending(_)
            | entries::Entry::Building(_)
            | entries::Entry::Responding(_)
            | entries::Entry::Response(_)
            | entries::Entry::Closing(_)
            | entries::Entry::Duplex(_)) => Destination::Stay(entry, Ok(task::Dispatch::Idle)),
            entry => {
                let phase = entry.phase();
                Destination::Stay(entry, Err(phase))
            }
        })
    }

    pub(in crate::manifold::requests) fn task(self) -> Option<task::Owned<'d, A::Task, M>> {
        self.entry.run(|entry| match entry {
            entries::Entry::Active(active) => {
                Destination::Remove(Some(task::Owned::Active(active)))
            }
            entries::Entry::Response(response) | entries::Entry::Closing(response) => {
                Destination::Remove(Some(task::Owned::Response(response)))
            }
            entry => Destination::Stay(entry, None),
        })
    }

    pub(in crate::manifold::requests) fn with_response<R>(
        self,
        run: impl FnOnce(
            requests::Id,
            Method,
            Result<task::Owned<'d, A::Task, M>, Code>,
        ) -> ControlFlow<task::Owned<'d, A::Task, M>, R>,
    ) -> Option<ControlFlow<(), (R, Option<bool>)>> {
        let id = self.id();
        let method = self.entry.get().method;
        let finished = self.entry.get().finished;
        self.entry.run(|entry| {
            let closing = matches!(
                entry,
                entries::Entry::Responding(_) | entries::Entry::Closing(_)
            );
            let owned = match entry {
                entries::Entry::Responding(code) => Err(code),
                entries::Entry::Active(active) => Ok(task::Owned::Active(active)),
                entries::Entry::Response(response) | entries::Entry::Closing(response) => {
                    Ok(task::Owned::Response(response))
                }
                entry => return Destination::Stay(entry, None),
            };
            match run(id, method, owned) {
                ControlFlow::Break(task::Owned::Active(active)) => {
                    let idle =
                        matches!(active.pending, task::Pending::Future | task::Pending::Body);
                    let entry = entries::Entry::Active(active);
                    if idle {
                        Destination::Idle(entry, Some(ControlFlow::Break(())))
                    } else {
                        Destination::Blocked(entry, Some(ControlFlow::Break(())))
                    }
                }
                ControlFlow::Break(task::Owned::Response(response)) => Destination::Blocked(
                    if closing {
                        entries::Entry::Closing(response)
                    } else {
                        entries::Entry::Response(response)
                    },
                    Some(ControlFlow::Break(())),
                ),
                ControlFlow::Continue(out) if closing || finished => {
                    Destination::Remove(Some(ControlFlow::Continue((out, Some(finished)))))
                }
                ControlFlow::Continue(out) => {
                    Destination::stop(finished, Some(ControlFlow::Continue((out, None))))
                }
            }
        })
    }
}
