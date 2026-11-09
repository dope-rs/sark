use crate::manifold::owners;
use core::marker::PhantomData;
use sark_core::manifold::task::app;
use std::num::NonZero;

use sark_core::{
    body::{self, credit, input, stream},
    duplex::input::PushError,
    manifold::stream::Boundary,
};
use sark_http::payload::response::Body as _;

use crate::{
    Payload,
    manifold::{
        Phase, encoder,
        requests::{
            self, Error,
            entries::{
                self, Destination,
                live::{Failure, Live, Push},
                status,
            },
            exchange, task,
        },
    },
};

mod buffers;

/// A selected request slot retaining its exact issuing account identity.
pub(in crate::manifold::requests) struct Credit<
    'a,
    'd,
    A: app::Runtime<'d>,
    M: owners::Handoff,
    O: credit::Origin,
> {
    live: Live<'a, 'd, A, M>,
    origin: O,
    invariant: PhantomData<fn(O) -> O>,
}

// SAFETY: rebinding retains the same exclusive request slot, queue roots and
// owners; the carrier supplies the exact account origin retained by Credit.
unsafe impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff> credit::raw::Bind
    for Live<'a, 'd, A, M>
{
    type Origin = credit::raw::Stored;
    type Bound<O: credit::Origin> = Credit<'a, 'd, A, M, O>;

    unsafe fn bind<O: credit::Origin>(self, origin: O) -> Self::Bound<O> {
        Credit {
            live: self,
            origin,
            invariant: PhantomData,
        }
    }
}

impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff, O: credit::Origin> Credit<'a, 'd, A, M, O> {
    pub(in crate::manifold::requests) fn reset<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        proof: E,
    ) -> Result<exchange::Reset, requests::Error> {
        use exchange::Reset;

        let origin = self.origin;
        self.live.entry.run(|entry| match entry {
            entries::Entry::Reserved => Destination::Remove(Ok(Reset::Cancelled)),
            entries::Entry::Pending(body) => Destination::Remove(
                // SAFETY: this selected slot owns the body charged to origin.
                unsafe { buffers::try_discard(body, ledger, proof, origin) }
                    .map(|()| Reset::Cancelled),
            ),
            entries::Entry::Building(body) => Destination::Remove(
                discard(
                    // SAFETY: Credit retains this selected slot's exact account proof.
                    unsafe { credit::raw::Bind::bind(body, origin) }.into_release(),
                    ledger,
                    proof,
                )
                .map(|()| Reset::Cancelled)
                .map_err(requests::Error::from),
            ),
            entries::Entry::Deferred(_)
            | entries::Entry::Responding(_)
            | entries::Entry::Duplex(_) => Destination::Remove(Ok(Reset::Cancelled)),
            entries::Entry::Stopped => Destination::Remove(Ok(Reset::Missing)),
            entry @ (entries::Entry::Active(_)
            | entries::Entry::Response(_)
            | entries::Entry::Closing(_)) => Destination::Stay(entry, Ok(Reset::Missing)),
        })
    }

    pub(in crate::manifold::requests) fn try_finish<E: credit::Evidence, F>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        proof: E,
        response: impl FnOnce(task::Buffered<'d, A::Prepared>) -> Result<encoder::Pending<'d, M>, F>,
    ) -> Result<exchange::Termination<F>, exchange::Error> {
        use exchange::Termination;

        let method = self.live.entry.get().method;
        let idle = self.live.entry.get().queue == status::Queue::Idle;
        let origin = self.origin;
        self.live.entry.run(|entry| match entry {
            entries::Entry::Reserved => Destination::Remove(Ok(Termination::Cancelled)),
            entries::Entry::Stopped => Destination::Remove(Ok(Termination::Removed)),
            entry @ (entries::Entry::Active(_)
            | entries::Entry::Deferred(_)
            | entries::Entry::Response(_)
            | entries::Entry::Closing(_)) => Destination::Stay(entry, Ok(Termination::Retained)),
            entry @ entries::Entry::Responding(_) => {
                Destination::Stay(entry, Ok(Termination::InputFinished))
            }
            entries::Entry::Pending(wait) => {
                // SAFETY: this selected slot owns the body charged to origin.
                match unsafe { buffers::try_finish(wait, ledger, proof, origin) } {
                    Ok(entries::Finished::Sync(request)) => {
                        match response(task::Buffered { request, method }) {
                            Ok(response) => Destination::Ready(
                                entries::Entry::Response(response),
                                Ok(Termination::Completed(Ok(()))),
                            ),
                            Err(error) => {
                                Destination::Remove(Ok(Termination::Completed(Err(error))))
                            }
                        }
                    }
                    Ok(entries::Finished::Task(request)) => Destination::Ready(
                        entries::Entry::Deferred(task::Input::Buffered(request)),
                        Ok(Termination::Deferred),
                    ),
                    Err(error) => Destination::Idle(entries::Entry::Stopped, Err(error)),
                }
            }
            entries::Entry::Building(body) => match
                // SAFETY: the selected slot and this account remain under Credit's exclusive loan.
                unsafe { credit::raw::Bind::bind(body, origin) }.try_finish(ledger, proof) {
                body::Transfer::Accepted(body) => Destination::Ready(
                    entries::Entry::Building(unsafe { ledger.store(body) }),
                    Ok(Termination::InputFinished),
                ),
                body::Transfer::Rejected { build, error } => Destination::Stay(
                    entries::Entry::Building(unsafe { ledger.store(build) }),
                    Err(exchange::Error::Request(error.into())),
                ),
            },
            entries::Entry::Duplex(mut state) => {
                if let Err(error) = state.input.finish() {
                    return Destination::Stay(
                        entries::Entry::Duplex(state),
                        Err(exchange::Error::Request(error.into())),
                    );
                }
                if idle {
                    Destination::Ready(
                        entries::Entry::Duplex(state),
                        Ok(Termination::InputFinished),
                    )
                } else {
                    Destination::Stay(
                        entries::Entry::Duplex(state),
                        Ok(Termination::InputFinished),
                    )
                }
            }
        })
    }

    pub(in crate::manifold::requests) fn stop<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        proof: E,
    ) -> Result<exchange::Stop<task::Owned<'d, A::Task, M>>, requests::Error> {
        use exchange::Stop;

        let finished = self.live.entry.get().finished;
        let origin = self.origin;
        self.live.entry.run(|entry| match entry {
            entries::Entry::Stopped => {
                Destination::Stay(entries::Entry::Stopped, Ok(Stop::Ignored))
            }
            entries::Entry::Active(active) => {
                Destination::stop(finished, Ok(Stop::Task(task::Owned::Active(active))))
            }
            entries::Entry::Response(response) | entries::Entry::Closing(response) => {
                Destination::stop(finished, Ok(Stop::Task(task::Owned::Response(response))))
            }
            entries::Entry::Reserved
            | entries::Entry::Deferred(_)
            | entries::Entry::Responding(_) => Destination::stop(finished, Ok(Stop::Exchange(0))),
            entries::Entry::Duplex(state) => Destination::stop(
                finished,
                Ok(Stop::Exchange(state.input.into_buffer().discard())),
            ),
            entries::Entry::Pending(body) => Destination::stop(
                finished,
                // SAFETY: this selected slot owns the body charged to origin.
                unsafe { buffers::try_discard(body, ledger, proof, origin) }
                    .map(|()| Stop::Exchange(0)),
            ),
            entries::Entry::Building(body) => Destination::stop(
                finished,
                discard(
                    // SAFETY: Credit retains this selected slot's exact account proof.
                    unsafe { credit::raw::Bind::bind(body, origin) }.into_release(),
                    ledger,
                    proof,
                )
                .map(|()| Stop::Exchange(0))
                .map_err(requests::Error::from),
            ),
        })
    }

    pub(in crate::manifold::requests) fn drive<E, F>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        proof: F,
        budget: NonZero<usize>,
    ) -> Result<entries::Built<<A::Build as body::Build<'d>>::StepError>, Phase>
    where
        E: credit::Evidence,
        F: FnOnce(requests::Id) -> E,
    {
        use body::BuildOutcome;

        let id = self.live.id();
        let origin = self.origin;
        let mut input = None;
        let result = self.live.entry.run(|entry| match entry {
            entries::Entry::Building(body) => {
                let proof = proof(id);
                // SAFETY: this body was selected from the slot carrying origin.
                let body = unsafe { credit::raw::Bind::bind(body, origin) };
                let (step, proof) = body.try_drive(ledger, proof, budget);
                match step {
                    stream::Drive::Pending(body) => Destination::Rotate(
                        entries::Entry::Building(unsafe { ledger.store(body) }),
                        Ok(BuildOutcome::Pending),
                    ),
                    stream::Drive::NeedInput(body) => {
                        input = Some(id);
                        Destination::Idle(
                            entries::Entry::Building(unsafe { ledger.store(body) }),
                            Ok(BuildOutcome::Pending),
                        )
                    }
                    stream::Drive::Ready(ready) => {
                        use sark_core::body::stream::Admission;

                        match ready.try_admit(ledger, proof) {
                            Admission::Accepted(output) => Destination::Rotate(
                                entries::Entry::Deferred(task::Input::Built { output }),
                                Ok(BuildOutcome::Pending),
                            ),
                            Admission::Rejected {
                                ready,
                                evidence: proof,
                                error,
                            } => {
                                let failed =
                                    Failure::admit(ledger, proof, ready.into_release(), error);
                                Destination::Idle(
                                    entries::Entry::Stopped,
                                    Ok(BuildOutcome::Failed {
                                        key: id,
                                        error: failed,
                                    }),
                                )
                            }
                        }
                    }
                    stream::Drive::Failed {
                        stream: body,
                        error,
                    } => {
                        let failed = match discard(body.into_release(), ledger, proof) {
                            Ok(()) => Failure::Step(error),
                            Err(release) => Failure::StepRelease { error, release },
                        };
                        Destination::Idle(
                            entries::Entry::Stopped,
                            Ok(BuildOutcome::Failed {
                                key: id,
                                error: failed,
                            }),
                        )
                    }
                    stream::Drive::Rejected {
                        stream: body,
                        error,
                    } => {
                        let failed = Failure::admit(ledger, proof, body.into_release(), error);
                        Destination::Idle(
                            entries::Entry::Stopped,
                            Ok(BuildOutcome::Failed {
                                key: id,
                                error: failed,
                            }),
                        )
                    }
                }
            }
            entry @ (entries::Entry::Pending(_)
            | entries::Entry::Deferred(_)
            | entries::Entry::Responding(_)
            | entries::Entry::Response(_)
            | entries::Entry::Closing(_)
            | entries::Entry::Duplex(_)) => Destination::Stay(entry, Ok(BuildOutcome::Idle)),
            entry => {
                let phase = entry.phase();
                Destination::Stay(entry, Err(phase))
            }
        });
        result.map(|outcome| (outcome, input))
    }
}

impl<'a, 'd, A: app::Runtime<'d>, M: owners::Handoff, O: credit::Origin> Credit<'a, 'd, A, M, O> {
    pub(in crate::manifold::requests) fn push<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        proof: E,
        input: Payload<'d>,
        boundary: Boundary,
        request: credit::Limits,
        conn: credit::Limits,
    ) -> Push<<A::Build as body::Push<'d>>::Error> {
        let origin = self.origin;
        let idle = self.live.entry.get().queue == status::Queue::Idle;
        self.live.entry.run(|entry| match entry {
            entries::Entry::Pending(mut body) => {
                // SAFETY: the loan names this selected slot's issuing account.
                match unsafe { buffers::try_push(&mut body, ledger, proof, input, request, conn) } {
                    Ok(()) => Destination::Stay(
                        entries::Entry::Pending(body),
                        Push::Accepted {
                            completion: boundary,
                        },
                    ),
                    Err(error) => {
                        Destination::Stay(entries::Entry::Pending(body), Push::Request(error))
                    }
                }
            }
            entries::Entry::Building(body) => {
                // SAFETY: both conversion directions retain this exact slot and account.
                let body = unsafe { credit::raw::Bind::bind(body, origin) };
                let bytes = input.len();
                let chunks = input.count();
                match body
                    .try_push(
                        ledger,
                        proof,
                        input::Input::new(input, bytes, chunks),
                        boundary,
                        (request, conn),
                        Payload::into_body,
                    )
                    .map(|body| unsafe { ledger.store(body) })
                {
                    stream::Push::Accepted(build) | stream::Push::Discarded { build, .. } => {
                        Destination::Ready(
                            entries::Entry::Building(build),
                            Push::Accepted {
                                completion: Boundary::More,
                            },
                        )
                    }
                    stream::Push::Refused { build, error, .. }
                    | stream::Push::Failed { build, error } => Destination::Stay(
                        entries::Entry::Building(build),
                        Push::Request(Error::Body(error)),
                    ),
                    stream::Push::Rejected { build, error } => {
                        Destination::Stay(entries::Entry::Building(build), Push::Build(error))
                    }
                }
            }
            entries::Entry::Duplex(mut state) => match state.input.push(input, boundary) {
                Ok(()) if idle => Destination::Ready(entries::Entry::Duplex(state), Push::Queued),
                Ok(()) => Destination::Stay(entries::Entry::Duplex(state), Push::Queued),
                Err(rejected) => {
                    let (input, error) = match rejected {
                        PushError::Finished(input) => {
                            (input, Error::Body(body::Error::InputFinished))
                        }
                        PushError::Buffer(rejected) => rejected.into_parts(),
                        PushError::Finish { input, error } => (input, Error::Duplex(error)),
                    };
                    drop(input);
                    Destination::Stay(entries::Entry::Duplex(state), Push::Request(error))
                }
            },
            entry @ (entries::Entry::Responding(_)
            | entries::Entry::Response(_)
            | entries::Entry::Closing(_)) => Destination::Stay(
                entry,
                Push::Accepted {
                    completion: boundary,
                },
            ),
            entry => {
                let phase = entry.phase();
                Destination::Stay(entry, Push::Inactive { phase })
            }
        })
    }
}

fn discard<O: credit::Origin, E: credit::Evidence>(
    release: credit::Release<O>,
    ledger: &mut credit::raw::Ledger<'_, O>,
    mut proof: E,
) -> Result<(), body::Error> {
    release
        .try_discard(ledger, &mut proof)
        .abandon(proof)
        .map_err(body::Error::Credit)
}
