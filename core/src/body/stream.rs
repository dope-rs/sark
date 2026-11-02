use core::num::NonZero;

use sark_http::payload::owned;

use crate::{
    body::{
        self, admit, credit, flow, ingress,
        input::{self, Parts},
    },
    manifold::stream::Boundary,
};

/// Incremental builder and the exact accounting state of its input.
pub struct Stream<'d, B: body::Build<'d>, O: credit::Origin = credit::raw::Stored> {
    pub(in crate::body) state: ingress::State<O>,
    pub(in crate::body) flow: flow::Flow<'d, B>,
}

/// Input ownership after one incremental stream transition.
#[must_use = "consume returned input owners or continue with the successor stream"]
pub enum Push<D, B, E> {
    Accepted(B),
    Discarded {
        build: B,
        input: D,
    },
    Refused {
        build: B,
        input: D,
        error: body::Error,
    },
    Failed {
        build: B,
        error: body::Error,
    },
    Rejected {
        build: B,
        error: E,
    },
}

impl<D, B, E> Push<D, B, E> {
    /// Changes only the builder owner, retaining the exact input and failure.
    pub fn map<C>(self, map: impl FnOnce(B) -> C) -> Push<D, C, E> {
        match self {
            Self::Accepted(build) => Push::Accepted(map(build)),
            Self::Discarded { build, input } => Push::Discarded {
                build: map(build),
                input,
            },
            Self::Refused {
                build,
                input,
                error,
            } => Push::Refused {
                build: map(build),
                input,
                error,
            },
            Self::Failed { build, error } => Push::Failed {
                build: map(build),
                error,
            },
            Self::Rejected { build, error } => Push::Rejected {
                build: map(build),
                error,
            },
        }
    }
}

/// The exact owner produced by one cooperative construction and admission turn.
#[must_use = "consume the output or retain the successor stream"]
pub enum Drive<'d, B: body::Build<'d>, O: credit::Origin = credit::raw::Stored> {
    NeedInput(Stream<'d, B, O>),
    Pending(Stream<'d, B, O>),
    Ready(flow::Ready<'d, B, O>),
    Failed {
        stream: Stream<'d, B, O>,
        error: flow::Error<B::StepError>,
    },
    Rejected {
        stream: Stream<'d, B, O>,
        error: body::Error,
    },
}

#[must_use = "consume the admitted output or retain the rejected ready owner"]
pub enum Admission<'d, B: body::Build<'d>, E, O: credit::Origin = credit::raw::Stored> {
    Accepted(B::Output),
    Rejected {
        ready: flow::Ready<'d, B, O>,
        evidence: E,
        error: body::Error,
    },
}

impl<'d, B: body::Build<'d>> Stream<'d, B> {
    pub fn new(open: admit::Track<admit::Open>, build: B) -> Self {
        Self {
            state: ingress::State::new(admit::Start::Open(open), credit::raw::Stored::new()),
            flow: flow::Flow::new(build),
        }
    }

    pub fn finished(done: admit::Track<admit::Finished>, build: B) -> Self {
        let state = ingress::State::new(admit::Start::Finished(done), credit::raw::Stored::new());
        let flow = flow::Flow::from_done(build, state.logical_len());
        Self { state, flow }
    }
}

impl<'d, B: body::Build<'d>, O: credit::Origin> Stream<'d, B, O> {
    pub fn start(
        open: admit::Track<admit::Open>,
        build: B,
        ledger: &credit::raw::Ledger<'_, O>,
    ) -> Self {
        Self {
            state: ingress::State::new(admit::Start::Open(open), ledger.origin()),
            flow: flow::Flow::new(build),
        }
    }

    pub fn ended(
        done: admit::Track<admit::Finished>,
        build: B,
        ledger: &credit::raw::Ledger<'_, O>,
    ) -> Self {
        let state = ingress::State::new(admit::Start::Finished(done), ledger.origin());
        let flow = flow::Flow::from_done(build, state.logical_len());
        Self { state, flow }
    }

    pub(super) const fn from_parts(state: ingress::State<O>, flow: flow::Flow<'d, B>) -> Self {
        Self { state, flow }
    }

    pub const fn max_body(&self) -> usize {
        self.state.max_body()
    }

    pub const fn retention(&self) -> admit::Retention {
        self.state.retention()
    }

    pub fn discard<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: &mut E,
    ) -> credit::Discard<O> {
        self.into_release().try_discard(ledger, event)
    }

    pub fn abandon<E: credit::Evidence>(self, event: E) {
        self.into_release().discard(event);
    }

    pub fn into_release(self) -> credit::Release<O> {
        self.state.abandon()
    }

    pub fn try_discard<E>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: E,
        _: owned::Segments<'d>,
        bytes: usize,
        boundary: Boundary,
    ) -> body::Transfer<Self, body::Error>
    where
        E: credit::Evidence,
    {
        let Self { mut state, flow } = self;
        let transfer = {
            let mut turn = ingress::Turn::new(&mut state, ledger, event);
            if turn.state().retention() != admit::Retention::Discarded {
                body::Transfer::Rejected {
                    build: flow,
                    error: body::Error::SegmentMismatch,
                }
            } else {
                match turn.try_advance(bytes) {
                    Err(error) => body::Transfer::Rejected { build: flow, error },
                    Ok(advance) => {
                        advance.commit();
                        match boundary {
                            Boundary::More => body::Transfer::Accepted(flow),
                            Boundary::End => flow.try_finish(turn.state_mut()),
                        }
                    }
                }
            }
        };
        transfer.map(|flow| Self { state, flow })
    }

    pub fn try_push<D, I, E, F>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: E,
        input: input::Input<D>,
        boundary: Boundary,
        (rmax, cmax): (credit::Limits, credit::Limits),
        parts: F,
    ) -> Push<D, Self, <B as body::Push<'d>>::Error>
    where
        B: body::Push<'d>,
        I: Parts<'d>,
        E: credit::Evidence,
        F: FnOnce(D) -> I,
    {
        let Self { mut state, flow } = self;
        let (data, bytes, chunks) = input.into_parts();
        let pushed = {
            let mut turn = ingress::Turn::new(&mut state, ledger, event);
            match turn.state().retention() {
                admit::Retention::Discarded => match turn.try_advance(bytes) {
                    Ok(advance) => {
                        advance.commit();
                        match boundary {
                            Boundary::End => match flow.try_finish(turn.state_mut()) {
                                body::Transfer::Accepted(flow) => Push::Discarded {
                                    build: flow,
                                    input: data,
                                },
                                body::Transfer::Rejected { build, error } => Push::Refused {
                                    build,
                                    input: data,
                                    error,
                                },
                            },
                            Boundary::More => Push::Discarded {
                                build: flow,
                                input: data,
                            },
                        }
                    }
                    Err(error) => Push::Refused {
                        build: flow,
                        input: data,
                        error,
                    },
                },
                admit::Retention::Retained => {
                    let transfer = flow.try_push(&mut turn, parts(data), bytes, chunks, rmax, cmax);
                    match transfer {
                        flow::Push::Accepted(flow) => match boundary {
                            Boundary::End => match flow.try_finish(turn.state_mut()) {
                                body::Transfer::Accepted(flow) => Push::Accepted(flow),
                                body::Transfer::Rejected { build, error } => {
                                    Push::Failed { build, error }
                                }
                            },
                            Boundary::More => Push::Accepted(flow),
                        },
                        flow::Push::Failed { build, error } => Push::Failed { build, error },
                        flow::Push::Rejected { build, error } => Push::Rejected { build, error },
                    }
                }
            }
        };
        pushed.map(|flow| Self { state, flow })
    }

    pub fn try_finish<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: E,
    ) -> body::Transfer<Self, body::Error> {
        let Self { mut state, flow } = self;
        let transfer = {
            let mut turn = ingress::Turn::new(&mut state, ledger, event);
            flow.try_finish(turn.state_mut())
        };
        transfer.map(|flow| Self { state, flow })
    }

    /// Finishes with the carrier's final logical total, including prior input.
    pub fn try_exact<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: E,
        len: usize,
    ) -> body::Transfer<Self, body::Error> {
        let Self { mut state, flow } = self;
        let transfer = {
            let mut turn = ingress::Turn::new(&mut state, ledger, event);
            flow.try_exact(&mut turn, len)
        };
        transfer.map(|flow| Self { state, flow })
    }

    pub fn try_drive<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        mut event: E,
        budget: NonZero<usize>,
    ) -> (Drive<'d, B, O>, E) {
        let Self { state, flow } = self;
        let step = flow.try_drive(state, ledger, &mut event, budget);
        (step, event)
    }

    pub fn try_step<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: &mut E,
        budget: NonZero<usize>,
    ) -> Drive<'d, B, O> {
        let (step, _) = self.try_drive(ledger, event, budget);
        step
    }
}

const _: () = assert!(
    core::mem::size_of::<Push<usize, usize, u8>>()
        <= core::mem::size_of::<Push<usize, usize, body::Error>>()
);
