use crate::body::admit;
type Transfer<T, E> = Result<T, mem::Rejected<T, Transition<E>>>;
use o3::{buffer::segments, mem};

use crate::{
    body::{self, credit, flow, ingress, input, stream},
    manifold::stream::Boundary,
};

mod loan;
mod receive;
mod rejected;
mod store;

pub use loan::Loan;
pub use rejected::Rejected;
pub use store::Store;

/// Logical body state and its retained transport storage.
#[derive(Debug)]
pub struct State<S, O: credit::Origin = credit::raw::Stored> {
    pub(in crate::body) state: ingress::State<O>,
    pub(in crate::body) storage: Option<S>,
}

/// Exact failure of one buffered body-input transition.
#[derive(Debug)]
pub enum Error<E> {
    Body(body::Error),
    Retain(E),
}

/// Exact failure of buffered-to-stream ownership transfer.
#[derive(Debug)]
pub enum Transition<E> {
    Body(body::Error),
    Build(E),
}

/// Input ownership returned by one retention attempt.
#[must_use = "retention must either consume or return the input owner"]
pub enum Retain<S, D, E> {
    Accepted {
        storage: Option<S>,
    },
    Rejected {
        storage: Option<S>,
        input: D,
        error: E,
    },
}

/// Input ownership after one buffered body transition.
#[must_use = "accepted discarded input still belongs to the caller"]
pub enum Push<D, E> {
    Accepted,
    Discarded(D),
    Rejected { input: D, error: Error<E> },
}

impl<S> State<S> {
    pub fn new(open: admit::Track<admit::Open>) -> Self {
        Self {
            state: ingress::State::new(admit::Start::Open(open), credit::raw::Stored::new()),
            storage: None,
        }
    }

    pub fn finished(done: admit::Track<admit::Finished>) -> Self {
        Self {
            state: ingress::State::new(admit::Start::Finished(done), credit::raw::Stored::new()),
            storage: None,
        }
    }
}

impl<S, O: credit::Origin> State<S, O> {
    pub fn start(open: admit::Track<admit::Open>, ledger: &credit::raw::Ledger<'_, O>) -> Self {
        Self {
            state: ingress::State::new(admit::Start::Open(open), ledger.origin()),
            storage: None,
        }
    }

    pub fn ended(done: admit::Track<admit::Finished>, ledger: &credit::raw::Ledger<'_, O>) -> Self {
        Self {
            state: ingress::State::new(admit::Start::Finished(done), ledger.origin()),
            storage: None,
        }
    }

    pub const fn max_body(&self) -> usize {
        self.state.max_body()
    }

    pub const fn retention(&self) -> admit::Retention {
        self.state.retention()
    }

    pub const fn stream(&self) -> admit::Stream {
        self.state.stream()
    }

    pub const fn len(&self) -> usize {
        self.state.logical_len()
    }

    pub const fn is_empty(&self) -> bool {
        self.state.logical_len() == 0
    }

    pub fn try_push<D, E, F, R>(
        &mut self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: E,
        input: input::Input<D>,
        rmax: credit::Limits,
        cmax: credit::Limits,
        retain: F,
    ) -> Push<D, R>
    where
        E: credit::Evidence,
        F: FnOnce(Option<S>, D) -> Retain<S, D, R>,
    {
        let turn = ingress::Turn::new(&mut self.state, ledger, event);
        receive::Receive {
            turn,
            storage: &mut self.storage,
        }
        .try_push(input, rmax, cmax, retain)
    }

    /// Ends input with its final logical total, preserving storage and credit.
    pub fn try_exact(mut self, len: usize) -> body::Transfer<Self, body::Error> {
        if let Err(error) = self.state.try_exact(len) {
            return body::Transfer::Rejected { build: self, error };
        }
        if let Err(error) = self.state.try_end() {
            return body::Transfer::Rejected { build: self, error };
        }
        body::Transfer::Accepted(self)
    }

    pub fn try_finish<'d, E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: &mut E,
    ) -> Result<body::Body<S::Output>, Rejected<S, S::Output, O>>
    where
        S: Store<'d>,
    {
        let Self { state, storage } = self;
        let (track, mut release) = match state.try_finish() {
            Ok(finished) => finished,
            Err(failure) => {
                let (state, error) = failure.into_parts();
                return Err(Rejected::Admission {
                    state: Self { state, storage },
                    error,
                });
            }
        };
        let source = S::finish(storage);
        let body = match body::Body::try_new(track, source) {
            Ok(body) => body,
            Err(failure) => {
                return Err(Rejected::Storage { failure, release });
            }
        };
        let freed = match ledger.amount().try_drain(&mut release.amount) {
            Ok(change) => change.commit(),
            Err(error) => {
                return Err(Rejected::Credit {
                    body,
                    release,
                    error: body::Error::Credit(error),
                });
            }
        };
        event.released(freed, credit::Disposition::Admitted);
        Ok(body)
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
}

impl<S, O: credit::Origin> State<S, O> {
    /// Moves already-accounted retained input into an incremental builder.
    /// The request state and its ledger charge remain the same capability;
    /// neither bytes nor owners are copied or reserved a second time.
    pub fn try_stream<'d, B, I, F>(
        self,
        build: B,
        boundary: Boundary,
        parts: F,
    ) -> Transfer<stream::Stream<'d, B, O>, <B as body::Push<'d>>::Error>
    where
        B: body::Push<'d>,
        I: input::Parts<'d>,
        F: FnOnce(S) -> I,
    {
        let Self {
            mut state,
            storage: store,
        } = self;
        let transfer = match store {
            Some(store) => flow::Flow::try_parts::<false, _>(
                build,
                parts(store),
                state.logical_len(),
                segments::Count::EMPTY,
            ),
            None if state.retention() == admit::Retention::Retained && state.logical_len() != 0 => {
                flow::Push::Failed {
                    build: flow::Flow::new(build),
                    error: body::Error::SegmentMismatch,
                }
            }
            None => flow::Push::Accepted(flow::Flow::new(build)),
        };
        let transfer = match boundary {
            Boundary::End => match transfer {
                flow::Push::Accepted(flow) => match flow.try_complete(&mut state) {
                    body::Transfer::Accepted(flow) => flow::Push::Accepted(flow),
                    body::Transfer::Rejected { build, error } => {
                        flow::Push::Failed { build, error }
                    }
                },
                failed => failed,
            },
            Boundary::More => transfer,
        };
        match transfer {
            flow::Push::Accepted(flow) => Ok(stream::Stream::from_parts(state, flow)),
            flow::Push::Failed { build: flow, error } => {
                let stream = stream::Stream::from_parts(state, flow);
                Err(mem::Rejected::new(stream, Transition::Body(error)))
            }
            flow::Push::Rejected { build: flow, error } => {
                let stream = stream::Stream::from_parts(state, flow);
                Err(mem::Rejected::new(stream, Transition::Build(error)))
            }
        }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Error<u8>>() == 3 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Result<(), Error<u8>>>() == 3 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Transition<u8>>() == 3 * core::mem::size_of::<usize>());
    assert!(
        core::mem::size_of::<Result<(), Transition<u8>>>() == 3 * core::mem::size_of::<usize>()
    );
};
