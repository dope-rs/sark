use core::{marker, num::NonZero};

use o3::buffer::segments;

use crate::body::{
    self, credit, ingress,
    input::Parts,
    stream::{Admission, Drive, Stream},
};

mod raw;

enum State<B, O> {
    Building(B),
    Ready(O),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error<E> {
    LengthMismatch,
    Credit(credit::Error),
    Build(E),
    Release { build: E, error: credit::Error },
}

pub(super) enum Push<B, E> {
    Accepted(B),
    Failed { build: B, error: body::Error },
    Rejected { build: B, error: E },
}

/// Linear request construction whose ready payload replaces its builder.
pub struct Flow<'d, B: body::Build<'d>> {
    state: State<B, B::Output>,
    pub(in crate::body) lifetime: marker::PhantomData<&'d ()>,
}

/// Completed output retaining its exact finished ingress and unsettled credit.
#[must_use = "admit the output or discard its retained owner"]
pub struct Ready<'d, B: body::Build<'d>, O: credit::Origin = credit::raw::Stored> {
    state: ingress::State<O>,
    output: B::Output,
    lifetime: marker::PhantomData<&'d ()>,
}

impl<'d, B: body::Build<'d>, O: credit::Origin> Ready<'d, B, O> {
    fn try_new<E: credit::Evidence>(
        state: ingress::State<O>,
        output: B::Output,
        event: &mut E,
    ) -> Drive<'d, B, O> {
        let state = match state.try_ready() {
            Ok(state) => state,
            Err(state) => {
                return Drive::Rejected {
                    stream: Stream::from_parts(state, Flow::from_output(output)),
                    error: body::Error::InputIncomplete,
                };
            }
        };
        event.build_ready();
        Drive::Ready(Self {
            state,
            output,
            lifetime: marker::PhantomData,
        })
    }

    pub fn try_admit<E: credit::Evidence>(
        mut self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        mut event: E,
    ) -> Admission<'d, B, E, O> {
        match ingress::Turn::new(&mut self.state, ledger, &mut event)
            .try_drain(credit::Disposition::Admitted)
        {
            Ok(()) => Admission::Accepted(self.output),
            Err(error) => Admission::Rejected {
                ready: self,
                evidence: event,
                error,
            },
        }
    }

    pub fn discard<E: credit::Evidence>(
        self,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: E,
    ) -> credit::Discard<O> {
        self.into_release().try_discard(ledger, event)
    }

    pub fn into_release(self) -> credit::Release<O> {
        let Self { state, output, .. } = self;
        drop(output);
        state.abandon()
    }
}

impl<'d, B: body::Build<'d>> Flow<'d, B> {
    pub const fn new(build: B) -> Self {
        Self {
            state: State::Building(build),
            lifetime: marker::PhantomData,
        }
    }

    pub(super) fn from_done(build: B, len: usize) -> Self {
        Self::new(build.finish_input(len))
    }

    fn from_output(output: B::Output) -> Self {
        Self {
            state: State::Ready(output),
            lifetime: marker::PhantomData,
        }
    }

    pub(super) fn try_parts<const COUNT: bool, I>(
        mut build: B,
        parts: I,
        bytes: usize,
        chunks: segments::Count,
    ) -> Push<Self, <B as body::Push<'d>>::Error>
    where
        B: body::Push<'d>,
        I: Parts<'d>,
    {
        if I::EXACT {
            let size = parts.dimensions();
            if size.bytes() != bytes || COUNT && size.chunks() != chunks {
                return Push::Failed {
                    build: Self::new(build),
                    error: body::Error::SegmentMismatch,
                };
            }
        }
        let mut seen = 0usize;
        let mut count = segments::Count::EMPTY;
        for part in parts {
            if !I::EXACT && part.is_empty() {
                continue;
            }
            if !I::EXACT {
                let Some(total) = seen.checked_add(part.len()).filter(|total| *total <= bytes)
                else {
                    return Push::Failed {
                        build: Self::new(build),
                        error: body::Error::SegmentMismatch,
                    };
                };
                seen = total;
                if COUNT {
                    let Some(tally) = count
                        .checked_add(segments::Count::ONE)
                        .filter(|tally| *tally <= chunks)
                    else {
                        return Push::Failed {
                            build: Self::new(build),
                            error: body::Error::SegmentMismatch,
                        };
                    };
                    count = tally;
                }
            }
            build = match body::Push::push(build, part) {
                body::Transfer::Accepted(build) => build,
                body::Transfer::Rejected { build, error } => {
                    return Push::Rejected {
                        build: Self::new(build),
                        error,
                    };
                }
            };
        }
        if !I::EXACT && (seen != bytes || COUNT && count != chunks) {
            return Push::Failed {
                build: Self::new(build),
                error: body::Error::SegmentMismatch,
            };
        }
        Push::Accepted(Self::new(build))
    }

    pub(super) fn try_push<I, E, O: credit::Origin>(
        self,
        turn: &mut ingress::Turn<'_, '_, E, O>,
        parts: I,
        bytes: usize,
        chunks: segments::Count,
        rmax: credit::Limits,
        cmax: credit::Limits,
    ) -> Push<Self, <B as body::Push<'d>>::Error>
    where
        B: body::Push<'d>,
        I: Parts<'d>,
        E: credit::Evidence,
    {
        let State::Building(build) = self.state else {
            return Push::Failed {
                build: self,
                error: body::Error::InputFinished,
            };
        };
        let next = match turn.try_advance(bytes) {
            Ok(next) => next,
            Err(error) => {
                return Push::Failed {
                    build: Self::new(build),
                    error,
                };
            }
        };
        let amount = credit::Amount::new(bytes, chunks);
        let next = match next.try_reserve(amount, rmax, cmax) {
            Ok(next) => next.commit(),
            Err(error) => {
                return Push::Failed {
                    build: Self::new(build),
                    error: body::Error::Credit(error),
                };
            }
        };

        let transfer = Self::try_parts::<true, _>(build, parts, bytes, chunks);
        if matches!(transfer, Push::Accepted(_)) {
            next.commit();
        }
        transfer
    }

    /// Finishes with the final logical total, including previously counted input.
    /// Discarded input may defer accounting until its carrier knows that total.
    pub fn try_exact<E: credit::Evidence, O: credit::Origin>(
        self,
        turn: &mut ingress::Turn<'_, '_, E, O>,
        len: usize,
    ) -> body::Transfer<Self, body::Error> {
        if let Err(error) = turn.state_mut().try_exact(len) {
            return body::Transfer::Rejected { build: self, error };
        }
        self.try_finish(turn.state_mut())
    }

    pub(super) fn try_finish<O: credit::Origin>(
        self,
        state: &mut ingress::State<O>,
    ) -> body::Transfer<Self, body::Error> {
        self.try_end_with(|| state.try_end())
    }

    pub(super) fn try_complete<O: credit::Origin>(
        self,
        state: &mut ingress::State<O>,
    ) -> body::Transfer<Self, body::Error> {
        self.try_end_with(|| {
            if state.is_finished() {
                Ok(state.logical_len())
            } else {
                state.try_end()
            }
        })
    }

    fn try_end_with(
        self,
        finish: impl FnOnce() -> Result<usize, body::Error>,
    ) -> body::Transfer<Self, body::Error> {
        let State::Building(build) = self.state else {
            return body::Transfer::Rejected {
                build: self,
                error: body::Error::InputFinished,
            };
        };
        let len = match finish() {
            Ok(len) => len,
            Err(error) => {
                return body::Transfer::Rejected {
                    build: Self::new(build),
                    error,
                };
            }
        };
        body::Transfer::Accepted(Self::new(build.finish_input(len)))
    }

    pub(super) fn try_drive<E: credit::Evidence, O: credit::Origin>(
        self,
        mut state: ingress::State<O>,
        ledger: &mut credit::raw::Ledger<'_, O>,
        event: &mut E,
        budget: NonZero<usize>,
    ) -> Drive<'d, B, O> {
        let build = match self.state {
            State::Building(build) => build,
            State::Ready(output) => return Ready::<B, O>::try_new(state, output, event),
        };
        match build.step(budget) {
            body::Step::NeedInput { build, released } => {
                let flow = Self::new(build);
                let result = ingress::Turn::new(&mut state, ledger, &mut *event)
                    .try_release(released, credit::Disposition::Discarded);
                let error = match result {
                    Ok(()) if state.is_finished() => Error::LengthMismatch,
                    Ok(()) => return Drive::NeedInput(Stream::from_parts(state, flow)),
                    Err(error) => Error::Credit(error),
                };
                Drive::Failed {
                    stream: Stream::from_parts(state, flow),
                    error,
                }
            }
            body::Step::Pending { build, released } => {
                let flow = Self::new(build);
                let result = ingress::Turn::new(&mut state, ledger, &mut *event)
                    .try_release(released, credit::Disposition::Discarded);
                match result {
                    Ok(()) => Drive::Pending(Stream::from_parts(state, flow)),
                    Err(error) => Drive::Failed {
                        stream: Stream::from_parts(state, flow),
                        error: Error::Credit(error),
                    },
                }
            }
            body::Step::Ready { output, released } => {
                if let Err(error) = ingress::Turn::new(&mut state, ledger, &mut *event)
                    .try_release(released, credit::Disposition::Discarded)
                {
                    return Drive::Failed {
                        stream: Stream::from_parts(state, Self::from_output(output)),
                        error: Error::Credit(error),
                    };
                }
                Ready::<B, O>::try_new(state, output, event)
            }
            body::Step::Failed {
                build,
                error,
                released,
            } => {
                let flow = Self::new(build);
                let error = match ingress::Turn::new(&mut state, ledger, &mut *event)
                    .try_release(released, credit::Disposition::Discarded)
                {
                    Ok(()) => Error::Build(error),
                    Err(release) => Error::Release {
                        build: error,
                        error: release,
                    },
                };
                Drive::Failed {
                    stream: Stream::from_parts(state, flow),
                    error,
                }
            }
        }
    }
}

const _: () = {
    assert!(core::mem::size_of::<Error<u8>>() == 4 * core::mem::size_of::<usize>());
};
