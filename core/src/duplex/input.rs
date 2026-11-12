//! Carrier-generic duplex input ownership and terminal ordering.
use sark_http::payload::owned;

use crate::{
    duplex::{self, admit, machine},
    manifold::stream::Boundary,
};

/// A carrier buffer whose physical owners can enter the canonical duplex ABI.
/// Implementations are statically dispatched; no erased storage or callback is
/// present in the hot path.
pub trait Buf<'d> {
    type Input;
    type Error;
    type Receipt;

    fn state(&self) -> Buffer;

    fn push(&mut self, input: Self::Input) -> Result<(), Rejected<Self::Input, Self::Error>>;

    fn take(&mut self) -> Take<'d, Self::Receipt>;
}

/// Exact occupancy of a carrier-owned input buffer.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Buffer {
    Empty,
    Queued,
}

/// Exact commit result of removing one carrier input quantum.
pub enum Take<'d, R> {
    Empty,
    Item {
        input: owned::Segments<'d>,
        receipt: R,
    },
}

use o3::mem::Rejected;

/// Failure to admit one carrier input into an established duplex direction.
#[must_use = "a rejected duplex input remains an affine owner"]
#[derive(Debug)]
pub enum PushError<I, E> {
    Finished(I),
    Buffer(Rejected<I, E>),
    Finish {
        input: I,
        error: machine::FinishError,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Phase {
    Open,
    Finished,
    Closed,
}

/// Exact external state of the carrier input direction.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Open,
    Closed,
}

/// Exact scheduling demand of the carrier input direction.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Work {
    Idle,
    Ready,
}

/// One protocol-neutral session paired with its carrier-owned input buffer.
pub struct State<'d, D, B>
where
    D: duplex::Session<'d>,
{
    raw: machine::Machine<'d, D>,
    buffer: B,
    phase: Phase,
}

/// One machine transaction joined to its affine carrier input disposition.
#[must_use = "consume the duplex transaction and its affine carrier receipt"]
pub struct Turn<T, F> {
    step: T,
    feed: Feed<F>,
}

impl<T, F> Turn<T, F> {
    pub fn into_parts(self) -> (T, Feed<F>) {
        (self.step, self.feed)
    }
}

/// Exact removal disposition for one carrier input quantum.
#[derive(PartialEq, Eq)]
#[must_use = "consume the affine receipt for the input callback that ran"]
pub enum Feed<R> {
    Idle,
    Consumed(R),
}

impl<'d, D, B> State<'d, D, B>
where
    D: duplex::Session<'d>,
    B: Buf<'d>,
{
    pub fn open(session: D, buffer: B) -> Self {
        Self {
            raw: machine::Machine::new(session),
            buffer,
            phase: Phase::Open,
        }
    }

    pub fn finished(session: D, buffer: B) -> Self {
        Self {
            raw: machine::Machine::with_finish(session),
            buffer,
            phase: Phase::Finished,
        }
    }

    /// Cancels the session and returns its queued carrier input.
    pub fn into_buffer(self) -> B {
        self.buffer
    }

    pub const fn status(&self) -> Status {
        match (self.phase, self.raw.progress()) {
            (Phase::Open, machine::Progress::Idle | machine::Progress::Turn) => Status::Open,
            (Phase::Finished | Phase::Closed, _) | (Phase::Open, machine::Progress::Finished) => {
                Status::Closed
            }
        }
    }

    pub fn work(&self) -> Work {
        match (self.raw.progress(), self.phase, self.buffer.state()) {
            (machine::Progress::Finished, _, _)
            | (machine::Progress::Idle, _, Buffer::Empty)
            | (machine::Progress::Idle, Phase::Closed, Buffer::Queued) => Work::Idle,
            (machine::Progress::Idle | machine::Progress::Turn, _, _) => Work::Ready,
        }
    }

    /// Appends one exact DATA owner to the carrier buffer without exposing the
    /// session machine between admission and callback consumption.
    pub fn push(
        &mut self,
        input: B::Input,
        boundary: Boundary,
    ) -> Result<(), PushError<B::Input, B::Error>> {
        match self.status() {
            Status::Open => {}
            Status::Closed => return Err(PushError::Finished(input)),
        }

        let (finish, input) = match boundary {
            Boundary::End => match self.raw.finish(input) {
                Ok((finish, input)) => (Some(finish), input),
                Err((error, input)) => return Err(PushError::Finish { input, error }),
            },
            Boundary::More => (None, input),
        };

        self.buffer.push(input).map_err(PushError::Buffer)?;
        if let Some(finish) = finish {
            finish.commit();
            self.phase = match self.buffer.state() {
                Buffer::Empty => Phase::Closed,
                Buffer::Queued => Phase::Finished,
            };
        }
        Ok(())
    }

    /// Records one peer FIN. Empty input can enter the machine immediately;
    /// queued bytes retain priority over the terminal callback.
    pub fn finish(&mut self) -> Result<(), machine::FinishError> {
        match self.phase {
            Phase::Open => {
                let finish = match self.raw.finish(()) {
                    Ok((finish, ())) => finish,
                    Err((error, ())) => return Err(error),
                };
                finish.commit();
                self.phase = match self.buffer.state() {
                    Buffer::Empty => Phase::Closed,
                    Buffer::Queued => Phase::Finished,
                };
                Ok(())
            }
            Phase::Finished => Err(machine::FinishError::Finished),
            Phase::Closed => Err(machine::FinishError::Closed),
        }
    }

    /// Removes at most one carrier quantum and executes exactly one canonical
    /// session/admission transaction. The returned receipt proves which
    /// carrier quantum was removed during this transaction.
    pub fn transact<R, E>(
        &mut self,
        admit: impl for<'turn> FnOnce(
            admit::Emission<'turn, 'd, D>,
        ) -> admit::Admission<'turn, 'd, D, R, E>,
    ) -> Turn<machine::Transaction<R, E>, B::Receipt> {
        let feed = self.feed();
        let step = self.raw.transact(
            o3::buffer::write::Slice::new(&mut []).reborrow(),
            &mut Vec::new(),
            admit,
        );
        Turn { step, feed }
    }

    fn feed(&mut self) -> Feed<B::Receipt> {
        match (self.phase, self.raw.progress()) {
            (Phase::Closed, _) | (Phase::Open | Phase::Finished, machine::Progress::Finished) => {
                return Feed::Idle;
            }
            (Phase::Open | Phase::Finished, machine::Progress::Idle | machine::Progress::Turn) => {}
        }
        let raw = &mut self.raw;
        let buffer = &mut self.buffer;
        let slot = match raw.input() {
            machine::Access::Vacant(slot) => slot,
            machine::Access::Blocked => return Feed::Idle,
        };
        match buffer.take() {
            Take::Empty => Feed::Idle,
            Take::Item { input, receipt } => {
                slot.commit(input);
                if self.phase == Phase::Finished {
                    match buffer.state() {
                        Buffer::Empty => self.phase = Phase::Closed,
                        Buffer::Queued => {}
                    }
                };
                Feed::Consumed(receipt)
            }
        }
    }
}
