//! Cooperative JSON decoding over independently retained transport segments.

use std::{iter, marker, mem, num::NonZero, ops::ControlFlow, task::Poll};

use o3::{
    alloc,
    buffer::{resident, segments, storage},
    mem::slot,
};
use sark_http::{payload::owned, request};

mod atom;
mod cursor;
mod document;
pub mod frames;
mod input;
mod nested;
mod scanner;
mod state;

pub(crate) use atom::Atom;
use cursor::Cursor;
pub use document::{
    Document, admitted::Admitted, project::Project, release::Release, value::Value,
};
pub use nested::Nested;
use scanner::{Prepared, Scanner};
pub(crate) use state::ValueKind;
use state::{Capture, ScanProgress, State, Stored, is_delimiter, is_overlap};

/// Bytes examined by one task poll before it yields to the application plane.
pub const DEFAULT_STEP_BUDGET: NonZero<usize> = NonZero::<usize>::MIN.saturating_add(4 * 1024 - 1);

#[derive(Clone, Copy, Eq, PartialEq)]
enum End {
    Allowed,
    Forbidden,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Unordered,
    Ordered,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Projection {
    None,
    Scalar,
    Bytes,
    Frame,
    /// A compound value consumed by another segmented decoder. Its transport
    /// owners must stay split instead of being coalesced at field boundaries.
    Segments,
}

#[derive(Clone, Copy)]
pub struct Field<'a> {
    name: &'a [u8],
    projection: Projection,
}

impl<'a> Field<'a> {
    #[must_use]
    pub const fn new(name: &'a [u8], mode: Projection) -> Self {
        Self {
            name,
            projection: mode,
        }
    }
}

/// Implemented by generated owned JSON values.
pub trait Decode<C: Copy = ()>: Sized {
    type State<'d>: Materialize<'d, Self>;

    const ORDER: Order;

    fn fields<'d>() -> &'d [Field<'d>];

    fn start<'d>(document: Document<'d>, context: C) -> Self::State<'d>;

    /// Retires input-sized collections before this completed value is dropped.
    /// Generated implementations visit array elements within the budget. A
    /// fixed-schema value's scalar fields and user-defined Drop are not split.
    fn release(&mut self, budget: &mut usize) -> Poll<()>;
}

/// Selects the generated value whose fields retain one exact request identity.
pub trait IntoRequest: Decode {
    type Request<'d>: Decode<request::Id<'d>>;
}

pub trait Materialize<'d, T>: Sized {
    fn step(self, budget: NonZero<usize>) -> Outcome<'d, Self, T>;

    /// Retains pending owners until their budgeted retirement is complete.
    fn release(&mut self, budget: &mut usize) -> Poll<()>;
}

pub enum Outcome<'d, S, T> {
    Pending(S),
    Ready(T),
    Failed(crate::parsing::Error<'d>),
}

/// A materialized value retained until its source owners finish bounded release.
pub struct Ready<'d, T> {
    value: Result<T, crate::parsing::Error<'d>>,
    release: Release<'d>,
}

impl<'d, T> Ready<'d, T> {
    #[must_use]
    pub fn new(value: T, document: Document<'d>) -> Self {
        Self {
            value: Ok(value),
            release: document.release(),
        }
    }

    #[must_use]
    pub fn try_new(value: Result<T, crate::parsing::Error<'d>>, document: Document<'d>) -> Self {
        Self {
            value,
            release: document.release(),
        }
    }
}

impl<'d, T> Materialize<'d, T> for Ready<'d, T> {
    fn step(self, budget: NonZero<usize>) -> Outcome<'d, Self, T> {
        match self.release.step(budget) {
            ControlFlow::Continue(release) => Outcome::Pending(Self {
                value: self.value,
                release,
            }),
            ControlFlow::Break(()) => match self.value {
                Ok(value) => Outcome::Ready(value),
                Err(error) => Outcome::Failed(error),
            },
        }
    }

    fn release(&mut self, budget: &mut usize) -> Poll<()> {
        self.release.retire(budget)
    }
}

/// Observable progress of a feedable decoder. `NeedInput` tells runtimes not
/// to poll again until another request segment arrives.
pub enum Progress<'d, T> {
    NeedInput,
    Pending,
    Ready(T),
    Failed(crate::parsing::Error<'d>),
}

/// Failure to allocate the initial decoder state.
#[repr(u8)]
#[derive(Debug)]
pub enum Create {
    Exhausted,
    Overflow,
}

impl From<alloc::Error> for Create {
    fn from(error: alloc::Error) -> Self {
        match error {
            alloc::Error::Exhausted => Self::Exhausted,
            alloc::Error::Overflow => Self::Overflow,
        }
    }
}

const _: () = assert!(mem::size_of::<Create>() == 1);

/// Failure to transfer one retained input owner into a streaming decoder.
pub enum Push<'d> {
    Complete(resident::Shared<'d>),
    Length {
        len: usize,
        add: usize,
        owner: resident::Shared<'d>,
    },
    Segments {
        current: segments::Count,
        add: segments::Count,
        owner: resident::Shared<'d>,
    },
    Allocation {
        error: alloc::Error,
        owner: resident::Shared<'d>,
    },
}

/// State ownership after one fallible transfer attempt.
#[must_use = "the accepted or rejected state must be consumed"]
pub enum Transfer<S, E> {
    Accepted(S),
    Rejected { state: S, error: E },
}

impl<S, E> Transfer<S, E> {
    pub(crate) fn map<T, F>(
        self,
        state: impl FnOnce(S) -> T,
        error: impl FnOnce(E) -> F,
    ) -> Transfer<T, F> {
        match self {
            Self::Accepted(value) => Transfer::Accepted(state(value)),
            Self::Rejected {
                state: value,
                error: cause,
            } => Transfer::Rejected {
                state: state(value),
                error: error(cause),
            },
        }
    }
}

/// Resumable decoder. Input, replay, field lookup, and owner release work are
/// charged to the caller-provided budget. Its driver brand cannot be widened even
/// when the scanner owns every input segment.
///
/// Error retirement charges input owners, field slots, and array elements.
/// Fixed-schema scalar destruction and allocation metadata are constant work;
/// an arbitrary user-provided materializer or destructor must uphold its own
/// cooperative work contract. Dropping a decoder cancels it immediately.
pub struct Decoder<'d, T: Decode<C>, C: Copy = ()> {
    phase: slot::Storage<Phase<'d, T::State<'d>>>,
    context: C,
    bytes: usize,
    chunks: segments::Count,
    marker: marker::PhantomData<fn() -> T>,
}

enum Phase<'d, S> {
    Scan(Scanner<'d>),
    Start(Document<'d>),
    Build(S),
    Release {
        release: Release<'d>,
        error: Option<crate::parsing::Error<'d>>,
    },
    ReleaseBuild(S),
}

impl<'d, T: Decode> Decoder<'d, T> {
    /// Creates a decoder whose input is still being received.
    pub fn try_stream() -> Result<Self, Create> {
        Self::try_feed(())
    }

    pub fn new(segments: Vec<storage::Shared>) -> crate::Result<Self> {
        let scan = Scanner::new(segments, T::fields(), T::ORDER)?;
        Ok(Self::with_scan(scan, ()))
    }

    pub fn one(segment: storage::Shared) -> crate::Result<Self> {
        let scan = Scanner::new(iter::once(segment), T::fields(), T::ORDER)?;
        Ok(Self::with_scan(scan, ()))
    }

    /// Creates a decoder over owners which remain tied to their driver.
    pub fn from_scoped(segments: Vec<resident::Shared<'d>>) -> crate::Result<Self> {
        let scan = Scanner::new(segments, T::fields(), T::ORDER)?;
        Ok(Self::with_scan(scan, ()))
    }
}

impl<'d, T: Decode<C>, C: Copy> Decoder<'d, T, C> {
    fn with_scan(scan: Scanner<'d>, context: C) -> Self {
        Self {
            phase: slot::Storage::filled(Phase::Scan(scan), ()),
            context,
            bytes: 0,
            chunks: segments::Count::EMPTY,
            marker: marker::PhantomData,
        }
    }

    /// Starts streaming directly into the final output representation.
    pub fn try_feed(context: C) -> Result<Self, Create> {
        let scan = Scanner::try_stream(T::fields(), T::ORDER)?;
        Ok(Self::with_scan(scan, context))
    }

    /// Builds a recursive worker without reconstructing its admitted owner table.
    /// Allocation failure leaves the input unchanged; success transfers it once.
    #[doc(hidden)]
    pub fn try_admitted(input: &mut Admitted<'d>, context: C) -> crate::Result<Box<Self>> {
        let selection =
            scanner::Selection::try_new(T::fields(), T::ORDER).map_err(crate::Error::Allocation)?;
        let slot: Box<mem::MaybeUninit<Self>> =
            alloc::BoxExt::try_new(mem::MaybeUninit::uninit()).map_err(crate::Error::Exhausted)?;
        let input = Admitted {
            input: mem::replace(&mut input.input, input::Input::empty()),
        };
        let scan = Scanner::admitted(input, selection);
        Ok(Box::write(slot, Self::with_scan(scan, context)))
    }

    /// Builds directly from exact retained payload owners.
    #[doc(hidden)]
    pub fn try_scoped(segments: owned::Segments<'d>, context: C) -> Result<Self, Create> {
        let scan = Scanner::try_new(segments, T::fields(), T::ORDER)?;
        Ok(Self::with_scan(scan, context))
    }

    /// Transfers one retained transport owner into the decoder.
    pub fn push(mut self, segment: resident::Shared<'d>) -> Transfer<Self, Push<'d>> {
        let Some(phase) = self.phase.occupied() else {
            return Transfer::Rejected {
                state: self,
                error: Push::Complete(segment),
            };
        };
        let (phase, vacant) = phase.take();
        let (phase, error) = match phase {
            Phase::Scan(scanner) => match scanner.push(segment) {
                Transfer::Accepted(scanner) => (Phase::Scan(scanner), None),
                Transfer::Rejected { state, error } => (Phase::Scan(state), Some(error)),
            },
            phase => (phase, Some(Push::Complete(segment))),
        };
        vacant.insert(phase);
        match error {
            Some(error) => Transfer::Rejected { state: self, error },
            None => Transfer::Accepted(self),
        }
    }

    /// Declares that no more input segments can arrive.
    pub fn close(mut self) -> Self {
        if let Some(phase) = self.phase.occupied() {
            let (phase, vacant) = phase.take();
            vacant.insert(match phase {
                Phase::Scan(scanner) => Phase::Scan(scanner.close()),
                phase => phase,
            });
        }
        self
    }

    /// Returns original input bytes retired by scanning or scanner-error cleanup
    /// since the previous call. Materialization and cancellation retain the
    /// caller's remaining transport credit until its terminal settlement.
    pub fn take_bytes(&mut self) -> usize {
        mem::replace(&mut self.bytes, 0)
    }

    /// Returns original transport owners retired by scanning or scanner-error
    /// cleanup. Projected copies are never counted as new transport owners.
    pub fn take_chunks(&mut self) -> segments::Count {
        mem::replace(&mut self.chunks, segments::Count::EMPTY)
    }

    /// Cooperatively retires this decoder without materializing another value.
    pub fn release(&mut self, budget: &mut usize) -> Poll<()> {
        let Some(phase) = self.phase.occupied() else {
            return Poll::Ready(());
        };
        let (phase, vacant) = phase.take();
        let (mut release, error) = match phase {
            Phase::Scan(scanner) => (scanner.release(), None),
            Phase::Start(document) => (document.release(), None),
            Phase::Build(mut state) | Phase::ReleaseBuild(mut state) => {
                let done = Materialize::release(&mut state, budget);
                if done.is_pending() {
                    vacant.insert(Phase::ReleaseBuild(state));
                }
                return done;
            }
            Phase::Release { release, error } => (release, error),
        };
        let done = release.retire(budget);
        if done.is_pending() {
            vacant.insert(Phase::Release { release, error });
        }
        done
    }

    /// Performs bounded work while preserving whether the decoder needs
    /// scheduling or another transport segment.
    pub fn poll(&mut self, budget: NonZero<usize>) -> Progress<'d, T> {
        let Some(phase) = self.phase.occupied() else {
            return Progress::Pending;
        };
        let (phase, vacant) = phase.take();
        let (state, budget, vacant) = match phase {
            Phase::ReleaseBuild(mut state) => {
                let mut left = budget.get();
                if Materialize::release(&mut state, &mut left).is_pending() {
                    vacant.insert(Phase::ReleaseBuild(state));
                }
                return Progress::Pending;
            }
            Phase::Release { mut release, error } => {
                let mut left = budget.get();
                let (done, released) = release.drain(&mut left);
                // Every reported owner is a disjoint member of this decoder's
                // admitted input, whose total length and count already fit.
                (self.bytes, self.chunks) = released.add(self.bytes, self.chunks);
                if done.is_ready() {
                    return match error {
                        Some(error) => Progress::Failed(error),
                        None => Progress::Pending,
                    };
                }
                vacant.insert(Phase::Release { release, error });
                return Progress::Pending;
            }
            Phase::Build(state) => (state, budget, vacant),
            Phase::Start(document) => (T::start(document, self.context), budget, vacant),
            Phase::Scan(scanner) => {
                let (document, left, released) = {
                    let mut prepared;
                    let left = 'prepare: {
                        let (mut scan, closed) = match scanner {
                            Scanner::Open(scan) => (scan, false),
                            Scanner::Closed(scan) => (scan, true),
                            Scanner::Preparing(value) => {
                                prepared = value;
                                break 'prepare budget.get();
                            }
                        };
                        let (progress, left) = match scan.step(budget, closed) {
                            Ok(progress) => progress,
                            Err(error) => {
                                let released = scan.stream.drain();
                                let release = scan.release();
                                (self.bytes, self.chunks) = released.add(self.bytes, self.chunks);
                                vacant.insert(Phase::Release {
                                    release,
                                    error: Some(error.into()),
                                });
                                return Progress::Pending;
                            }
                        };
                        let progress = match progress {
                            ScanProgress::Ready => {
                                prepared = Prepared::new(scan);
                                break 'prepare left;
                            }
                            ScanProgress::NeedInput => Progress::NeedInput,
                            ScanProgress::Pending => Progress::Pending,
                        };
                        let released = scan.stream.drain();
                        vacant.insert(Phase::Scan(if closed {
                            Scanner::Closed(scan)
                        } else {
                            Scanner::Open(scan)
                        }));
                        (self.bytes, self.chunks) = released.add(self.bytes, self.chunks);
                        return progress;
                    };
                    let progress = match prepared.step(left) {
                        Ok(progress) => progress,
                        Err(error) => {
                            let released = prepared.stream.drain();
                            let release = prepared.release();
                            (self.bytes, self.chunks) = released.add(self.bytes, self.chunks);
                            vacant.insert(Phase::Release {
                                release,
                                error: Some(error.into()),
                            });
                            return Progress::Pending;
                        }
                    };
                    let released = prepared.stream.drain();
                    match progress {
                        Poll::Ready(left) => (prepared.document, left, released),
                        Poll::Pending => {
                            vacant.insert(Phase::Scan(Scanner::Preparing(prepared)));
                            (self.bytes, self.chunks) = released.add(self.bytes, self.chunks);
                            return Progress::Pending;
                        }
                    }
                };
                (self.bytes, self.chunks) = released.add(self.bytes, self.chunks);
                let Some(left) = NonZero::new(left) else {
                    vacant.insert(Phase::Start(document));
                    return Progress::Pending;
                };
                (T::start(document, self.context), left, vacant)
            }
        };
        match Materialize::step(state, budget) {
            Outcome::Ready(value) => Progress::Ready(value),
            Outcome::Pending(state) => {
                vacant.insert(Phase::Build(state));
                Progress::Pending
            }
            Outcome::Failed(error) => Progress::Failed(error),
        }
    }
}
