//! Affine duplex transition machine.
use core::marker;

use o3::buffer::{resident, segments};
use sark_http::payload::owned;

use crate::duplex::{self, admit};

const INPUT_FINISH_PENDING: u8 = 1;
const INPUT_FINISH_HELD: u8 = 1 << 1;
const INITIAL_ACTIVATION: u8 = 1 << 2;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Phase {
    Idle,
    Turn,
    Wait,
    Activate,
    FinishOutput,
    FinishSession,
    QueuedWait,
    QueuedActivate,
    QueuedFinishOutput,
    QueuedFinishSession,
    Finished,
}

impl Phase {
    const fn action(self) -> Option<Staged> {
        let (dir, resume) = match self {
            Self::Wait => (duplex::Directive::Wait, Resume::Idle),
            Self::Activate => (duplex::Directive::Activate, Resume::Idle),
            Self::FinishOutput => (duplex::Directive::FinishOutput, Resume::Idle),
            Self::FinishSession => (duplex::Directive::FinishSession, Resume::Idle),
            Self::QueuedWait => (duplex::Directive::Wait, Resume::Turn),
            Self::QueuedActivate => (duplex::Directive::Activate, Resume::Turn),
            Self::QueuedFinishOutput => (duplex::Directive::FinishOutput, Resume::Turn),
            Self::QueuedFinishSession => (duplex::Directive::FinishSession, Resume::Turn),
            Self::Idle | Self::Turn | Self::Finished => return None,
        };
        Some(Staged {
            directive: dir,
            resume,
        })
    }

    const fn from_action(dir: duplex::Directive, resume: Resume) -> Self {
        match (dir, resume) {
            (duplex::Directive::Wait, Resume::Idle) => Self::Wait,
            (duplex::Directive::Activate, Resume::Idle) => Self::Activate,
            (duplex::Directive::FinishOutput, Resume::Idle) => Self::FinishOutput,
            (duplex::Directive::FinishSession, Resume::Idle) => Self::FinishSession,
            (duplex::Directive::Wait, Resume::Turn) => Self::QueuedWait,
            (duplex::Directive::Activate, Resume::Turn) => Self::QueuedActivate,
            (duplex::Directive::FinishOutput, Resume::Turn) => Self::QueuedFinishOutput,
            (duplex::Directive::FinishSession, Resume::Turn) => Self::QueuedFinishSession,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum Resume {
    Idle,
    Turn,
}

#[derive(Clone, Copy)]
struct Staged {
    directive: duplex::Directive,
    resume: Resume,
}

/// Exact disposition of the session output half.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Output {
    Open,
    Finished,
}

#[derive(Clone, Copy)]
struct Next {
    settled: After,
    flags: u8,
    output: Output,
}

impl Next {
    const fn after(&self) -> After {
        match self.settled {
            After::Finished => After::Finished,
            After::Turn => After::Turn,
            After::Idle if self.flags & INPUT_FINISH_PENDING != 0 => After::Turn,
            After::Idle => After::Idle,
        }
    }

    const fn reduce(mut flags: u8, mut output: Output, staged: Staged) -> Self {
        let settled = match staged.directive {
            duplex::Directive::Wait => match staged.resume {
                Resume::Idle => After::Idle,
                Resume::Turn => After::Turn,
            },
            duplex::Directive::Activate => After::Turn,
            duplex::Directive::FinishOutput => {
                output = Output::Finished;
                if flags == INPUT_FINISH_HELD {
                    flags = 0;
                    After::Finished
                } else {
                    match staged.resume {
                        Resume::Idle => After::Idle,
                        Resume::Turn => After::Turn,
                    }
                }
            }
            duplex::Directive::FinishSession => {
                flags = 0;
                output = Output::Finished;
                After::Finished
            }
        };
        Self {
            settled,
            flags,
            output,
        }
    }
}

/// Exact session owner with one mutually exclusive input/output slot.
pub struct Machine<'d, D: duplex::Session<'d>> {
    /// Released before the session that produced it.
    pending: Option<segments::Nonempty<resident::Shared<'d>>>,
    session: D,
    phase: Phase,
    flags: u8,
    output: Output,
}

/// Exclusive vacant input slot bound to one machine borrow.
#[must_use = "commit the input owner or keep it at the carrier boundary"]
pub struct Input<'a, 'd, D: duplex::Session<'d>>(&'a mut Machine<'d, D>);
/// Exact availability of the session's one callback input slot.
pub enum Access<T> {
    Vacant(T),
    Blocked,
}
/// Exact current progress of the session machine.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Progress {
    Idle,
    Turn,
    Finished,
}
/// Exact retention of an observed orderly input finish.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Hold {
    Released,
    Held,
}
impl<'d, D: duplex::Session<'d>> Input<'_, 'd, D> {
    pub fn commit(self, input: owned::Segments<'d>) {
        if let Some(input) = input.take() {
            self.0.pending = Some(input);
        }
    }
}
/// Validated orderly-input finish bound to one machine borrow.
#[must_use = "commit the validated input finish"]
pub struct Finish<'a, 'd, D: duplex::Session<'d>>(&'a mut Machine<'d, D>);

impl<'d, D: duplex::Session<'d>> Finish<'_, 'd, D> {
    pub fn commit(self) {
        self.0.flags |= INPUT_FINISH_PENDING | INPUT_FINISH_HELD;
    }
}

/// Transport disposition causally adjacent to one admitted output.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum End {
    Open,
    FinishOutput,
    FinishSession,
}

/// Projected state produced by the same reducer as the physical commit.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum After {
    Idle,
    Turn,
    Finished,
}

/// Protocol-neutral result of one machine transaction.
pub enum Transaction<R, E> {
    Idle,
    Committed { receipt: R, after: After },
    Pending { receipt: R },
    Rejected(E),
    Violation(Violation),
}

/// A session/carrier contract violation detected at the canonical boundary.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Violation {
    OutputAfterFinish,
}

/// Rejection of one orderly peer-input terminal transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FinishError {
    Finished,
    Closed,
}

impl<'d, D: duplex::Session<'d>> Machine<'d, D> {
    pub fn new(inner: D) -> Self {
        Self {
            session: inner,
            pending: None,
            phase: Phase::Turn,
            flags: INITIAL_ACTIVATION,
            output: Output::Open,
        }
    }

    pub(super) fn with_finish(inner: D) -> Self {
        Self {
            flags: INPUT_FINISH_PENDING | INPUT_FINISH_HELD | INITIAL_ACTIVATION,
            ..Self::new(inner)
        }
    }

    /// Validates the one callback slot and binds vacancy to this borrow.
    pub fn input(&mut self) -> Access<Input<'_, 'd, D>> {
        match self.access() {
            Access::Vacant(()) => Access::Vacant(Input(self)),
            Access::Blocked => Access::Blocked,
        }
    }

    pub const fn access(&self) -> Access<()> {
        match (&self.pending, self.phase) {
            (None, Phase::Idle | Phase::Turn) => Access::Vacant(()),
            (None | Some(_), _) => Access::Blocked,
        }
    }

    pub const fn progress(&self) -> Progress {
        if self.pending.is_some() || self.flags & INPUT_FINISH_PENDING != 0 {
            return Progress::Turn;
        }
        match self.phase {
            Phase::Idle => Progress::Idle,
            Phase::Turn
            | Phase::Wait
            | Phase::Activate
            | Phase::FinishOutput
            | Phase::FinishSession
            | Phase::QueuedWait
            | Phase::QueuedActivate
            | Phase::QueuedFinishOutput
            | Phase::QueuedFinishSession => Progress::Turn,
            Phase::Finished => Progress::Finished,
        }
    }

    pub const fn output(&self) -> &Output {
        &self.output
    }

    pub const fn hold(&self) -> Hold {
        if self.flags & INPUT_FINISH_HELD == 0 {
            Hold::Released
        } else {
            Hold::Held
        }
    }

    /// Binds an orderly-input finish while preserving the caller's exact owner
    /// on both transition outcomes.
    pub fn finish<I>(&mut self, owner: I) -> Result<(Finish<'_, 'd, D>, I), (FinishError, I)> {
        match self.finish_error() {
            Some(error) => Err((error, owner)),
            None => Ok((Finish(self), owner)),
        }
    }

    /// Joins at most one session callback to one native admission attempt.
    pub fn transact<R, E>(
        &mut self,
        admit: impl for<'turn> FnOnce(
            admit::Emission<'turn, 'd, D>,
        ) -> admit::Admission<'turn, 'd, D, R, E>,
    ) -> Transaction<R, E> {
        let staged = match self.phase.action() {
            Some(staged) => staged,
            None => match self.stage() {
                Some(staged) => staged,
                None => return Transaction::Idle,
            },
        };
        if matches!(self.output, Output::Finished) && self.pending.is_some() {
            self.terminate();
            return Transaction::Violation(Violation::OutputAfterFinish);
        }
        let end = match staged.directive {
            duplex::Directive::Wait | duplex::Directive::Activate => End::Open,
            duplex::Directive::FinishOutput => End::FinishOutput,
            duplex::Directive::FinishSession => End::FinishSession,
        };
        let next = Next::reduce(self.flags, self.output, staged);
        let after = next.after();
        let payload = match self.pending.take() {
            Some(output) => admit::Payload::Present {
                output,
                permit: admit::Permit {
                    owner: marker::PhantomData,
                    mode: marker::PhantomData,
                },
            },
            None => admit::Payload::Vacant(admit::Permit {
                owner: marker::PhantomData,
                mode: marker::PhantomData,
            }),
        };
        let emit = admit::Emission {
            payload,
            end,
            after,
        };

        let admit::Admission {
            decision,
            owner: marker::PhantomData,
        } = admit(emit);
        match decision {
            admit::Decision::Committed { receipt: proof } => {
                self.phase = match next.settled {
                    After::Idle => Phase::Idle,
                    After::Turn => Phase::Turn,
                    After::Finished => Phase::Finished,
                };
                self.flags = next.flags;
                self.output = next.output;
                Transaction::Committed {
                    receipt: proof,
                    after,
                }
            }
            admit::Decision::Pending {
                remainder: rest,
                receipt: proof,
            } => {
                self.pending = Some(rest);
                Transaction::Pending { receipt: proof }
            }
            admit::Decision::Blocked { receipt: proof } => Transaction::Pending { receipt: proof },
            admit::Decision::Rejected { error } => {
                self.terminate();
                Transaction::Rejected(error)
            }
            admit::Decision::Returned {
                error,
                remainder: rest,
            } => {
                self.pending = Some(rest);
                self.terminate();
                Transaction::Rejected(error)
            }
        }
    }

    fn stage(&mut self) -> Option<Staged> {
        let pending = self.pending.is_some();
        let turn = matches!(self.phase, Phase::Turn);
        let finish = self.flags & INPUT_FINISH_PENDING != 0;
        if !pending && !turn && !finish {
            return None;
        }
        // Only initial activation precedes FIN; later activation remains queued.
        let activate = turn && (!finish || self.flags & INITIAL_ACTIVATION != 0);
        let resume = if turn && (pending || !activate) {
            Resume::Turn
        } else {
            Resume::Idle
        };
        let action = if let Some(input) = self.pending.take() {
            self.session.receive(owned::Segments::from(input))
        } else if activate {
            self.flags &= !INITIAL_ACTIVATION;
            self.session.activate()
        } else {
            self.flags &= !INPUT_FINISH_PENDING;
            self.session.finish()
        };
        let (output, dir) = action.into_parts();
        self.pending = output.and_then(owned::Segments::take);
        self.phase = Phase::from_action(dir, resume);
        Some(Staged {
            directive: dir,
            resume,
        })
    }

    fn terminate(&mut self) {
        self.pending = None;
        self.phase = Phase::Finished;
        self.flags = 0;
        self.output = Output::Finished;
    }

    fn finish_error(&self) -> Option<FinishError> {
        match self.progress() {
            Progress::Finished => Some(FinishError::Closed),
            Progress::Idle | Progress::Turn
                if self.flags & (INPUT_FINISH_PENDING | INPUT_FINISH_HELD) != 0 =>
            {
                Some(FinishError::Finished)
            }
            Progress::Idle | Progress::Turn => None,
        }
    }

    /// Retires the session once at the adapter's observable close boundary.
    pub fn retire(&mut self) {
        if matches!(self.phase, Phase::Finished) {
            return;
        }
        self.terminate();
    }
}

const _: () = {
    assert!(core::mem::size_of::<Resume>() == core::mem::size_of::<bool>());
    assert!(core::mem::align_of::<Resume>() == core::mem::align_of::<bool>());
    assert!(core::mem::size_of::<Next>() == 3);
    assert!(core::mem::align_of::<Next>() == 1);
    assert!(core::mem::size_of::<After>() == core::mem::size_of::<u8>());
    assert!(core::mem::align_of::<After>() == core::mem::align_of::<u8>());
    assert!(core::mem::size_of::<Output>() == core::mem::size_of::<u8>());
    assert!(core::mem::align_of::<Output>() == core::mem::align_of::<u8>());
    assert!(
        core::mem::size_of::<Machine<'static, duplex::Never>>()
            == core::mem::size_of::<Option<segments::Nonempty<resident::Shared<'static>>>>()
                + core::mem::size_of::<usize>()
    );
    assert!(
        core::mem::size_of::<Input<'static, 'static, duplex::Never>>()
            == core::mem::size_of::<usize>()
    );
    assert!(
        core::mem::size_of::<Finish<'static, 'static, duplex::Never>>()
            == core::mem::size_of::<usize>()
    );
};
