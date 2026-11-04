use std::{num::NonZero, time};

use dope::{
    core::driver::result::SubmitCause,
    manifold::client::{close::LocalOrigin, conn::Transmit, port::error::Open},
    net::link::event,
};
use dope_fiber::task::time::budget;
use sark_http::method::Method;

use crate::{Error, Failure, Safety, connect, timeout};

#[derive(Clone, Copy)]
enum Class {
    Never,
    Stale,
    Transient,
}

impl Class {
    fn of<E, M>(error: &Error<E, M>) -> Self {
        match error {
            Error::NotConnected => Self::Stale,
            Error::Transport(crate::conn::Error::Park(event::ParkError::Submit(error)))
            | Error::Connect(connect::Error::Attempt(event::ConnectFault::Transport(
                event::ConnectError::Admission(error),
            ))) if error.cause() == SubmitCause::IdentityExhausted => Self::Never,
            Error::Transport(crate::conn::Error::Transmit {
                error: Transmit::Rejected(failure),
                ..
            }) if failure.into_error().cause() == SubmitCause::IdentityExhausted => Self::Never,
            Error::Transport(error) => match error.kind() {
                crate::conn::Kind::RemoteEof
                | crate::conn::Kind::Receive
                | crate::conn::Kind::Transport
                | crate::conn::Kind::Wire
                | crate::conn::Kind::EndpointRetired
                | crate::conn::Kind::Local(
                    LocalOrigin::Superseded
                    | LocalOrigin::ScheduledRotation
                    | LocalOrigin::ResourceFailure,
                ) => Self::Stale,
                crate::conn::Kind::Timeout(_) => Self::Transient,
                crate::conn::Kind::Protocol
                | crate::conn::Kind::Local(
                    LocalOrigin::ApiRequest
                    | LocalOrigin::SideComplete
                    | LocalOrigin::OperationCancelled
                    | LocalOrigin::RuntimeShutdown,
                ) => Self::Never,
            },
            Error::Connect(
                connect::Error::Resolve(_)
                | connect::Error::IdentityExhausted
                | connect::Error::Activation(Open::Exhausted)
                | connect::Error::Mailbox(_)
                | connect::Error::Reply(_)
                | connect::Error::Attempt(
                    event::ConnectFault::Attempt(_) | event::ConnectFault::Cleanup { .. },
                ),
            ) => Self::Never,
            Error::Timeout(error) if error.phase() == timeout::Phase::Dispatch => Self::Never,
            Error::Timeout(error) => match error.elapsed() {
                budget::Elapsed::Budget => Self::Never,
                budget::Elapsed::Limit => Self::Transient,
            },
            Error::Connect(_) => Self::Transient,
            _ => Self::Never,
        }
    }
}

#[derive(Clone, Copy)]
pub struct Policy {
    /// Total attempts allowed for methods defined as idempotent by HTTP semantics.
    pub idempotent_attempts: NonZero<u8>,
    /// Total attempts allowed for non-idempotent methods when retry safety admission.
    pub non_idempotent_attempts: NonZero<u8>,
    /// Allows stale-connection recovery only when failure occurred before submission.
    ///
    /// This flag cannot authorize replay after a request entered egress.
    pub reconnect_on_stale_non_idempotent: bool,
}

#[doc(hidden)]
pub struct State {
    reconnect_on_stale_non_idempotent: bool,
    mode: Mode,
    attempts: NonZero<u8>,
    limit: NonZero<u8>,
}

#[derive(Clone, Copy)]
enum Mode {
    IdempotentBefore,
    IdempotentCommitted,
    NonIdempotentBefore,
    NonIdempotentCommitted,
}

impl Mode {
    fn safety(self) -> Safety {
        match self {
            Self::IdempotentBefore | Self::NonIdempotentBefore => Safety::BeforeSubmit,
            Self::IdempotentCommitted | Self::NonIdempotentCommitted => Safety::PossiblyCommitted,
        }
    }

    fn observe(&mut self, safety: Safety) {
        if safety == Safety::BeforeSubmit {
            return;
        }
        *self = match *self {
            Self::IdempotentBefore | Self::IdempotentCommitted => Self::IdempotentCommitted,
            Self::NonIdempotentBefore | Self::NonIdempotentCommitted => {
                Self::NonIdempotentCommitted
            }
        };
    }
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            idempotent_attempts: NonZero::<u8>::MIN.saturating_add(2),
            non_idempotent_attempts: NonZero::<u8>::MIN.saturating_add(1),
            reconnect_on_stale_non_idempotent: true,
        }
    }
}

/// One admitted backoff, retaining the last attempt until the next begins.
pub struct Backoff {
    state: State,
    next: NonZero<u8>,
}

impl State {
    pub const fn attempt(&self) -> NonZero<u8> {
        self.attempts
    }

    /// Enters the first dispatch attempt, including its deadline admission.
    pub fn new(policy: Policy, method: &Method) -> Self {
        let (mode, limit) = match *method {
            Method::GET
            | Method::HEAD
            | Method::OPTIONS
            | Method::TRACE
            | Method::PUT
            | Method::DELETE => (Mode::IdempotentBefore, policy.idempotent_attempts),
            _ => (Mode::NonIdempotentBefore, policy.non_idempotent_attempts),
        };
        Self {
            reconnect_on_stale_non_idempotent: policy.reconnect_on_stale_non_idempotent,
            mode,
            attempts: NonZero::<u8>::MIN,
            limit,
        }
    }

    pub fn with_prior(mut self, safety: Safety) -> Self {
        self.mode.observe(safety);
        self
    }

    pub fn finish<E, M>(&self, failure: Failure<E, M>) -> Failure<E, M> {
        let (cause, safety) = failure.into_parts();
        Failure::new(cause, safety.merge(self.mode.safety())).attempted(self.attempts)
    }

    pub fn retry_after<E, M>(mut self, failure: &Failure<E, M>) -> Result<Backoff, Self> {
        self.mode.observe(failure.safety());
        if self.attempts >= self.limit {
            return Err(self);
        }
        let next = self.attempts.saturating_add(1);
        let allowed = match self.mode {
            Mode::IdempotentBefore | Mode::IdempotentCommitted => {
                !matches!(Class::of(failure.cause()), Class::Never)
            }
            Mode::NonIdempotentCommitted => false,
            Mode::NonIdempotentBefore => match Class::of(failure.cause()) {
                Class::Transient => matches!(failure.cause(), Error::Connect(_)),
                Class::Stale => self.reconnect_on_stale_non_idempotent,
                Class::Never => false,
            },
        };
        if !allowed {
            return Err(self);
        }
        Ok(Backoff { state: self, next })
    }
}

impl Backoff {
    pub fn delay(&self) -> time::Duration {
        time::Duration::from_millis(25 * u64::from(self.state.attempts.get()))
    }

    /// Enters the admitted dispatch attempt after its backoff completed.
    pub fn start(self) -> State {
        State {
            attempts: self.next,
            ..self.state
        }
    }

    pub fn finish<E, M>(&self, failure: Failure<E, M>) -> Failure<E, M> {
        self.state.finish(failure)
    }
}
