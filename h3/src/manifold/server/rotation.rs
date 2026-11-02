//! HTTP/3 drain mechanics for the shared session lifetime policy.
use dope::core::driver::schedule::clock;
use dope_quic::wire::stream::Id;
use o3::base::time::Positive;
use sark_core::life::session;
use sark_http::field::metadata::section::rules;

use crate::{conn, conn::io};

/// Exact failure to arm a connection's finite lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error<'d> {
    Age { source: clock::Range<Positive> },
    Open { deadline: clock::Deadline<'d> },
    Draining { deadline: clock::Deadline<'d> },
}

/// Exact failure to admit a request after the drain window begins.
#[derive(Debug)]
pub enum Admission<'d> {
    Draining { deadline: clock::Deadline<'d> },
}

/// Exact failure while entering the finite drain window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Failure {
    Deadline { source: clock::Range<Positive> },
    GoAway(conn::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expired {
    Draining,
    Closing,
}

/// Connection-local rotation state.
pub trait State<'d> {
    type Policy: ?Sized;

    fn accepting(&self) -> Result<(), Admission<'d>>;

    fn admit(&mut self, id: Id) -> Result<(), Admission<'d>>;

    fn try_open(&mut self, policy: &Self::Policy, now: clock::Moment<'d>) -> Result<(), Error<'d>>;

    fn deadline(&self) -> Option<clock::Deadline<'d>>;

    fn expire<P: rules::Policy>(
        &mut self,
        policy: &Self::Policy,
        h3: &mut crate::manifold::Session<'_, P>,
        now: clock::Moment<'d>,
    ) -> Result<Option<Expired>, Failure>;
}

#[derive(Clone, Copy, Default)]
pub struct Inactive;

impl<'d> State<'d> for Inactive {
    type Policy = session::Disabled;

    fn accepting(&self) -> Result<(), Admission<'d>> {
        Ok(())
    }

    fn admit(&mut self, _: Id) -> Result<(), Admission<'d>> {
        Ok(())
    }

    fn try_open(&mut self, _: &Self::Policy, _: clock::Moment<'d>) -> Result<(), Error<'d>> {
        Ok(())
    }

    fn deadline(&self) -> Option<clock::Deadline<'d>> {
        None
    }

    fn expire<P: rules::Policy>(
        &mut self,
        _: &Self::Policy,
        _: &mut crate::manifold::Session<'_, P>,
        _: clock::Moment<'d>,
    ) -> Result<Option<Expired>, Failure> {
        Ok(None)
    }
}

/// Compile-time H3 session policy.
pub trait Policy<'d> {
    type Drain: State<'d, Policy = Self>;

    fn drain() -> Self::Drain;
}

impl<'d> Policy<'d> for session::Disabled {
    type Drain = Inactive;

    fn drain() -> Self::Drain {
        Inactive
    }
}

#[derive(Clone, Copy, Default)]
enum Phase<'d> {
    #[default]
    Pending,
    Open(clock::Deadline<'d>),
    Draining(clock::Deadline<'d>),
    Closing(clock::Deadline<'d>),
}

/// Connection-local state present only in the finite-rotation specialization.
/// Exhausted request IDs drain without GOAWAY, as allowed by RFC 9114 section 5.2.
#[derive(Default)]
pub struct Drain<'d> {
    admission_limit: u64,
    phase: Phase<'d>,
}

impl<'d> State<'d> for Drain<'d> {
    type Policy = session::Config;

    fn accepting(&self) -> Result<(), Admission<'d>> {
        match self.phase {
            Phase::Draining(deadline) | Phase::Closing(deadline) => {
                Err(Admission::Draining { deadline })
            }
            Phase::Pending | Phase::Open(_) => Ok(()),
        }
    }

    fn admit(&mut self, id: Id) -> Result<(), Admission<'d>> {
        self.accepting()?;
        let next = id.get() + 4;
        self.admission_limit = self.admission_limit.max(next);
        Ok(())
    }

    fn try_open(&mut self, policy: &Self::Policy, now: clock::Moment<'d>) -> Result<(), Error<'d>> {
        match self.phase {
            Phase::Pending => {}
            Phase::Open(deadline) => return Err(Error::Open { deadline }),
            Phase::Draining(deadline) | Phase::Closing(deadline) => {
                return Err(Error::Draining { deadline });
            }
        }
        self.phase = Phase::Open(
            now.try_add(policy.age())
                .map_err(|source| Error::Age { source })?,
        );
        Ok(())
    }

    fn deadline(&self) -> Option<clock::Deadline<'d>> {
        match self.phase {
            Phase::Pending | Phase::Closing(_) => None,
            Phase::Open(deadline) | Phase::Draining(deadline) => Some(deadline),
        }
    }

    fn expire<P: rules::Policy>(
        &mut self,
        policy: &Self::Policy,
        h3: &mut crate::manifold::Session<'_, P>,
        now: clock::Moment<'d>,
    ) -> Result<Option<Expired>, Failure> {
        match self.phase {
            Phase::Pending | Phase::Closing(_) => Ok(None),
            Phase::Open(deadline) if !deadline.is_due(now) => Ok(None),
            Phase::Open(_) => {
                let deadline = now
                    .try_add(policy.drain())
                    .map_err(|source| Failure::Deadline { source })?;
                if self.admission_limit < 1 << 62 {
                    io::Send::new(h3.h3_mut())
                        .goaway(self.admission_limit)
                        .map_err(Failure::GoAway)?;
                }
                self.phase = Phase::Draining(deadline);
                Ok(Some(Expired::Draining))
            }
            Phase::Draining(deadline) if !deadline.is_due(now) => Ok(None),
            Phase::Draining(deadline) => {
                self.phase = Phase::Closing(deadline);
                Ok(Some(Expired::Closing))
            }
        }
    }
}

impl<'d> Policy<'d> for session::Config {
    type Drain = Drain<'d>;

    fn drain() -> Self::Drain {
        Drain::default()
    }
}
