use dope::{
    manifold::network::listener::AcceptError,
    runtime::{
        executor::{drive, session},
        topology::{self, lending},
    },
};

use super::{Output, Pending};

/// The first listener failure, retaining any failure of the driving executor.
#[derive(Debug)]
pub enum ExecutionError {
    Accept {
        cause: AcceptError,
        drive: Option<drive::Error>,
    },
    Drive(drive::Error),
}

/// Exact installation, execution, and session-retirement failures.
#[derive(Debug)]
pub enum Failure<'report, I, T> {
    Install(I),
    Execute(ExecutionError),
    Run(session::Error<'report, Output<T>>),
}

impl<'report, I, T> Failure<'report, I, T> {
    /// Resolves the listener result after the session has retired its owners.
    pub fn session(result: session::Result<'report, Pending<'_, '_, T>>) -> Result<T, Self> {
        match result {
            Ok(output) => output.finish().map_err(Self::Execute),
            Err(error) => Err(Self::Run(match error {
                session::Error::Drive(error) => session::Error::Drive(error),
                session::Error::Install { cause, disarm } => {
                    session::Error::Install { cause, disarm }
                }
                session::Error::Quiesce {
                    output,
                    drive,
                    disarm,
                } => session::Error::Quiesce {
                    output: output.finish(),
                    drive,
                    disarm,
                },
            })),
        }
    }

    /// Resolves a dependent server only after its provider has also quiesced.
    pub fn lending(result: lending::Result<'report, I, Pending<'_, '_, T>>) -> Result<T, Self> {
        match result {
            Ok(output) => Self::session(Ok(output)),
            Err(lending::Error::Build(error)) => Err(Self::Install(error)),
            Err(lending::Error::Run(error)) => Self::session(Err(error)),
        }
    }

    pub fn map_install<J>(self, map: impl FnOnce(I) -> J) -> Failure<'report, J, T> {
        match self {
            Self::Install(error) => Failure::Install(map(error)),
            Self::Execute(error) => Failure::Execute(error),
            Self::Run(error) => Failure::Run(error),
        }
    }

    pub fn cause(&self) -> Cause<'_, I> {
        match self {
            Self::Install(error) => Cause::Install(error),
            Self::Execute(error) => Cause::Execute(error),
            Self::Run(error) => Cause::Run {
                cause: error.cause(),
                output: error.output().and_then(|output| output.as_ref().err()),
            },
        }
    }
}

#[derive(Debug)]
pub enum Cause<'a, I> {
    Install(&'a I),
    Execute(&'a ExecutionError),
    Run {
        cause: session::Cause<'a>,
        output: Option<&'a ExecutionError>,
    },
}

/// Retains rejected operations and closing topology owners unchanged.
pub enum RunError<'owner, S, I, O, T, Q = ()> {
    Failure(Failure<'owner, I, T>),
    Enter(topology::EnterError<'owner, S, O, Result<T, Failure<'owner, I, T>>, Q>),
}

impl<S, I, O, T, Q> RunError<'_, S, I, O, T, Q> {
    pub fn cause(&self) -> RunCause<'_, S, I> {
        match self {
            Self::Failure(error) => RunCause::Failure(error.cause()),
            Self::Enter(error) => RunCause::Enter(
                error
                    .kind()
                    .map(|output| output.as_ref().map(|_| ()).map_err(Failure::cause)),
            ),
        }
    }
}

impl<'owner, S, I, O, T, Q>
    From<topology::EnterError<'owner, S, O, Result<T, Failure<'owner, I, T>>, Q>>
    for RunError<'owner, S, I, O, T, Q>
{
    fn from(
        error: topology::EnterError<'owner, S, O, Result<T, Failure<'owner, I, T>>, Q>,
    ) -> Self {
        Self::Enter(error)
    }
}

#[derive(Debug)]
pub enum RunCause<'a, S, I> {
    Failure(Cause<'a, I>),
    Enter(topology::error::Kind<'a, S, Result<(), Cause<'a, I>>>),
}
