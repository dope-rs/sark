use o3::depot::map::direct;
use sark_core::{
    body::{self, buffered, credit, flow},
    manifold::task::app::Runtime,
    route,
};

use crate::{conn, frame::numbers, server::payload, stream};

pub(in crate::server::task) fn reject(
    slot: direct::Error<stream::Key>,
    released: Result<(), payload::Error>,
) -> conn::Error {
    match released {
        Ok(()) => conn::Error::Incoming(slot),
        Err(payload::Error::Body(error)) => conn::Error::IncomingBody { slot, error },
        Err(payload::Error::Segments(error)) => conn::Error::IncomingSegments { slot, error },
    }
}

/// Exact failure produced while pushing input into one task request body.
pub(in crate::server::task) enum Push<E> {
    Payload(payload::Error),
    Stream(buffered::Transition<E>),
    Input(body::Error),
    Build(E),
    StreamRelease {
        error: buffered::Transition<E>,
        release: payload::Error,
    },
}

/// Exact failure produced while advancing one task request builder.
pub(in crate::server::task) enum Build<E> {
    Step(flow::Error<E>),
    Admit(body::Error),
    AdmitRelease {
        error: body::Error,
        release: credit::Error,
    },
}

impl<E> Push<E> {
    pub(in crate::server::task) fn code<'d, R>(self) -> numbers::ErrorCode
    where
        R: Runtime<'d>,
        R::Build: body::Push<'d, Error = E>,
    {
        match self {
            Self::Payload(error) => error.code(),
            Self::Input(error) | Self::Stream(buffered::Transition::Body(error)) => {
                payload::Error::Body(error).code()
            }
            Self::Build(error) | Self::Stream(buffered::Transition::Build(error)) => {
                reject_code(R::reject_push(error))
            }
            Self::StreamRelease { .. } => numbers::ErrorCode::InternalError,
        }
    }
}

impl<E> Build<E> {
    pub(super) fn rejected(error: body::Error, release: Result<(), credit::Error>) -> Self {
        match release {
            Ok(()) => Self::Admit(error),
            Err(release) => Self::AdmitRelease { error, release },
        }
    }

    pub(in crate::server::task) fn code<'d, R>(self) -> numbers::ErrorCode
    where
        R: Runtime<'d>,
        R::Build: body::Build<'d, StepError = E>,
    {
        match self {
            Self::Step(flow::Error::LengthMismatch) => numbers::ErrorCode::ProtocolError,
            Self::Step(flow::Error::Credit(error)) => {
                payload::Error::Body(body::Error::Credit(error)).code()
            }
            Self::Step(flow::Error::Build(error)) => reject_code(R::reject_step(error)),
            Self::Step(flow::Error::Release { .. }) | Self::AdmitRelease { .. } => {
                numbers::ErrorCode::InternalError
            }
            Self::Admit(error) => payload::Error::Body(error).code(),
        }
    }
}

pub(in crate::server::task) fn reject_code(error: route::Reject) -> numbers::ErrorCode {
    match error {
        route::Reject::BadRequest => numbers::ErrorCode::ProtocolError,
        route::Reject::Internal => numbers::ErrorCode::InternalError,
        route::Reject::Unavailable => numbers::ErrorCode::RefusedStream,
    }
}
