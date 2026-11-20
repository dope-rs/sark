use core::ops::ControlFlow;

use dope::core::BranchError;
use o3::buffer::{resident, segments};
use sark_core::decode::{self, request, state};
use sark_http::field::metadata::section::rules;

use crate::{
    conn::{self, event},
    frame::numbers,
    server::{
        payload::Buffer,
        response::{self, Encoder, Handoff},
        state::exchange::{Accepted, Input, Rejected},
    },
};

fn resolve_body<'d, T, S>(
    outcome: ControlFlow<T, S>,
    body: Buffer<'d>,
) -> ControlFlow<(T, Rejected<'d>), (S, Buffer<'d>)> {
    match outcome {
        ControlFlow::Continue(slot) => ControlFlow::Continue((slot, body)),
        ControlFlow::Break(stop) => ControlFlow::Break((stop, Rejected::Body(body))),
    }
}

impl<'d, F: Input<'d> + state::admit::Bodies> state::admit::Resolve<F> for Accepted<'d> {
    type Stopped<T> = (T, Rejected<'d>);

    fn open<T, S>(
        outcome: ControlFlow<T, S>,
        body: F::SyncOpen,
    ) -> ControlFlow<Self::Stopped<T>, (S, Buffer<'d>)> {
        resolve_body(outcome, body.into())
    }

    fn finished<T, S>(
        outcome: ControlFlow<T, S>,
        body: F::SyncFinished,
    ) -> ControlFlow<Self::Stopped<T>, (S, Buffer<'d>)> {
        resolve_body(outcome, body.into())
    }

    fn duplex<T, S>(
        outcome: ControlFlow<T, S>,
        body: F::DuplexOpen,
    ) -> ControlFlow<Self::Stopped<T>, (S, Option<segments::Chain<resident::Shared<'d>>>)> {
        match outcome {
            ControlFlow::Continue(slot) => ControlFlow::Continue((slot, F::duplex(body))),
            ControlFlow::Break(stop) => {
                drop(body);
                ControlFlow::Break((stop, Rejected::Duplex))
            }
        }
    }
}

type Outcome<'d, P> = ControlFlow<
    (
        ControlFlow<response::Head<'d>, response::Receipt<'d>>,
        Rejected<'d>,
    ),
    <<P as request::Prepared>::Family as state::Family<P>>::State<Accepted<'d>>,
>;

pub(in crate::server) fn run<'req, 'd, R, F, M, E, P>(
    app: &R,
    routed: request::Guarded<<R::Routes as state::Family<R::Prepared>>::State<F>>,
    encoder: Encoder<'_, 'req, 'd, P, E>,
) -> Result<
    Outcome<'d, R::Prepared>,
    (
        BranchError<conn::Error, numbers::ErrorCode>,
        crate::stream::Key,
        Rejected<'d>,
    ),
>
where
    R: decode::Guarded,
    F: Input<'d> + state::admit::Bodies,
    M: Handoff,
    E: event::Evidence,
    P: rules::Policy,
{
    match app.try_admit::<_, _, Accepted<'d>>(routed, encoder) {
        ControlFlow::Continue(admitted) => Ok(ControlFlow::Continue(admitted)),
        ControlFlow::Break((outcome, input)) => match outcome {
            Ok(response) => {
                let stream = response.stream();
                match response.defer::<M>() {
                    Ok(response) => Ok(ControlFlow::Break((response, input))),
                    Err(error) => Err((BranchError::First(error), stream, input)),
                }
            }
            Err((error, stream)) => {
                use decode::dispatch::Error;

                let error = match error {
                    Error::Busy | Error::Sequence(_) => {
                        BranchError::Second(numbers::ErrorCode::RefusedStream)
                    }
                    Error::Encode(error) => BranchError::First(error),
                    Error::Pending | Error::Moved | Error::Unsupported => {
                        BranchError::Second(numbers::ErrorCode::InternalError)
                    }
                    Error::Invalid(never) | Error::Body(never) => match never {},
                };
                Err((error, stream, input))
            }
        },
    }
}
