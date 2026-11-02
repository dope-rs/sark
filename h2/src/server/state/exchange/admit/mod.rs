use core::ops::ControlFlow;

use sark_core::decode::{self, request, state};

use crate::{
    conn::event,
    frame::numbers,
    server::{
        response::{self, Encoder, Handoff},
        state::exchange::{Accepted, Input, Rejected},
    },
};

mod guard;
mod plain;

pub(crate) use guard::Guard;
pub(crate) use plain::Plain;

type Outcome<'d, P> = ControlFlow<
    (
        ControlFlow<response::Head<'d>, response::Receipt<'d>>,
        Rejected<'d>,
    ),
    <<P as request::Prepared>::Family as state::Family<P>>::State<Accepted<'d>>,
>;

pub(in crate::server) fn run<'req, 'd, R, F, M, E>(
    app: &R,
    routed: request::Guarded<<R::Routes as state::Family<R::Prepared>>::State<F>>,
    encoder: Encoder<'_, 'req, 'd, R::Plan, E>,
) -> Result<Outcome<'d, R::Prepared>, (numbers::ErrorCode, crate::stream::Key, Rejected<'d>)>
where
    R: decode::Guarded,
    F: Input<'d>,
    M: Handoff,
    E: event::Evidence,
{
    let mut guard = Guard::<'d>::new();
    match decode::Guarded::try_admit(app, routed, encoder, &mut guard) {
        ControlFlow::Continue(admitted) => Ok(ControlFlow::Continue(admitted)),
        ControlFlow::Break((outcome, input)) => match outcome {
            Ok(response) => {
                let stream = response.stream();
                match response.defer::<M>() {
                    Ok(response) => Ok(ControlFlow::Break((response, input))),
                    Err(error) => Err((error.code(), stream, input)),
                }
            }
            Err((error, stream)) => {
                use decode::dispatch::Error;

                let code = match error {
                    Error::Busy | Error::Sequence(_) => numbers::ErrorCode::RefusedStream,
                    Error::Encode(error) => error.code(),
                    Error::Pending | Error::Moved | Error::Unsupported => {
                        numbers::ErrorCode::InternalError
                    }
                    Error::Invalid(never) | Error::Body(never) => match never {},
                };
                Err((code, stream, input))
            }
        },
    }
}
