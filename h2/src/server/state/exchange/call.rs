use core::ops;

use dope::core::BranchError;
use o3::buffer::resident;
use sark_core::decode::{self, exec, request};
use sark_http::{field::metadata::section::rules, payload::owned, request::body::Error};

use crate::{
    conn::{event, machine},
    frame::numbers,
    server::response::{self, Encoder, Handoff},
    stream,
};

pub(in crate::server) fn try_dispatch<'d, R, M, E, F>(
    ready: request::Ready<exec::Sync, R::Prepared, owned::Segments<'d>>,
    router: &R,
    conn: &mut machine::Connection<
        crate::ServerRole,
        F,
        resident::Shared<'d>,
        rules::RejectPush,
        E,
    >,
    stream: stream::Key,
) -> Result<
    ops::ControlFlow<response::Head<'d>, response::Receipt<'d>>,
    (
        BranchError<crate::conn::Error, numbers::ErrorCode>,
        stream::Key,
    ),
>
where
    R: decode::Decode,
    M: Handoff,
    E: event::Evidence,
    F: rules::Policy,
{
    ready.with_input(|prepared, input| {
        let (outcome, owner) = router
            .try_project(prepared, input, |scoped| {
                let (request, slot) = scoped.into_parts();
                let encoder = Encoder::selected(conn, stream, slot);
                let prepared = match router.try_dispatch(request, encoder) {
                    Ok(prepared) => prepared,
                    Err((error, failed)) => {
                        use decode::dispatch::Error;

                        let error = match error {
                            Error::Busy | Error::Sequence(_) => {
                                BranchError::Second(numbers::ErrorCode::RefusedStream)
                            }
                            Error::Encode(error) => BranchError::First(error),
                            Error::Pending
                            | Error::Moved
                            | Error::Unsupported
                            | Error::Invalid(_) => {
                                BranchError::Second(numbers::ErrorCode::InternalError)
                            }
                            Error::Body(never) => match never {},
                        };
                        return Err((error, failed));
                    }
                };
                let failed = prepared.stream();
                let detached = prepared
                    .detach::<M>()
                    .map_err(|error| (BranchError::First(error), failed))?;
                Ok((detached, failed))
            })
            .map_err(|failure| {
                let code = match failure.discard() {
                    Error::Borrowed { .. } | Error::Retention { .. } | Error::Owned(_) => {
                        numbers::ErrorCode::InternalError
                    }
                };
                (BranchError::Second(code), stream)
            })?;
        let (detached, failed) = outcome?;
        detached
            .defer(owner)
            .map_err(|error| (BranchError::First(error), failed))
    })
}
