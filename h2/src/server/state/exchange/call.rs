use core::{mem, ops};

use dope::core::BranchError;
use o3::buffer::resident;
use sark_core::{
    body, decode,
    decode::{exec, request},
};
use sark_http::{field::metadata::section::rules, payload::owned, request::body::Error};

use crate::{
    conn::{event, machine},
    frame::numbers,
    server::response::{self, Encoder, Handoff},
    stream,
};

#[must_use]
pub(crate) struct Call<'d, P> {
    ready: request::Ready<exec::Sync, P, owned::Segments<'d>>,
}

const _: () = {
    assert!(mem::size_of::<Call<'_, ()>>() == mem::size_of::<body::Body<owned::Segments<'_>>>());
    assert!(mem::align_of::<Call<'_, ()>>() == mem::align_of::<body::Body<owned::Segments<'_>>>());
};

impl<'d, P> Call<'d, P> {
    pub(crate) const fn new(ready: request::Ready<exec::Sync, P, owned::Segments<'d>>) -> Self {
        Self { ready }
    }

    pub(in crate::server) fn try_dispatch<R, M, E, F>(
        self,
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
        R: decode::Decode<Prepared = P>,
        M: Handoff,
        E: event::Evidence,
        F: rules::Policy,
    {
        let Self { ready } = self;
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
}
