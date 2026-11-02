use sark_core::decode::{self, exec, request};
use sark_http::payload::owned;

use crate::server::{payload, response, state::exchange::Deferred, sync::book::Delayed};

pub(in crate::server::sync) enum Phase<'d, R: decode::Decode> {
    Routed(Delayed<'d, R>),
    Request(request::Ready<exec::Sync, R::Prepared, owned::Segments<'d>>),
    Status(response::Status),
    Head(response::Head<'d>),
}

impl<'d, R: decode::Decode> Phase<'d, R>
where
    Delayed<'d, R>: Deferred<'d, R::Prepared, R::Routes>,
{
    pub(super) fn try_finish(self) -> (Self, Option<Result<(), payload::Error>>) {
        match self {
            Self::Routed(routed) => match Deferred::try_finish(routed) {
                Ok(routed) => (Self::Routed(routed), Some(Ok(()))),
                Err((error, routed)) => (Self::Routed(routed), Some(Err(error))),
            },
            phase => (phase, None),
        }
    }
}
