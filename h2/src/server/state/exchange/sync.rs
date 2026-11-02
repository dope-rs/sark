use sark_core::decode::{exec, request, state};

use crate::server::{payload::Buffer, state::exchange::Accepted};

pub(crate) struct Sync<'d, P>
where
    P: request::Synchronous,
{
    request: <P::Family as state::Family<P>>::State<Accepted<'d>>,
}

impl<'d, P> Sync<'d, P>
where
    P: request::Synchronous,
{
    pub(crate) const fn new(request: <P::Family as state::Family<P>>::State<Accepted<'d>>) -> Self {
        Self { request }
    }

    pub(crate) fn take(self) -> request::Body<request::Admitted<exec::Sync, P, Buffer<'d>>> {
        <P::Family as state::Homogeneous<P, exec::Sync>>::fold(self.request, Take)
    }
}

struct Take;

impl<'d, P> exec::Fold<P, Accepted<'d>, exec::Sync> for Take
where
    P: request::Synchronous,
{
    type Output = request::Body<request::Admitted<exec::Sync, P, Buffer<'d>>>;

    fn open<S: state::Slot<P>>(self, slot: S, body: Buffer<'d>) -> Self::Output {
        request::Body::Open(slot.admit(body))
    }

    fn finished<S: state::Slot<P>>(self, slot: S, body: Buffer<'d>) -> Self::Output {
        request::Body::Finished(slot.admit(body))
    }
}
