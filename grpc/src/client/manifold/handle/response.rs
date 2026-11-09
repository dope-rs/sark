use dope::core::driver::{
    route::{Identity, KeyTag},
    schedule::ready::wake::{self, table},
};
use dope_fiber::abi;
use o3::{base::cell::region, buffer::resident};
use sark_h2::stream;

use crate::{
    Reason,
    client::{
        call::response,
        manifold::{api, error, handle, ops::cancels},
        receive,
    },
};

#[must_use = "a response stream is cancelled unless it is drained"]
pub struct Response<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity = KeyTag<0>> {
    pub(super) scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
    pub(super) stream: Option<cancels::Registered<response::Stream<'d, stream::Key>>>,
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity>
    Response<'app, 'life, 'e, 'd, P, ID>
{
    pub(in crate::client::manifold) fn new(
        scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
        stream: cancels::Registered<response::Stream<'d, stream::Key>>,
    ) -> Self {
        Self {
            scope,
            stream: Some(stream),
        }
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> handle::Completion<'life, 'd>
    for Response<'app, 'life, 'e, 'd, P, ID>
{
    type Output = handle::Delivery<'app, 'd, P, receive::Response<resident::Shared<'d>>>;

    fn poll(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        lease: &mut table::Registration<'life, 'd>,
        wake: &wake::Waker<'d>,
    ) -> Result<Option<Self::Output>, error::Error> {
        let stream = self.stream.as_ref().ok_or_else(|| {
            error::Error::Protocol(crate::Status::new(
                crate::Code::Internal,
                Reason::ResponseCancelled,
            ))
        })?;
        self.scope.poll_response(token, stream, Some((lease, wake)))
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> api::Recv<'app, 'd, P>
    for Response<'app, 'life, 'e, 'd, P, ID>
{
    type Error = error::Error;

    fn recv<'a, D: crate::Codec + 'a>(
        &'a mut self,
        codec: D,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            handle::Delivery<'app, 'd, P, receive::Decoded<D::Owned<resident::Shared<'d>>>>,
            Self::Error,
        >,
    > + 'a
    where
        'd: 'a,
    {
        handle::Recv {
            cap: Some(self),
            codec,
            slot: table::Registration::new(),
        }
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Drop
    for Response<'app, 'life, 'e, 'd, P, ID>
{
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            self.scope.cancel(stream.map(cancels::Call::Response));
        }
    }
}
