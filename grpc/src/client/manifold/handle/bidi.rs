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
        call::bidi,
        manifold::{
            api, error, handle,
            ops::{cancels, polls::Completion, send},
        },
        receive,
    },
};

#[must_use = "a bidirectional stream is cancelled unless it is drained"]
pub struct Bidi<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity = KeyTag<0>> {
    pub(super) scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
    pub(super) stream: Option<cancels::Registered<bidi::Stream<'d, stream::Key>>>,
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Bidi<'app, 'life, 'e, 'd, P, ID> {
    pub(in crate::client::manifold) fn new(
        scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
        stream: cancels::Registered<bidi::Stream<'d, stream::Key>>,
    ) -> Self {
        Self {
            scope,
            stream: Some(stream),
        }
    }

    fn consumed() -> error::Error {
        error::Error::Protocol(crate::Status::new(
            crate::Code::Internal,
            Reason::BidiConsumed,
        ))
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> handle::Completion<'life, 'd>
    for Bidi<'app, 'life, 'e, 'd, P, ID>
{
    type Output = handle::Delivery<'app, 'd, P, receive::Response<resident::Shared<'d>>>;

    fn poll(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        lease: &mut table::Registration<'life, 'd>,
        wake: &wake::Waker<'d>,
    ) -> Result<Option<Self::Output>, error::Error> {
        let stream = self.stream.as_ref().ok_or_else(Self::consumed)?;
        Completion::new(token, self.scope).response(&stream.response, Some((lease, wake)))
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> api::Recv<'app, 'd, P>
    for Bidi<'app, 'life, 'e, 'd, P, ID>
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

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> api::Duplex<'app, 'd, P>
    for Bidi<'app, 'life, 'e, 'd, P, ID>
{
    type Response = handle::Response<'app, 'life, 'e, 'd, P, ID>;
    type SendHalf = handle::Send<'app, 'life, 'e, 'd, P, ID>;

    fn send<'a, D: crate::Codec + 'a>(
        &'a mut self,
        mut codec: D,
        message: &'a D::Encode,
    ) -> impl abi::Fiber<'d, Output = Result<(), Self::Error>> + 'a
    where
        'd: 'a,
    {
        handle::Write::send(
            self.scope,
            &mut self.stream,
            Self::consumed,
            move |token, scope, stream| {
                send::Sender::bound(token, scope.port, scope.connection)
                    .try_bidi(stream, &mut codec, message)
            },
        )
    }

    fn try_finish(
        mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> Result<Self::Response, Self::Error> {
        let stream = self.stream.take().ok_or_else(Self::consumed)?;
        let port = self.scope.port;
        let response =
            send::Sender::bound(token, port, self.scope.connection).finish_bidi(stream)?;
        Ok(handle::Response::new(self.scope, response))
    }

    fn split(mut self) -> Result<(Self::SendHalf, Self::Response), Self::Error> {
        let stream = self.stream.take().ok_or_else(Self::consumed)?;
        let (send, response) = stream.split();
        Ok((
            handle::Send {
                scope: self.scope,
                send: Some(send),
            },
            handle::Response::new(self.scope, response),
        ))
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Drop
    for Bidi<'app, 'life, 'e, 'd, P, ID>
{
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            self.scope.cancel(stream.map(cancels::Call::Bidi));
        }
    }
}
