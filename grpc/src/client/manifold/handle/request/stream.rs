//! One client-streaming request capability.
use std::{pin, task};

use dope::core::driver::{
    route::{Identity, KeyTag},
    schedule::ready::wake::table,
};
use dope_fiber::{abi, context};
use o3::{base::cell::region, buffer::resident};
use sark_h2::stream;

use crate::{
    Reason,
    client::{
        call,
        call::{request, unary},
        manifold::{
            api, error, handle,
            ops::{cancels, send},
        },
        receive::Unary as _,
    },
};

#[must_use = "a request stream is cancelled unless it is finished"]
pub struct Stream<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity = KeyTag<0>> {
    pub(super) scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
    pub(super) stream: Option<cancels::Registered<request::Stream<'d, stream::Key>>>,
}

enum Finish<S, U> {
    Request(S),
    Response(U),
    Done,
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Stream<'app, 'life, 'e, 'd, P, ID> {
    pub(in crate::client::manifold) fn new(
        scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
        stream: cancels::Registered<request::Stream<'d, stream::Key>>,
    ) -> Self {
        Self {
            scope,
            stream: Some(stream),
        }
    }

    /// Sends immediately, returning the admission error if capacity is unavailable.
    /// Use [`api::Request::send`] to wait for capacity.
    pub fn try_send<C: crate::Codec>(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        codec: &mut C,
        message: &C::Encode,
    ) -> Result<(), error::Error> {
        let stream = self.stream.as_mut().ok_or_else(Self::consumed)?;
        let port = self.scope.port;
        let conn = self.scope.connection;
        match send::Sender::bound(token, port, conn).try_request(stream, codec, message)? {
            send::Outcome::Sent => Ok(()),
            send::Outcome::Pending => Err(error::Error::Protocol(call::Admission::<()>::reject())),
        }
    }

    /// Ends the request immediately and returns its undecoded response capability.
    /// Use [`api::Request::finish`] to wait for and decode the response.
    pub fn try_finish(
        mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> Result<handle::Unary<'app, 'life, 'e, 'd, P, ID>, error::Error> {
        let call = self.try_end(token)?;
        Ok(handle::Unary::new(self.scope, call))
    }

    fn try_end(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> Result<cancels::Registered<unary::Handle<'d, stream::Key>>, error::Error> {
        let stream = self.stream.take().ok_or_else(Self::consumed)?;
        let port = self.scope.port;
        let conn = self.scope.connection;
        send::Sender::bound(token, port, conn).end_request(stream)
    }

    fn consumed() -> error::Error {
        error::Error::Protocol(crate::Status::new(
            crate::Code::Internal,
            Reason::RequestConsumed,
        ))
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> api::Request<'app, 'd, P>
    for Stream<'app, 'life, 'e, 'd, P, ID>
{
    type Error = error::Error;

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
                    .try_request(stream, &mut codec, message)
            },
        )
    }

    fn finish<'a, D: crate::Codec + 'a>(
        self,
        mut codec: D,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<handle::Delivery<'app, 'd, P, D::Owned<resident::Shared<'d>>>, Self::Error>,
    > + 'a
    where
        'd: 'a,
        Self: 'a,
    {
        let mut phase: Finish<Self, handle::Unary<'app, 'life, 'e, 'd, P, ID>> =
            Finish::Request(self);
        let mut slot: table::Registration<'life, 'd> = table::Registration::new();
        abi::poll_fn(
            move |mut context: pin::Pin<&mut context::Context<'_, 'd>>| {
                let wake = context.as_ref().completion();
                let mut driver = context.as_mut().driver();
                let token = driver.state_token();
                loop {
                    let output = match &mut phase {
                        Finish::Request(stream) => {
                            let scope = stream.scope;
                            match stream.try_end(token) {
                                Ok(call) => {
                                    phase = Finish::Response(handle::Unary::new(scope, call));
                                    continue;
                                }
                                Err(error) => Err(error),
                            }
                        }
                        Finish::Response(call) => match call.poll_with(token, &wake, &mut slot) {
                            task::Poll::Ready(Ok(result)) => result
                                .into_decoded(&mut codec)
                                .map_err(error::Error::Protocol),
                            task::Poll::Ready(Err(error)) => Err(error),
                            task::Poll::Pending => return task::Poll::Pending,
                        },
                        Finish::Done => Err(Self::consumed()),
                    };
                    phase = Finish::Done;
                    slot.clear();
                    return task::Poll::Ready(output);
                }
            },
        )
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Drop
    for Stream<'app, 'life, 'e, 'd, P, ID>
{
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            self.scope.cancel(stream.map(cancels::Call::Request));
        }
    }
}
