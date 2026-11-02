use dope::core::driver::route::{Identity, KeyTag};
use dope_fiber::abi;
use o3::base::cell::region;
use sark_h2::stream;

use crate::{
    Reason,
    client::{
        call::bidi,
        manifold::{
            api, error, handle,
            ops::{cancels, send},
        },
    },
};

#[must_use = "a bidirectional send half is cancelled unless it is finished"]
pub struct Send<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity = KeyTag<0>> {
    pub(super) scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
    pub(super) send: Option<cancels::Registered<bidi::Send<'d, stream::Key>>>,
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Send<'app, 'life, 'e, 'd, P, ID> {
    fn consumed() -> error::Error {
        error::Error::Protocol(crate::Status::new(
            crate::Code::Internal,
            Reason::BidiSendConsumed,
        ))
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> api::Write<'app, 'd, P>
    for Send<'app, 'life, 'e, 'd, P, ID>
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
            &mut self.send,
            Self::consumed,
            move |token, scope, stream| {
                send::Sender::bound(token, scope.port, scope.connection)
                    .try_half(stream, &mut codec, message)
            },
        )
    }

    fn try_finish(
        mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> Result<(), Self::Error> {
        let send = self.send.take().ok_or_else(Self::consumed)?;
        let port = self.scope.port;
        let conn = self.scope.connection;
        send::Sender::bound(token, port, conn).finish_half(send)
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Drop
    for Send<'app, 'life, 'e, 'd, P, ID>
{
    fn drop(&mut self) {
        if let Some(send) = self.send.take() {
            self.scope.cancel(send.map(cancels::Call::Send));
        }
    }
}
