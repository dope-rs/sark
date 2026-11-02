use std::{pin, task};

use dope::core::driver::{
    route::{Identity, KeyTag},
    schedule::ready::wake::{self, table},
};
use dope_fiber::{abi, context};
use o3::{base::cell::region, buffer::resident};
use sark_h2::stream;

use crate::{
    Reason,
    client::{
        call::unary,
        manifold::{
            error, handle,
            ops::{cancels, polls::Completion},
        },
        receive,
    },
};

#[must_use = "a unary call is cancelled unless it is polled to completion"]
pub struct Unary<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity = KeyTag<0>> {
    pub(super) scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
    pub(super) call: Option<cancels::Registered<unary::Handle<'d, stream::Key>>>,
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Unary<'app, 'life, 'e, 'd, P, ID> {
    pub(in crate::client::manifold) fn new(
        scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
        call: cancels::Registered<unary::Handle<'d, stream::Key>>,
    ) -> Self {
        Self {
            scope,
            call: Some(call),
        }
    }

    pub fn try_poll(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
    ) -> Result<
        Option<handle::Delivery<'app, 'd, P, receive::UnaryResult<resident::Shared<'d>>>>,
        error::Error,
    > {
        let call = self.call.as_ref().ok_or_else(Self::consumed)?;
        let result = Completion::new(token, self.scope).unary(call, None)?;
        if let Some(result) = result {
            self.call = None;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    pub fn recv<'a>(
        &'a mut self,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<
            handle::Delivery<'app, 'd, P, receive::UnaryResult<resident::Shared<'d>>>,
            error::Error,
        >,
    > + 'a {
        let mut call = Some(self);
        let mut slot: table::Registration<'life, 'd> = table::Registration::new();
        abi::poll_fn(
            move |mut context: pin::Pin<&mut context::Context<'_, 'd>>| {
                let wake = context.as_ref().completion();
                let mut driver = context.as_mut().driver();
                let token = driver.state_token();
                let Some(owner) = call.as_deref_mut() else {
                    return task::Poll::Pending;
                };
                let result = owner.poll_with(token, &wake, &mut slot);
                if result.is_ready() {
                    slot.clear();
                    call = None;
                }
                result
            },
        )
    }

    pub(in crate::client::manifold) fn poll_with<'target>(
        &mut self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        wake: &wake::Waker<'d>,
        lease: &mut table::Registration<'target, 'd>,
    ) -> task::Poll<
        Result<
            handle::Delivery<'app, 'd, P, receive::UnaryResult<resident::Shared<'d>>>,
            error::Error,
        >,
    >
    where
        'life: 'target,
    {
        let call = self.call.as_ref().ok_or_else(Self::consumed)?;
        match Completion::new(token, self.scope).unary(call, Some((lease, wake)))? {
            Some(result) => {
                self.call = None;
                task::Poll::Ready(Ok(result))
            }
            None => task::Poll::Pending,
        }
    }

    fn consumed() -> error::Error {
        error::Error::Protocol(crate::Status::new(
            crate::Code::Internal,
            Reason::UnaryConsumed,
        ))
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Drop
    for Unary<'app, 'life, 'e, 'd, P, ID>
{
    fn drop(&mut self) {
        if let Some(call) = self.call.take() {
            self.scope.cancel(call.map(cancels::Call::Unary));
        }
    }
}
