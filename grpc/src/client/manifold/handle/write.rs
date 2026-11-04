use std::{pin, task};

use dope::core::driver::{route::Identity, schedule::ready::wake::queue};
use dope_fiber::{abi, context};
use o3::{base::cell::region, depot::queue::resident::Fixed};

use crate::client::manifold::{error, handle, ops::send::Outcome};

pub(super) struct Write<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> {
    scope: Option<handle::Scope<'app, 'life, 'e, 'd, P, ID>>,
    slot: queue::Registration<'life, 'd, Fixed<1>>,
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Drop
    for Write<'app, 'life, 'e, 'd, P, ID>
{
    fn drop(&mut self) {
        if let Some(scope) = self.scope {
            scope.port.shared.sender_waiters.cancel(&mut self.slot);
        }
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Write<'app, 'life, 'e, 'd, P, ID> {
    pub(super) fn send<'a, S: 'a>(
        scope: handle::Scope<'app, 'life, 'e, 'd, P, ID>,
        stream: &'a mut Option<S>,
        mut consumed: impl FnMut() -> error::Error + 'a,
        mut write: impl FnMut(
            &mut region::Token<region::Identity<'d, 1>>,
            handle::Scope<'app, 'life, 'e, 'd, P, ID>,
            &mut S,
        ) -> Result<Outcome, error::Error>
        + 'a,
    ) -> impl abi::Fiber<'d, Output = Result<(), error::Error>> + 'a
    where
        Self: 'a,
        'd: 'a,
    {
        let mut sender = Self {
            scope: Some(scope),
            slot: queue::Registration::new(),
        };
        abi::poll_fn(
            move |mut context: pin::Pin<&mut context::Context<'_, 'd>>| {
                let sender = &mut sender;
                let Some(owner) = sender.scope else {
                    return task::Poll::Pending;
                };
                let wake = context.as_ref().completion();
                let mut driver = context.as_mut().driver();
                let token = driver.state_token();
                let result = match stream.as_mut() {
                    Some(stream) => {
                        handle::poll_sender(owner.port, &wake, &mut sender.slot, || {
                            Ok(match write(token, owner, stream)? {
                                Outcome::Sent => Some(()),
                                Outcome::Pending => None,
                            })
                        })
                    }
                    None => task::Poll::Ready(Err(consumed())),
                };
                if result.is_ready() {
                    sender.slot.clear();
                    sender.scope = None;
                }
                result
            },
        )
    }
}
