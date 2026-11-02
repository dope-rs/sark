use std::{mem, pin, task};

use dope::{
    core::driver::{
        route::{Identity, KeyTag},
        schedule::ready::wake::{fanout, queue},
    },
    runtime::client,
};
use dope_fiber::{abi, context};
use o3::{base::cell::region, buffer::resident, depot::queue::resident::Fixed};

use crate::{
    client::manifold::{
        api, channel, error,
        handle::{self, request},
        ops::send,
    },
    metadata,
};

/// Retains the exact provider through every request transition.
///
///
pub struct Handle<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity = KeyTag<0>> {
    pub(super) port: &'life channel::Port<'e, 'd, ID>,
    pub(super) bound: client::Bound<'app, 'd, P>,
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Copy
    for Handle<'app, 'life, 'e, 'd, P, ID>
{
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Clone
    for Handle<'app, 'life, 'e, 'd, P, ID>
{
    fn clone(&self) -> Self {
        *self
    }
}

const _: () = {
    assert!(
        mem::size_of::<Handle<'static, 'static, 'static, 'static, str>>()
            == mem::size_of::<usize>()
    );
    assert!(
        mem::size_of::<Option<Handle<'static, 'static, 'static, 'static, str>>>()
            == mem::size_of::<usize>()
    );
};

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> Handle<'app, 'life, 'e, 'd, P, ID> {
    pub(super) const fn new(
        port: &'life channel::Port<'e, 'd, ID>,
        bound: client::Bound<'app, 'd, P>,
    ) -> Self {
        Self { port, bound }
    }

    pub fn wait_active<'a>(
        &'a self,
    ) -> impl abi::Fiber<'d, Output = Result<(), error::Error>> + 'a {
        let mut cap = Some(*self);
        let mut slot: fanout::Registration<'life, 'd> = fanout::Registration::new();
        abi::poll_fn(
            move |mut context: pin::Pin<&mut context::Context<'_, 'd>>| {
                let wake = context.as_ref().completion();
                let mut driver = context.as_mut().driver();
                let token = driver.state_token();
                let Some(handle) = cap else {
                    return task::Poll::Pending;
                };
                let result = (|| {
                    let state = handle.port.shared.get_mut(token);
                    if state.open().is_some() {
                        return task::Poll::Ready(Ok(()));
                    }
                    if let Some(error) = state.terminal() {
                        return task::Poll::Ready(Err(error));
                    }
                    if let Err(cause) = handle
                        .port
                        .shared
                        .change_waiters
                        .try_register(&mut slot, &wake)
                    {
                        return task::Poll::Ready(Err(error::Error::Change(cause)));
                    }
                    task::Poll::Pending
                })();
                if result.is_ready() {
                    slot.clear();
                    cap = None;
                }
                result
            },
        )
    }

    pub fn poll_unary<'a, C: crate::Codec + 'a>(
        &'a self,
        path: &'a [u8],
        target: Option<&'a [u8]>,
        metadata: &'a metadata::Metadata,
        codec: &'a mut C,
        message: &'a C::Encode,
    ) -> impl abi::Fiber<'d, Output = Result<handle::Unary<'app, 'life, 'e, 'd, P, ID>, error::Error>> + 'a
    {
        let mut cap = Some(*self);
        let mut slot: queue::Registration<'life, 'd, Fixed<1>> = queue::Registration::new();
        abi::poll_fn(
            move |mut context: pin::Pin<&mut context::Context<'_, 'd>>| {
                let wake = context.as_ref().completion();
                let mut driver = context.as_mut().driver();
                let token = driver.state_token();
                let Some(handle) = cap else {
                    return task::Poll::Pending;
                };
                let result = handle::poll_sender(handle.port, &wake, &mut slot, || {
                    let mut sender = send::Sender::new(token, handle.port)?;
                    match sender.try_unary(path, target, metadata, codec, message)? {
                        Some(call) => {
                            let scope =
                                handle::Scope::new(handle.port, sender.connection(), handle.bound);
                            Ok(Some(handle::Unary::new(scope, call)))
                        }
                        None => Ok(None),
                    }
                });
                if result.is_ready() {
                    slot.clear();
                    cap = None;
                }
                result
            },
        )
    }
}

impl<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> api::Session<'app, 'd, P>
    for Handle<'app, 'life, 'e, 'd, P, ID>
{
    type Error = error::Error;
    type Request = request::Stream<'app, 'life, 'e, 'd, P, ID>;
    type Response = handle::Response<'app, 'life, 'e, 'd, P, ID>;
    type Bidi = handle::Bidi<'app, 'life, 'e, 'd, P, ID>;

    fn try_request(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        path: &[u8],
        target: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<Self::Request, Self::Error> {
        let mut sender = send::Sender::new(token, self.port)?;
        let stream = sender.open_request(path, target, metadata)?;
        let scope = handle::Scope::new(self.port, sender.connection(), self.bound);
        Ok(request::Stream::new(scope, stream))
    }

    fn try_bidi(
        &self,
        token: &mut region::Token<region::Identity<'d, 1>>,
        path: &[u8],
        target: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<Self::Bidi, Self::Error> {
        let mut sender = send::Sender::new(token, self.port)?;
        let stream = sender.open_bidi(path, target, metadata)?;
        let scope = handle::Scope::new(self.port, sender.connection(), self.bound);
        Ok(handle::Bidi::new(scope, stream))
    }

    fn unary<'a, D: crate::Codec + 'a>(
        &'a self,
        path: &'a [u8],
        target: Option<&'a [u8]>,
        metadata: &'a metadata::Metadata,
        codec: D,
        message: &'a D::Encode,
    ) -> impl abi::Fiber<
        'd,
        Output = Result<handle::Delivery<'app, 'd, P, D::Owned<resident::Shared<'d>>>, Self::Error>,
    > + 'a
    where
        'd: 'a,
    {
        channel::Unary::new(*self, path, target, metadata, codec, message)
    }

    fn stream<'a, D: crate::Codec + 'a>(
        &'a self,
        path: &'a [u8],
        target: Option<&'a [u8]>,
        metadata: &'a metadata::Metadata,
        mut codec: D,
        message: &'a D::Encode,
    ) -> impl abi::Fiber<'d, Output = Result<Self::Response, Self::Error>> + 'a
    where
        'd: 'a,
    {
        let mut cap = Some(*self);
        let mut slot: queue::Registration<'life, 'd, Fixed<1>> = queue::Registration::new();
        abi::poll_fn(
            move |mut context: pin::Pin<&mut context::Context<'_, 'd>>| {
                let wake = context.as_ref().completion();
                let mut driver = context.as_mut().driver();
                let token = driver.state_token();
                let Some(handle) = cap else {
                    return task::Poll::Pending;
                };
                let result = handle::poll_sender(handle.port, &wake, &mut slot, || {
                    let mut sender = send::Sender::new(token, handle.port)?;
                    match sender.try_server(path, target, metadata, &mut codec, message)? {
                        Some(stream) => {
                            let scope =
                                handle::Scope::new(handle.port, sender.connection(), handle.bound);
                            Ok(Some(handle::Response::new(scope, stream)))
                        }
                        None => Ok(None),
                    }
                });
                if result.is_ready() {
                    slot.clear();
                    cap = None;
                }
                result
            },
        )
    }
}
