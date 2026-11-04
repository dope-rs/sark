use core::{pin, task};

use dope::core::driver::{
    route::Identity,
    schedule::ready::wake::{queue, table},
};
use dope_fiber::{abi, context};
use o3::{buffer::resident, depot::queue::resident::Fixed};

use crate::{
    client::{
        manifold::{channel::Handle, error, handle, ops::send},
        receive::Unary as _,
    },
    metadata,
};

enum Phase<'app, 'life, 'e, 'd: 'app, P: ?Sized + 'app, ID: Identity> {
    Starting(queue::Registration<'life, 'd, Fixed<1>>),
    Calling {
        call: handle::Unary<'app, 'life, 'e, 'd, P, ID>,
        lease: table::Registration<'life, 'd>,
    },
}

#[doc(hidden)]
#[pin_project::pin_project(PinnedDrop)]
pub struct Unary<'target, 'app, 'life, 'e, 'd, P: ?Sized + 'app, D: crate::Codec, ID: Identity>
where
    'd: 'app + 'target,
{
    handle: Handle<'app, 'life, 'e, 'd, P, ID>,
    path: &'target [u8],
    target: Option<&'target [u8]>,
    metadata: &'target metadata::Metadata,
    codec: D,
    message: &'target D::Encode,
    phase: Phase<'app, 'life, 'e, 'd, P, ID>,
}

#[pin_project::pinned_drop]
impl<'target, 'app, 'life, 'e, 'd, P: ?Sized + 'app, D: crate::Codec, ID: Identity> PinnedDrop
    for Unary<'target, 'app, 'life, 'e, 'd, P, D, ID>
where
    'd: 'app + 'target,
{
    fn drop(self: pin::Pin<&mut Self>) {
        let this = self.project();
        if let Phase::Starting(lease) = this.phase {
            this.handle.port.shared.sender_waiters.cancel(lease);
        }
    }
}

impl<'target, 'app, 'life, 'e, 'd, P: ?Sized + 'app, D: crate::Codec, ID: Identity>
    Unary<'target, 'app, 'life, 'e, 'd, P, D, ID>
where
    'd: 'app + 'target,
{
    pub(super) fn new(
        handle: Handle<'app, 'life, 'e, 'd, P, ID>,
        path: &'target [u8],
        target: Option<&'target [u8]>,
        metadata: &'target metadata::Metadata,
        codec: D,
        message: &'target D::Encode,
    ) -> Self {
        Self {
            handle,
            path,
            target,
            metadata,
            codec,
            message,
            phase: Phase::Starting(queue::Registration::new()),
        }
    }
}

impl<'target, 'app, 'life, 'e, 'd, P: ?Sized + 'app, D: crate::Codec, ID: Identity> abi::Fiber<'d>
    for Unary<'target, 'app, 'life, 'e, 'd, P, D, ID>
where
    'd: 'app + 'target,
{
    type Output =
        Result<handle::Delivery<'app, 'd, P, D::Owned<resident::Shared<'d>>>, error::Error>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut context) = call.into_parts();
        let this = this.get_mut();
        let wake = context.as_ref().completion();
        let mut driver = context.as_mut().driver();
        let token = driver.state_token();
        loop {
            match &mut this.phase {
                Phase::Starting(lease) => {
                    match handle::poll_sender(this.handle.port, &wake, lease, || {
                        let mut sender = send::Sender::new(token, this.handle.port)?;
                        sender
                            .try_unary(
                                this.path,
                                this.target,
                                this.metadata,
                                &mut this.codec,
                                this.message,
                            )
                            .map(|call| call.map(|call| (call, sender.connection())))
                    }) {
                        task::Poll::Ready(Ok((started, conn))) => {
                            lease.clear();
                            let scope =
                                handle::Scope::new(this.handle.port, conn, this.handle.bound);
                            this.phase = Phase::Calling {
                                call: handle::Unary::new(scope, started),
                                lease: table::Registration::new(),
                            };
                        }
                        task::Poll::Ready(Err(error)) => {
                            lease.clear();
                            return task::Poll::Ready(Err(error));
                        }
                        task::Poll::Pending => return task::Poll::Pending,
                    }
                }
                Phase::Calling { call, lease } => {
                    let result = call.poll_with(token, &wake, lease);
                    if result.is_ready() {
                        lease.clear();
                    }
                    return match result {
                        task::Poll::Ready(Ok(result)) => task::Poll::Ready(
                            result
                                .into_decoded(&mut this.codec)
                                .map_err(error::Error::Protocol),
                        ),
                        task::Poll::Ready(Err(error)) => task::Poll::Ready(Err(error)),
                        task::Poll::Pending => task::Poll::Pending,
                    };
                }
            }
        }
    }
}
