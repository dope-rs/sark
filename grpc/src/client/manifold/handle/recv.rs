use std::task;

use dope::core::driver::schedule::ready::wake::table;
use dope_fiber::{abi, context};

use crate::client::{
    manifold::{error, handle},
    receive::{self, Event as _},
};

#[pin_project::pin_project]
pub(super) struct Recv<'a, 'life: 'a, 'd: 'life, C, D>
where
    C: handle::Completion<'life, 'd>,
{
    pub(super) cap: Option<&'a mut C>,
    pub(super) codec: D,
    pub(super) slot: table::Registration<'life, 'd>,
}

impl<'life, 'd: 'life, C, D: crate::Codec> abi::Fiber<'d> for Recv<'_, 'life, 'd, C, D>
where
    C: handle::Completion<'life, 'd>,
{
    type Output = Result<<C::Output as receive::Event>::Decoded<D>, error::Error>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut context) = call.into_parts();
        let this = this.get_mut();
        let wake = context.as_ref().completion();
        let mut driver = context.as_mut().driver();
        let token = driver.state_token();
        let Some(cap) = this.cap.as_deref_mut() else {
            return task::Poll::Pending;
        };
        let result = match cap.poll(token, &mut this.slot, &wake) {
            Ok(Some(event)) => task::Poll::Ready(
                event
                    .into_decoded(&mut this.codec)
                    .map_err(error::Error::Protocol),
            ),
            Ok(None) => task::Poll::Pending,
            Err(error) => task::Poll::Ready(Err(error)),
        };
        if result.is_ready() {
            this.slot.clear();
            this.cap = None;
        }
        result
    }
}
