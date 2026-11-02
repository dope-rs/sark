use std::{collections::VecDeque, pin::Pin, task::Poll};

use dope_fiber::{
    abi,
    context::{Context, PollCall},
};
use sark::http::payload::response::dispatch::chunk::Chunk;

pub struct Chunks<'a> {
    chunks: VecDeque<Chunk<'a>>,
    pend_next: bool,
}

impl<'a> Chunks<'a> {
    pub(super) fn new<I: IntoIterator<Item = Chunk<'a>>>(parts: I) -> Self {
        Self {
            chunks: parts.into_iter().collect(),
            pend_next: true,
        }
    }

    fn arm_pend(&mut self, cx: Pin<&mut Context<'_, '_>>) -> Poll<()> {
        if self.pend_next {
            self.pend_next = false;
            cx.wake();
            return Poll::Pending;
        }
        self.pend_next = true;
        Poll::Ready(())
    }
}

impl<'d> abi::Stream<'d> for Chunks<'d> {
    type Item = Chunk<'d>;
    fn poll_next(call: PollCall<'_, '_, 'd, Self>) -> Poll<Option<Self::Item>> {
        let (self_, cx) = call.into_parts();
        let this = self_.get_mut();
        if this.chunks.is_empty() {
            return Poll::Ready(None);
        }
        if this.arm_pend(cx).is_pending() {
            return Poll::Pending;
        }
        Poll::Ready(this.chunks.pop_front())
    }
}
