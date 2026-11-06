use std::{cell::Cell, collections::VecDeque, pin::Pin, rc::Rc, task::Poll};

use dope_fiber::{abi, context::PollCall};

#[derive(Default)]
pub struct Trace {
    pub heads: Cell<usize>,
    pub polls: Cell<usize>,
    pub drops: Cell<usize>,
    pub moved: Cell<bool>,
    pub resident: Cell<usize>,
}

#[pin_project::pin_project(PinnedDrop, !Unpin)]
pub struct Chunks<T> {
    chunks: VecDeque<T>,
    pend_next: bool,
    trace: Option<Rc<Trace>>,
    address: Option<usize>,
    park: bool,
}

impl<T> Chunks<T> {
    pub(crate) fn new<I: IntoIterator<Item = T>>(parts: I, trace: Option<Rc<Trace>>) -> Self {
        Self {
            chunks: parts.into_iter().collect(),
            pend_next: true,
            trace,
            address: None,
            park: false,
        }
    }

    pub(crate) fn park(mut self, park: bool) -> Self {
        self.park = park;
        self
    }
}

#[pin_project::pinned_drop]
impl<T> PinnedDrop for Chunks<T> {
    fn drop(self: Pin<&mut Self>) {
        if let Some(trace) = &self.project().trace {
            trace.drops.set(trace.drops.get() + 1);
        }
    }
}

impl<'d, T> abi::Stream<'d> for Chunks<T> {
    type Item = T;
    fn poll_next(call: PollCall<'_, '_, 'd, Self>) -> Poll<Option<Self::Item>> {
        let (self_, cx) = call.into_parts();
        let address = self_.as_ref().get_ref() as *const Self as usize;
        let this = self_.project();
        if let Some(trace) = &this.trace {
            trace.polls.set(trace.polls.get() + 1);
            if this
                .address
                .replace(address)
                .is_some_and(|old| old != address)
            {
                trace.moved.set(true);
            }
        }
        if this.chunks.is_empty() {
            return if *this.park {
                Poll::Pending
            } else {
                Poll::Ready(None)
            };
        }
        if *this.pend_next {
            *this.pend_next = false;
            cx.wake();
            return Poll::Pending;
        }
        *this.pend_next = true;
        Poll::Ready(this.chunks.pop_front())
    }
}
