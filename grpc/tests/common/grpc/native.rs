use std::{cell::Cell, pin::Pin, task::Poll};

use dope::core::driver::schedule::clock::timer::Timer;
use dope_fiber::{abi, context};
use sark_grpc::{
    Code, Status,
    encode::Prost,
    server::service::{
        handler::modes,
        task::{stream, unary},
    },
};

#[derive(Default)]
pub struct Facts {
    pub polls: Cell<usize>,
    pub messages: Cell<usize>,
    pub closed: Cell<bool>,
    pub drops: Cell<usize>,
}

pub struct Handler<'a>(pub &'a Facts);

pub struct Task<'a> {
    facts: &'a Facts,
    yielded: bool,
}

impl unary::Handler for Handler<'_> {
    type Request<'d> = ();
    type Response = ();
    type Codec = Prost<()>;
    type Task<'life, 'd>
        = Task<'life>
    where
        'd: 'life,
        Self: 'life;

    fn unary<'life, 'd>(&'life self, _: Timer<'d>, _: modes::Request<()>) -> Self::Task<'life, 'd>
    where
        'd: 'life,
    {
        self.0.messages.set(self.0.messages.get() + 1);
        Task {
            facts: self.0,
            yielded: false,
        }
    }
}

impl<'d> abi::Fiber<'d> for Task<'_> {
    type Output = modes::Response<()>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (task, context) = call.into_parts();
        let task = task.get_mut();
        task.facts.polls.set(task.facts.polls.get() + 1);
        if task.yielded {
            Poll::Ready(modes::Response::new(()))
        } else {
            task.yielded = true;
            context.as_ref().completion().wake();
            Poll::Pending
        }
    }
}

impl stream::Handler for Handler<'_> {
    type Request<'d> = ();
    type Response = ();
    type Codec = Prost<()>;
    type Route = ();
    type Task<'life, 'd>
        = Task<'life>
    where
        'd: 'life,
        Self: 'life;

    fn codec(&self, _: &stream::Request) -> Result<(Self::Codec, Self::Route), Status> {
        Ok((Prost::new(), ()))
    }

    fn stream<'life, 'd>(
        &'life self,
        _: stream::Request,
        (): (),
    ) -> stream::Call<Self::Task<'life, 'd>>
    where
        'd: 'life,
    {
        stream::Call::new(Task {
            facts: self.0,
            yielded: false,
        })
    }
}

impl<'d> stream::Work<'d, (), (), Prost<()>> for Task<'_> {
    fn poll(
        self: Pin<&mut Self>,
        mut context: stream::Context<'_, 'd, (), (), Prost<()>>,
        _: context::Admission<'_, '_, 'd>,
    ) -> Poll<stream::Finish> {
        let facts = self.facts;
        facts.polls.set(facts.polls.get() + 1);
        match context.poll_receive() {
            Poll::Pending => Poll::Pending,
            Poll::Ready(stream::Input::Message(())) => {
                facts.messages.set(facts.messages.get() + 1);
                match context.try_send(()) {
                    Ok(()) => Poll::Pending,
                    Err(()) => Poll::Ready(stream::Finish {
                        status: Status::from_code(Code::Internal),
                        ..stream::Finish::ok()
                    }),
                }
            }
            Poll::Ready(stream::Input::Closed(_)) => {
                facts.closed.set(true);
                Poll::Ready(stream::Finish::ok())
            }
        }
    }
}

impl Drop for Task<'_> {
    fn drop(&mut self) {
        self.facts.drops.set(self.facts.drops.get() + 1);
    }
}

#[derive(Default)]
pub struct DueFacts {
    pub polls: [Cell<usize>; 2],
    pub drops: [Cell<usize>; 2],
    pub overlap: Cell<bool>,
}

pub struct DueHandler<'a>(pub &'a DueFacts);

pub struct DueTask<'a> {
    facts: &'a DueFacts,
    index: usize,
}

impl unary::Handler for DueHandler<'_> {
    type Request<'d> = ();
    type Response = ();
    type Codec = Prost<()>;
    type Task<'life, 'd>
        = DueTask<'life>
    where
        'd: 'life,
        Self: 'life;

    fn unary<'life, 'd>(
        &'life self,
        _: Timer<'d>,
        request: modes::Request<()>,
    ) -> Self::Task<'life, 'd>
    where
        'd: 'life,
    {
        DueTask {
            facts: self.0,
            index: usize::from(request.head.path() == b"/deadline/live"),
        }
    }
}

impl<'d> abi::Fiber<'d> for DueTask<'_> {
    type Output = modes::Response<()>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (task, context) = call.into_parts();
        let task = task.get_mut();
        task.facts.polls[task.index].set(task.facts.polls[task.index].get() + 1);
        if task.index == 1 {
            if task.facts.drops[0].get() == 1 {
                return Poll::Ready(modes::Response::new(()));
            }
            task.facts.overlap.set(true);
        }
        context.as_ref().completion().wake();
        Poll::Pending
    }
}

impl Drop for DueTask<'_> {
    fn drop(&mut self) {
        self.facts.drops[self.index].set(self.facts.drops[self.index].get() + 1);
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Sender {
    Unary,
    Stream,
    Borrowed,
}

/// Cancels the first notified sender before polling its admission again.
#[pin_project::pin_project]
pub struct CancelSender<'a, F, S> {
    #[pin]
    wait: Option<abi::Task<F>>,
    cancel: bool,
    cancelled: &'a Cell<bool>,
    start: Option<S>,
}

impl<'a, F, S> CancelSender<'a, F, S> {
    pub fn new(wait: F, cancel: bool, cancelled: &'a Cell<bool>, start: S) -> Self {
        Self {
            wait: Some(abi::Task::new(wait)),
            cancel,
            cancelled,
            start: Some(start),
        }
    }
}

impl<'d, F, S: FnOnce(), T> abi::Fiber<'d> for CancelSender<'_, F, S>
where
    F: abi::Fiber<'d, Output = Result<T, sark_grpc::client::manifold::Error>>,
{
    type Output = Option<F::Output>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Self::Output> {
        let (this, admit) = call.into_admit();
        let mut this = this.project();
        if this.start.is_none() && *this.cancel {
            this.wait.set(None);
            this.cancelled.set(true);
            return Poll::Ready(None);
        }
        let Some(wait) = this.wait.as_mut().as_pin_mut() else {
            return Poll::Pending;
        };
        let (poll, context) = admit.poll(wait);
        match poll {
            Poll::Ready(output) => {
                assert!(
                    this.start.is_none(),
                    "sender must first wait for an output owner: {:?}",
                    output.as_ref().err(),
                );
                Poll::Ready(Some(output))
            }
            Poll::Pending => {
                // A budget yield does not prove that the inner sender registered.
                if this.start.is_some()
                    && let context::Continue::Permit(_) = context.as_ref().try_continue()
                    && let Some(start) = this.start.take()
                {
                    start();
                }
                Poll::Pending
            }
        }
    }
}
