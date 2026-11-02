use std::{num::NonZero, ops::ControlFlow, pin, task};

use dope_fiber::{abi, context};
use o3::buffer::{resident, segments};
use sark_client::{Failure, Safety, protocol::response, reply, reply::assembly};

pub(in crate::client) trait Events<'d>: Unpin {
    type Error;

    fn attempted(&mut self, attempt: NonZero<u8>);

    fn poll_next(
        &mut self,
        context: pin::Pin<&mut context::Context<'_, 'd>>,
    ) -> task::Poll<Option<Result<response::Event<'d>, Failure<Self::Error>>>>;
}

impl<'app, 'd, S> Head<'app, 'd, S> {
    pub(in crate::client) fn new(
        stream: S,
        pool: &'app segments::Pool<resident::Shared<'d>>,
    ) -> Self {
        Self {
            state: Some((stream, assembly::Assembly::new(pool))),
        }
    }
}

/// Reads until the response head is complete.
#[must_use = "a response head is not read unless this fiber is polled"]
pub(in crate::client) struct Head<'app, 'd, S> {
    state: Option<(S, assembly::Assembly<'app, 'd>)>,
}

impl<'app, 'd, S: Events<'d>> abi::Fiber<'d> for Head<'app, 'd, S> {
    type Output = Result<(S, assembly::Body<'app, 'd>), Failure<S::Error>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut context) = call.into_parts();
        let this = this.get_mut();
        let Some((mut stream, mut assembly)) = this.state.take() else {
            return task::Poll::Pending;
        };
        loop {
            match stream.poll_next(context.as_mut()) {
                task::Poll::Pending => {
                    this.state = Some((stream, assembly));
                    return task::Poll::Pending;
                }
                task::Poll::Ready(Some(Ok(event))) => {
                    assembly = match assembly.accept(event) {
                        Ok(ControlFlow::Break(body)) => {
                            return task::Poll::Ready(Ok((stream, body)));
                        }
                        Ok(ControlFlow::Continue(assembly)) => assembly,
                        Err(error) => {
                            return task::Poll::Ready(Err(Failure::new(
                                error.generalize(),
                                Safety::PossiblyCommitted,
                            )));
                        }
                    };
                    if let context::Continue::Exhausted = context.as_ref().try_continue() {
                        this.state = Some((stream, assembly));
                        return task::Poll::Pending;
                    }
                }
                task::Poll::Ready(Some(Err(error))) => return task::Poll::Ready(Err(error)),
                task::Poll::Ready(None) => {
                    let cause = assembly.missing();
                    return task::Poll::Ready(Err(Failure::new(
                        cause.generalize(),
                        Safety::PossiblyCommitted,
                    )));
                }
            }
        }
    }
}

/// Reads the body and trailers of a response whose head is already complete.
#[must_use = "a response body is not read unless this fiber is polled"]
pub(in crate::client) struct Body<'app, 'd, S> {
    stream: S,
    assembly: Option<assembly::Body<'app, 'd>>,
}

impl<'app, 'd, S> Body<'app, 'd, S> {
    pub(in crate::client) fn new(stream: S, assembly: assembly::Body<'app, 'd>) -> Self {
        Self {
            stream,
            assembly: Some(assembly),
        }
    }
}

impl<'app, 'd, S: Events<'d>> abi::Fiber<'d> for Body<'app, 'd, S> {
    type Output = Result<reply::Scoped<'app, 'd>, Failure<S::Error>>;

    fn poll(call: context::PollCall<'_, '_, 'd, Self>) -> task::Poll<Self::Output> {
        let (this, mut context) = call.into_parts();
        let this = this.get_mut();
        let Some(mut assembly) = this.assembly.take() else {
            return task::Poll::Pending;
        };
        loop {
            match this.stream.poll_next(context.as_mut()) {
                task::Poll::Pending => {
                    this.assembly = Some(assembly);
                    return task::Poll::Pending;
                }
                task::Poll::Ready(Some(Ok(event))) => {
                    if let Err(error) = assembly.accept(event) {
                        return task::Poll::Ready(Err(Failure::new(
                            error.generalize(),
                            Safety::PossiblyCommitted,
                        )));
                    }
                    if let context::Continue::Exhausted = context.as_ref().try_continue() {
                        this.assembly = Some(assembly);
                        return task::Poll::Pending;
                    }
                }
                task::Poll::Ready(Some(Err(error))) => return task::Poll::Ready(Err(error)),
                task::Poll::Ready(None) => {
                    return task::Poll::Ready(Ok(assembly.finish()));
                }
            }
        }
    }
}
