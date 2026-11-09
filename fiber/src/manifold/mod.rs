//! Fiber execution bindings layered onto protocol-neutral manifold contracts.

use dope_fiber::{abi, context};
use sark_core::manifold::task::response;
use sark_http::payload::response::dispatch::chunk::Chunk;
use std::{fmt, pin::Pin, task::Poll};

mod h1;
pub mod runtime;

pub use h1::H1;

type Event<'d, S> =
    response::Event<'d, <<S as abi::Stream<'d>>::Item as TryInto<Chunk<'d>>>::Error>;

#[doc(hidden)]
#[pin_project::pin_project(project = PhaseProjection)]
pub enum Phase<F, O, S> {
    Future(#[pin] abi::Task<F>),
    Head(Option<O>),
    Body(#[pin] abi::Task<S>),
    Done,
}

impl<F, O, S> Phase<F, O, S> {
    pub const fn new(future: F) -> Self {
        Self::Future(abi::Task::new(future))
    }

    pub const fn is_head(&self) -> bool {
        matches!(self, Self::Head(_))
    }

    pub fn head<T>(mut self: Pin<&mut Self>, emit: impl FnOnce(O) -> (Option<S>, T)) -> Option<T> {
        let PhaseProjection::Head(output) = self.as_mut().project() else {
            return None;
        };
        let (stream, value) = emit(output.take()?);
        self.set(match stream {
            Some(stream) => Self::Body(abi::Task::new(stream)),
            None => Self::Done,
        });
        Some(value)
    }
}

impl<'d, F, O, S> abi::Stream<'d> for Phase<F, O, S>
where
    F: abi::Fiber<'d, Output = O>,
    S: abi::Stream<'d, Item: TryInto<Chunk<'d>, Error: fmt::Debug>>,
{
    type Item = Event<'d, S>;

    fn poll_next(call: context::PollCall<'_, '_, 'd, Self>) -> Poll<Option<Self::Item>> {
        let (mut this, admit) = call.into_admit();
        match this.as_mut().project() {
            PhaseProjection::Future(future) => match admit.poll(future).0 {
                Poll::Pending => Poll::Pending,
                Poll::Ready(output) => {
                    this.set(Self::Head(Some(output)));
                    Poll::Ready(Some(response::Event::Head))
                }
            },
            PhaseProjection::Head(_) => Poll::Pending,
            PhaseProjection::Body(stream) => match admit.poll_next(stream).0 {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Some(item)) => match item.try_into() {
                    Ok(chunk) => Poll::Ready(Some(response::Event::Chunk(chunk))),
                    Err(cause) => Poll::Ready(Some(response::Event::Failed(cause))),
                },
                Poll::Ready(None) => {
                    this.set(Self::Done);
                    Poll::Ready(None)
                }
            },
            PhaseProjection::Done => Poll::Ready(None),
        }
    }
}
