use std::{cell, task::Poll};

use crate::{
    metadata,
    server::service::task::stream::{Input, Shape, exchange},
};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Cardinality {
    Streaming,
    Empty,
    One,
}

#[derive(Default)]
enum Tail {
    #[default]
    Open,
    Ready(metadata::Decoded),
    Seen,
}

pub(super) struct Ingress<Request> {
    cardinality: cell::Cell<Cardinality>,
    message: cell::Cell<Option<Request>>,
    tail: cell::Cell<Tail>,
}

impl<Request> Ingress<Request> {
    pub(super) const fn new(shape: Shape) -> Self {
        Self {
            cardinality: cell::Cell::new(match shape {
                Shape::Client | Shape::Bidi => Cardinality::Streaming,
                Shape::Unary | Shape::Server => Cardinality::Empty,
            }),
            message: cell::Cell::new(None),
            tail: cell::Cell::new(Tail::Open),
        }
    }

    pub(super) fn push_with<E>(
        &mut self,
        decode: impl FnOnce() -> Result<Request, E>,
    ) -> Result<(), exchange::PushError<E>> {
        if self.message.get_mut().is_some() || !matches!(self.tail.get_mut(), Tail::Open) {
            return Err(exchange::PushError::Busy);
        }
        let count = self.cardinality.get_mut();
        match count {
            Cardinality::Streaming | Cardinality::Empty => {}
            Cardinality::One => return Err(exchange::PushError::Cardinality),
        }
        let message = decode().map_err(exchange::PushError::Decode)?;
        if *count == Cardinality::Empty {
            *count = Cardinality::One;
        }
        *self.message.get_mut() = Some(message);
        Ok(())
    }

    pub(super) fn close(
        &mut self,
        trailers: metadata::Decoded,
    ) -> Result<(), exchange::CloseError> {
        let tail = self.tail.get_mut();
        if !matches!(tail, Tail::Open) {
            return Err(exchange::CloseError::Closed);
        }
        if self.cardinality.get() == Cardinality::Empty {
            return Err(exchange::CloseError::Empty);
        }
        *tail = Tail::Ready(trailers);
        Ok(())
    }

    pub(super) fn receive(&self) -> Poll<Input<Request>> {
        if let Some(message) = self.message.take() {
            return Poll::Ready(Input::Message(message));
        }
        match self.tail.take() {
            Tail::Open => Poll::Pending,
            Tail::Ready(trailers) => {
                self.tail.set(Tail::Seen);
                Poll::Ready(Input::Closed(trailers))
            }
            Tail::Seen => {
                self.tail.set(Tail::Seen);
                Poll::Pending
            }
        }
    }
}
